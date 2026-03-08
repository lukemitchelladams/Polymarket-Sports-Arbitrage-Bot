#!/usr/bin/env python3
"""
sync_ledger.py — Reconcile ledger with actual Polymarket positions.

Usage:
  python3.11 sync_ledger.py                  # Interactive: show all active, mark sold ones
  python3.11 sync_ledger.py --show           # Just show current ledger state
  python3.11 sync_ledger.py --mark-sold "lakers vs. warriors" --pl -50.0
                                              # Mark matching positions as resolved with P&L
  python3.11 sync_ledger.py --nuke "lakers vs. warriors"
                                              # Remove positions entirely (as if they never existed)
"""

import json, os, sys, argparse, shutil
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LEDGER_FILE = os.path.join(SCRIPT_DIR, "ledger.json")
BACKUP_DIR = os.path.join(SCRIPT_DIR, "ledger_backups")


def load_ledger():
    if not os.path.exists(LEDGER_FILE):
        print("No ledger.json found!")
        sys.exit(1)
    with open(LEDGER_FILE) as f:
        return json.load(f)


def save_ledger(data):
    # Always backup first
    os.makedirs(BACKUP_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(BACKUP_DIR, f"ledger_{ts}.json")
    shutil.copy2(LEDGER_FILE, backup_path)
    print(f"  Backup saved: {backup_path}")

    with open(LEDGER_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Ledger saved: {LEDGER_FILE}")


def calc_stats(pos):
    """Calculate position stats from ledger entry."""
    yes_total = sum(a for _, a in pos.get("yes_fills", []))
    no_total = sum(a for _, a in pos.get("no_fills", []))
    yes_avg = sum(p * a for p, a in pos.get("yes_fills", [])) / yes_total if yes_total > 0 else 0
    no_avg = sum(p * a for p, a in pos.get("no_fills", [])) / no_total if no_total > 0 else 0
    yes_sh = yes_total / yes_avg if yes_avg > 0 else 0
    no_sh = no_total / no_avg if no_avg > 0 else 0
    cost = yes_total + no_total

    if yes_total > 0 and no_total > 0:
        est_pl = min(yes_sh - cost, no_sh - cost)
        spread = est_pl / cost * 100 if cost > 0 else 0
    else:
        est_pl = 0
        spread = 0

    return {
        "yes_total": yes_total, "no_total": no_total,
        "yes_avg": yes_avg, "no_avg": no_avg,
        "yes_sh": yes_sh, "no_sh": no_sh,
        "cost": cost, "est_pl": est_pl, "spread": spread,
    }


def show_ledger(data):
    active = []
    resolved = []
    for mid, pos in data.items():
        title = pos.get("title", mid)[:60]
        stats = calc_stats(pos)
        entry = (mid, title, pos, stats)
        if pos.get("resolved"):
            resolved.append(entry)
        else:
            active.append(entry)

    print(f"\n{'='*80}")
    print(f"  ACTIVE POSITIONS ({len(active)})")
    print(f"{'='*80}")
    for i, (mid, title, pos, s) in enumerate(active):
        status = "STAGNANT" if pos.get("stagnant") else ("PENDING" if pos.get("pending") else "ACTIVE")
        has_yes = s["yes_total"] > 0
        has_no = s["no_total"] > 0
        if has_yes and has_no:
            state = f"LOCKED ({s['spread']:.1f}%)" if s["spread"] >= 0 else f"UNDERWATER ({s['spread']:.1f}%)"
        elif has_yes:
            state = f"SEED YES@{s['yes_avg']:.3f}"
        elif has_no:
            state = f"SEED NO@{s['no_avg']:.3f}"
        else:
            state = "EMPTY"

        print(f"\n  [{i}] {title}")
        print(f"      YES: ${s['yes_total']:.2f} ({s['yes_sh']:.1f}sh @{s['yes_avg']:.3f}) | "
              f"NO: ${s['no_total']:.2f} ({s['no_sh']:.1f}sh @{s['no_avg']:.3f})")
        print(f"      Cost: ${s['cost']:.2f} | Est P&L: ${s['est_pl']:.2f} | State: {state} | {status}")

    total_deployed = sum(s["cost"] for _, _, p, s in active if not p.get("resolved"))
    total_est = sum(s["est_pl"] for _, _, p, s in active if not p.get("resolved") and s["yes_total"] > 0 and s["no_total"] > 0)
    print(f"\n  Total deployed: ${total_deployed:.2f} | Est P&L (locked): ${total_est:.2f}")

    resolved_pl = sum(p.get("resolved_pl", 0) or 0 for _, _, p, _ in resolved)
    print(f"\n  RESOLVED: {len(resolved)} positions | Total P&L: ${resolved_pl:.2f}")
    print(f"{'='*80}")

    return active


def interactive_sync(data):
    active = show_ledger(data)
    if not active:
        print("\nNo active positions to sync.")
        return

    print("\nOptions:")
    print("  s <index> <pl>  — Mark position as SOLD/RESOLVED with P&L (e.g. 's 0 -15.50')")
    print("  n <index>       — NUKE position (remove entirely, as if it never existed)")
    print("  r <index>       — RESET position to resolved with P&L = 0")
    print("  a               — Mark ALL active positions matching a keyword")
    print("  q               — Quit (save changes)")
    print("  x               — Exit without saving")
    print()

    changes = False
    while True:
        try:
            cmd = input("  > ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not cmd:
            continue
        if cmd.lower() == 'x':
            print("  Exiting without saving.")
            return
        if cmd.lower() == 'q':
            break

        parts = cmd.split()
        action = parts[0].lower()

        if action == 's' and len(parts) >= 3:
            try:
                idx = int(parts[1])
                pl = float(parts[2])
                mid, title, pos, stats = active[idx]
                data[mid]["resolved"] = True
                data[mid]["resolved_pl"] = pl
                print(f"  ✓ Marked RESOLVED: {title} | P&L: ${pl:.2f}")
                changes = True
            except (IndexError, ValueError) as e:
                print(f"  Error: {e}")

        elif action == 'n' and len(parts) >= 2:
            try:
                idx = int(parts[1])
                mid, title, pos, stats = active[idx]
                confirm = input(f"  Delete '{title}'? This removes ALL history. (y/n): ")
                if confirm.lower() == 'y':
                    del data[mid]
                    print(f"  ✓ NUKED: {title}")
                    changes = True
            except (IndexError, ValueError) as e:
                print(f"  Error: {e}")

        elif action == 'r' and len(parts) >= 2:
            try:
                idx = int(parts[1])
                mid, title, pos, stats = active[idx]
                data[mid]["resolved"] = True
                data[mid]["resolved_pl"] = 0
                print(f"  ✓ Marked RESOLVED (P&L $0): {title}")
                changes = True
            except (IndexError, ValueError) as e:
                print(f"  Error: {e}")

        elif action == 'a' and len(parts) >= 2:
            keyword = " ".join(parts[1:]).lower()
            matching = [(mid, title, pos, stats) for mid, title, pos, stats in active if keyword in title.lower()]
            if not matching:
                print(f"  No positions matching '{keyword}'")
                continue
            print(f"  Found {len(matching)} matching positions:")
            for j, (mid, title, pos, stats) in enumerate(matching):
                print(f"    {j}. {title[:55]} | cost ${stats['cost']:.2f}")
            pl_str = input(f"  Enter total P&L for ALL, or 'each' to enter individually: ").strip()
            if pl_str.lower() == 'each':
                for mid, title, pos, stats in matching:
                    try:
                        individual_pl = float(input(f"    P&L for '{title[:50]}'? ").strip())
                    except ValueError:
                        individual_pl = 0.0
                    data[mid]["resolved"] = True
                    data[mid]["resolved_pl"] = round(individual_pl, 2)
                    print(f"    ✓ {title[:50]} → ${individual_pl:.2f}")
                    changes = True
            else:
                try:
                    total_pl = float(pl_str)
                    per_pos = round(total_pl / len(matching), 2)
                    for mid, title, pos, stats in matching:
                        data[mid]["resolved"] = True
                        data[mid]["resolved_pl"] = per_pos
                        print(f"    ✓ {title[:50]} → ${per_pos:.2f}")
                        changes = True
                except ValueError:
                    print(f"  Invalid P&L: '{pl_str}'")
            if changes:
                print(f"  Resolved {len(matching)} positions.")

        else:
            print("  Unknown command. Use: s/n/r/a/q/x")

    if changes:
        save_ledger(data)
        print("\n  Changes saved! Restart the bot to pick up the new ledger.")
    else:
        print("\n  No changes made.")


def mark_sold(data, keyword, pl):
    keyword = keyword.lower()
    count = 0
    for mid, pos in data.items():
        title = pos.get("title", "")
        if keyword in title.lower() and not pos.get("resolved"):
            pos["resolved"] = True
            pos["resolved_pl"] = round(pl, 2)
            print(f"  ✓ {title[:60]} → resolved P&L: ${pl:.2f}")
            count += 1
    if count == 0:
        print(f"  No active positions matching '{keyword}'")
    else:
        save_ledger(data)
        print(f"\n  Resolved {count} positions. Restart the bot to pick up changes.")


def nuke(data, keyword):
    keyword = keyword.lower()
    to_delete = [mid for mid, pos in data.items()
                 if keyword in pos.get("title", "").lower() and not pos.get("resolved")]
    if not to_delete:
        print(f"  No active positions matching '{keyword}'")
        return
    for mid in to_delete:
        title = data[mid].get("title", mid)[:60]
        print(f"  ✗ Removing: {title}")
        del data[mid]
    save_ledger(data)
    print(f"\n  Nuked {len(to_delete)} positions.")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Sync ledger with actual Polymarket positions")
    p.add_argument("--show", action="store_true", help="Just show current state")
    p.add_argument("--mark-sold", type=str, help="Mark positions matching keyword as resolved")
    p.add_argument("--pl", type=float, default=0.0, help="P&L for --mark-sold")
    p.add_argument("--nuke", type=str, help="Remove positions matching keyword entirely")
    args = p.parse_args()

    data = load_ledger()

    if args.show:
        show_ledger(data)
    elif args.mark_sold:
        mark_sold(data, args.mark_sold, args.pl)
    elif args.nuke:
        nuke(data, args.nuke)
    else:
        interactive_sync(data)
