#!/usr/bin/env python3
"""
PolyArb Analytics — reads ledger.json and tells you what's actually working.
Usage: python3.11 ~/polycopy/analyze.py
"""
import json, os, sys
from collections import defaultdict

LEDGER = os.path.expanduser("~/polycopy/ledger.json")

def load():
    try:
        return json.load(open(LEDGER))
    except FileNotFoundError:
        print(f"No ledger found at {LEDGER}"); sys.exit(1)

def classify(title):
    t = title.lower()
    if any(x in t for x in ["o/u","over/under","total"]):
        if any(x in t for x in ["points","rebounds","assists","steals","blocks"]):
            return "player_prop"
        return "game_total"
    if "spread" in t or "handicap" in t:  return "spread"
    if "will " in t and "win" in t:        return "moneyline"
    if "game " in t and "map" in t:        return "esports_map"
    return "other"

def analyze(data):
    by_type   = defaultdict(list)
    completed = []   # both sides filled
    seeds     = []   # one side only
    all_pos   = []

    for mid, d in data.items():
        title      = d.get("title","")
        yes_fills  = d.get("yes_fills",[])
        no_fills   = d.get("no_fills",[])
        yes_usdc   = sum(a for _,a in yes_fills)
        no_usdc    = sum(a for _,a in no_fills)
        yes_avg    = sum(p*a for p,a in yes_fills)/yes_usdc if yes_usdc else 0
        no_avg     = sum(p*a for p,a in no_fills)/no_usdc   if no_usdc  else 0
        total      = yes_usdc + no_usdc
        spread     = (1 - yes_avg - no_avg) if (yes_usdc and no_usdc) else None
        mtype      = classify(title)

        pos = {
            "title":    title[:60],
            "type":     mtype,
            "yes_usdc": yes_usdc,
            "no_usdc":  no_usdc,
            "total":    total,
            "yes_avg":  yes_avg,
            "no_avg":   no_avg,
            "spread":   spread,
            "est":      spread * min(yes_usdc, no_usdc) if spread else None,
        }

        all_pos.append(pos)
        by_type[mtype].append(pos)
        if yes_usdc > 0 and no_usdc > 0:
            completed.append(pos)
        elif yes_usdc > 0 or no_usdc > 0:
            seeds.append(pos)

    RESET="\033[0m"; BOLD="\033[1m"; GREEN="\033[92m"; RED="\033[91m"; CYAN="\033[96m"; DIM="\033[2m"

    print(f"\n{BOLD}{CYAN}{'='*70}{RESET}")
    print(f"{BOLD}{CYAN}  PolyArb Analytics Report{RESET}")
    print(f"{BOLD}{CYAN}{'='*70}{RESET}")
    print(f"\n  Total positions tracked: {len(all_pos)}")
    print(f"  Completed (both sides):  {len(completed)}")
    print(f"  Seeds (one side only):   {len(seeds)}")

    # ── By market type ──────────────────────────────────────────────
    print(f"\n{BOLD}  BY MARKET TYPE{RESET}")
    print(f"  {'Type':<16} {'Count':>5} {'Completed':>9} {'Avg Spread':>10} {'Total $':>8} {'Est Profit':>10}")
    print(f"  {'─'*16} {'─'*5} {'─'*9} {'─'*10} {'─'*8} {'─'*10}")
    for mtype, positions in sorted(by_type.items(), key=lambda x: -len(x[1])):
        comp  = [p for p in positions if p["spread"] is not None]
        pos_s = [p for p in comp if p["spread"] > 0]
        neg_s = [p for p in comp if p["spread"] <= 0]
        avg_s = sum(p["spread"] for p in comp)/len(comp)*100 if comp else 0
        total = sum(p["total"] for p in positions)
        est   = sum(p["est"] for p in comp if p["est"])
        color = GREEN if avg_s > 0 else RED
        print(f"  {mtype:<16} {len(positions):>5} {len(comp):>5}/{len(positions):<3} "
              f"{color}{avg_s:>+9.1f}%{RESET} {total:>7.0f}  {color}{est:>+9.2f}{RESET}")

    # ── Top performers ──────────────────────────────────────────────
    print(f"\n{BOLD}  TOP 10 BEST ARBS (by est profit){RESET}")
    print(f"  {'Market':<55} {'Spread':>7} {'Est':>7}")
    print(f"  {'─'*55} {'─'*7} {'─'*7}")
    top = sorted([p for p in completed if p["est"]], key=lambda x: -x["est"])[:10]
    for p in top:
        c = GREEN if p["est"] > 0 else RED
        print(f"  {p['title']:<55} {c}{p['spread']*100:>+6.1f}%{RESET} {c}${p['est']:>6.2f}{RESET}")

    # ── Worst underwater ────────────────────────────────────────────
    print(f"\n{BOLD}  TOP 10 WORST UNDERWATER{RESET}")
    print(f"  {'Market':<55} {'Spread':>7} {'Est':>7}")
    print(f"  {'─'*55} {'─'*7} {'─'*7}")
    worst = sorted([p for p in completed if p["est"] and p["est"] < 0], key=lambda x: x["est"])[:10]
    for p in worst:
        print(f"  {RED}{p['title']:<55} {p['spread']*100:>+6.1f}% ${p['est']:>6.2f}{RESET}")

    # ── Seed completion rate ────────────────────────────────────────
    print(f"\n{BOLD}  SEED COMPLETION RATE BY TYPE{RESET}")
    print(f"  {'Type':<16} {'Seeds':>6} {'Completed':>9} {'Rate':>6}")
    print(f"  {'─'*16} {'─'*6} {'─'*9} {'─'*6}")
    for mtype, positions in sorted(by_type.items(), key=lambda x: -len(x[1])):
        total_p  = len(positions)
        comp_p   = len([p for p in positions if p["spread"] is not None])
        rate     = comp_p/total_p*100 if total_p else 0
        c        = GREEN if rate > 60 else (RED if rate < 30 else RESET)
        print(f"  {mtype:<16} {total_p:>6} {comp_p:>9} {c}{rate:>5.0f}%{RESET}")

    # ── Key insight ─────────────────────────────────────────────────
    print(f"\n{BOLD}  KEY INSIGHTS{RESET}")
    prop_neg = [p for p in by_type.get("player_prop",[]) if p["spread"] and p["spread"] < 0]
    prop_all = by_type.get("player_prop",[])
    if prop_all:
        loss = sum(p["est"] for p in prop_neg if p["est"])
        print(f"  • Player props:    {len(prop_neg)}/{len(prop_all)} positions underwater, "
              f"${abs(loss):.2f} in losses — {RED}avoid these{RESET}")
    ml = [p for p in by_type.get("moneyline",[]) if p["spread"] and p["spread"] > 0]
    if ml:
        avg = sum(p["spread"] for p in ml)/len(ml)*100
        print(f"  • Moneyline arbs:  avg {avg:.1f}% spread — {GREEN}best market type{RESET}")
    gt = [p for p in by_type.get("game_total",[]) if p["spread"] and p["spread"] > 0]
    if gt:
        avg = sum(p["spread"] for p in gt)/len(gt)*100
        print(f"  • Game totals:     avg {avg:.1f}% spread")
    spreads_ = [p for p in by_type.get("spread",[]) if p["spread"] and p["spread"] > 0]
    if spreads_:
        avg = sum(p["spread"] for p in spreads_)/len(spreads_)*100
        print(f"  • Point spreads:   avg {avg:.1f}% spread")
    print()

if __name__ == "__main__":
    analyze(load())
