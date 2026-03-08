#!/usr/bin/env python3
"""
cleanup_positions.py — Resolve finished positions using CLOB API.

Uses CLOB /markets/{condition_id} to check closed status.
Uses data-api /positions to check current value (value=0 = lost).

Usage:
  python3.11 cleanup_positions.py              # Show what would change
  python3.11 cleanup_positions.py --apply      # Apply resolutions
  python3.11 cleanup_positions.py --debug      # Show raw API data
"""

import json, os, sys, time, shutil, argparse
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LEDGER_FILE = os.path.join(SCRIPT_DIR, "ledger.json")
BACKUP_DIR = os.path.join(SCRIPT_DIR, "ledger_backups")
CLOB = "https://clob.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

PROXY_WALLET = os.getenv("PROXY_WALLET", "")
for fname in ["keys.env", ".env"]:
    fpath = os.path.join(SCRIPT_DIR, fname)
    if not os.path.exists(fpath): continue
    with open(fpath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            if line.startswith("export "): line = line[7:]
            if "=" not in line: continue
            k, v = line.split("=", 1)
            os.environ[k] = v.strip().strip('"').strip("'")
PROXY_WALLET = os.getenv("PROXY_WALLET", PROXY_WALLET)

try:
    import httpx
    _session = httpx.Client(timeout=15, follow_redirects=True)
    def http_get(url, params=None):
        r = _session.get(url, params=params or {})
        return r.json() if r.status_code == 200 else None
except ImportError:
    import urllib.request, urllib.parse
    def http_get(url, params=None):
        if params: url += "?" + urllib.parse.urlencode(params)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "PolyArb/1.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read())
        except: return None


def calculate_pl(pos_data: dict, winner: str) -> float:
    yes_fills = pos_data.get("yes_fills", [])
    no_fills = pos_data.get("no_fills", [])
    yes_cost = sum(amt for _, amt in yes_fills)
    no_cost = sum(amt for _, amt in no_fills)
    total_cost = yes_cost + no_cost
    if winner == "YES" and yes_fills:
        payout = sum(amt / price for price, amt in yes_fills if price > 0)
    elif winner == "NO" and no_fills:
        payout = sum(amt / price for price, amt in no_fills if price > 0)
    else:
        payout = 0
    return round(payout - total_cost, 2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--wallet", type=str)
    args = parser.parse_args()

    wallet = args.wallet or PROXY_WALLET

    with open(LEDGER_FILE) as f:
        ledger = json.load(f)
    active = {k: v for k, v in ledger.items() if not v.get("resolved")}

    print(f"\n{'='*70}")
    print(f"  POSITION CLEANUP (CLOB API)")
    print(f"  Ledger: {len(active)} active positions")
    print(f"  Wallet: {wallet[:10]}...{wallet[-6:]}" if wallet else "  Wallet: not set")
    print(f"{'='*70}\n")

    to_resolve = []
    still_open = []
    total_pl = 0

    for i, (mid, pos) in enumerate(active.items()):
        title = pos.get("title", mid)[:55]
        yes_fills = pos.get("yes_fills", [])
        no_fills = pos.get("no_fills", [])
        yes_cost = sum(a for _, a in yes_fills)
        no_cost = sum(a for _, a in no_fills)
        cost = yes_cost + no_cost
        has_yes = yes_cost > 0
        has_no = no_cost > 0

        if i > 0 and i % 10 == 0:
            time.sleep(0.3)
            sys.stdout.write(f"  Checking {i}/{len(active)}...\r")
            sys.stdout.flush()

        # Step 1: CLOB API — is market closed?
        clob = http_get(f"{CLOB}/markets/{mid}")
        if not clob or not isinstance(clob, dict) or not clob.get("condition_id"):
            still_open.append((mid, title, cost))
            continue

        is_closed = clob.get("closed", False)
        accepting = clob.get("accepting_orders", True)

        if not is_closed and accepting:
            still_open.append((mid, title, cost))
            continue

        # Market is CLOSED.
        winner = None
        pl = None

        # Step 2: data-api — check position value
        if wallet:
            pos_info = http_get(f"{DATA_API}/positions", params={
                "user": wallet.lower(),
                "market": mid,
            })
            if pos_info and isinstance(pos_info, list) and pos_info:
                p = pos_info[0]
                value = float(p.get("currentValue") or p.get("value") or 0)
                size = float(p.get("size") or 0)
                outcome = p.get("outcome", "")

                if args.debug:
                    print(f"  DEBUG: {title[:40]}")
                    print(f"    outcome={outcome} size={size:.2f} value={value:.2f} cost=${cost:.2f}")

                if value < 0.01:
                    # Worthless = LOST
                    pl = round(-cost, 2)
                elif value >= cost * 0.8:
                    # Has value = probably WON
                    if has_yes and not has_no:
                        pl = calculate_pl(pos, "YES")
                        winner = "YES"
                    elif has_no and not has_yes:
                        pl = calculate_pl(pos, "NO")
                        winner = "NO"
                    else:
                        pl = round(value - cost, 2)
                        winner = "WIN" if pl >= 0 else "LOSS"
                else:
                    # Partial value — unclear
                    pl = round(-cost, 2)
            else:
                # Not found in data-api
                pl = round(-cost, 2)
        else:
            pl = round(-cost, 2)

        total_pl += pl
        emoji = "+" if pl >= 0 else "-"
        to_resolve.append((mid, pl, winner))
        tag = winner or ("LOSS" if pl < 0 else "WIN")
        print(f"  [{emoji}] {title}")
        print(f"      {tag} | Cost ${cost:.2f} | P&L ${pl:+.2f}")

    sys.stdout.write(" " * 40 + "\r")  # clear progress line
    print(f"\n{'='*70}")
    print(f"  SUMMARY")
    print(f"{'='*70}")
    print(f"  To resolve:  {len(to_resolve)}")
    print(f"  Still open:  {len(still_open)}")
    print(f"  Net P&L:     ${total_pl:+.2f}")

    open_cost = sum(c for _, _, c in still_open)
    print(f"  Remaining $: ${open_cost:.2f}")

    if still_open:
        print(f"\n  Still open ({len(still_open)}):")
        for _, title, cost in sorted(still_open, key=lambda x: -x[2])[:20]:
            print(f"    ${cost:7.2f}  {title}")

    if args.apply and to_resolve:
        print(f"\n  Applying {len(to_resolve)} resolutions...")
        os.makedirs(BACKUP_DIR, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.copy2(LEDGER_FILE, os.path.join(BACKUP_DIR, f"ledger_{ts}_cleanup.json"))
        for mid, pl, winner in to_resolve:
            if mid in ledger:
                ledger[mid]["resolved"] = True
                ledger[mid]["stagnant"] = True
                ledger[mid]["resolved_pl"] = pl
        with open(LEDGER_FILE, "w") as f:
            json.dump(ledger, f, indent=2)
        remaining = sum(1 for v in ledger.values() if not v.get("resolved"))
        print(f"  Done! Active positions: {remaining}")
    elif to_resolve:
        print(f"\n  Run with --apply to resolve {len(to_resolve)} positions")


if __name__ == "__main__":
    main()
