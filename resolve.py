#!/usr/bin/env python3
"""
resolve.py — Manually resolve positions by keyword.

Usage:
  python3.11 resolve.py                          # List all unresolved positions
  python3.11 resolve.py "lakers" "raptors"       # Resolve any matching keywords
  python3.11 resolve.py --all-onesided           # Resolve all one-sided seeds
  python3.11 resolve.py --cost-under 3           # Resolve all positions under $3
"""

import json, os, sys, shutil
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LEDGER_FILE = os.path.join(SCRIPT_DIR, "ledger.json")
BACKUP_DIR = os.path.join(SCRIPT_DIR, "ledger_backups")

with open(LEDGER_FILE) as f:
    ledger = json.load(f)

active = {k: v for k, v in ledger.items() if not v.get("resolved")}

# No args = list mode
if len(sys.argv) == 1:
    print(f"\n  Unresolved positions: {len(active)}\n")
    by_cost = []
    for mid, pos in active.items():
        title = pos.get("title", mid)[:60]
        yc = sum(a for _, a in pos.get("yes_fills", []))
        nc = sum(a for _, a in pos.get("no_fills", []))
        cost = yc + nc
        has_yes = yc > 0
        has_no = nc > 0
        sides = "TWO" if (has_yes and has_no) else ("YES" if has_yes else "NO")
        by_cost.append((cost, title, sides, mid))
    by_cost.sort(key=lambda x: -x[0])
    for cost, title, sides, mid in by_cost:
        print(f"  ${cost:7.2f}  [{sides:3s}]  {title}")
    print(f"\n  Usage: python3.11 resolve.py \"keyword1\" \"keyword2\" ...")
    print(f"         python3.11 resolve.py --win \"keyword\"     # mark as win (calculates profit)")
    print(f"         python3.11 resolve.py --all-onesided")
    print(f"         python3.11 resolve.py --cost-under 3")
    sys.exit(0)

# Parse args
keywords = []
all_onesided = False
cost_under = None

assume_win = False

i = 1
while i < len(sys.argv):
    arg = sys.argv[i]
    if arg == "--all-onesided":
        all_onesided = True
    elif arg == "--win":
        assume_win = True
    elif arg == "--cost-under" and i + 1 < len(sys.argv):
        cost_under = float(sys.argv[i + 1])
        i += 1
    else:
        keywords.append(arg.lower())
    i += 1

# Find matches
to_resolve = []
for mid, pos in active.items():
    title = pos.get("title", "")
    yc = sum(a for _, a in pos.get("yes_fills", []))
    nc = sum(a for _, a in pos.get("no_fills", []))
    cost = yc + nc
    has_yes = yc > 0
    has_no = nc > 0
    is_onesided = (has_yes != has_no)

    match = False
    if keywords and any(kw in title.lower() for kw in keywords):
        match = True
    if all_onesided and is_onesided:
        match = True
    if cost_under is not None and cost <= cost_under:
        match = True

    if match:
        sides = "TWO" if (has_yes and has_no) else ("YES" if has_yes else "NO")
        to_resolve.append((mid, title, cost, sides))

if not to_resolve:
    print("\n  No matching positions found.")
    sys.exit(0)

print(f"\n  Will resolve {len(to_resolve)} positions:\n")
total_cost = 0
for mid, title, cost, sides in sorted(to_resolve, key=lambda x: -x[2]):
    print(f"  ${cost:7.2f}  [{sides:3s}]  {title[:60]}")
    total_cost += cost

print(f"\n  Total cost of resolved positions: ${total_cost:.2f}")
print(f"\n  Confirm? (y/n): ", end="", flush=True)
confirm = input().strip().lower()

if confirm != "y":
    print("  Cancelled.")
    sys.exit(0)

# Backup and apply
os.makedirs(BACKUP_DIR, exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
shutil.copy2(LEDGER_FILE, os.path.join(BACKUP_DIR, f"ledger_{ts}_manual.json"))

for mid, title, cost, sides in to_resolve:
    ledger[mid]["resolved"] = True
    ledger[mid]["stagnant"] = True
    if assume_win and sides == "TWO":
        # Two-sided arb: payout = max of YES shares or NO shares (whichever wins)
        yf = ledger[mid].get("yes_fills", [])
        nf = ledger[mid].get("no_fills", [])
        yes_shares = sum(a / p for p, a in yf if p > 0)
        no_shares = sum(a / p for p, a in nf if p > 0)
        payout = max(yes_shares, no_shares)  # guaranteed payout from arb
        ledger[mid]["resolved_pl"] = round(payout - cost, 2)
    elif assume_win:
        # One-sided: assume our side won
        fills = ledger[mid].get("yes_fills") or ledger[mid].get("no_fills") or []
        shares = sum(a / p for p, a in fills if p > 0)
        ledger[mid]["resolved_pl"] = round(shares - cost, 2)
    else:
        ledger[mid]["resolved_pl"] = round(-cost, 2)  # conservative: assume loss

with open(LEDGER_FILE, "w") as f:
    json.dump(ledger, f, indent=2)

remaining = sum(1 for v in ledger.values() if not v.get("resolved"))
print(f"\n  Done! Resolved {len(to_resolve)} positions.")
print(f"  Active positions remaining: {remaining}")
