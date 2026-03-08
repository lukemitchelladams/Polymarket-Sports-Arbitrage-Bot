#!/usr/bin/env python3
"""Fix a resolved position's P&L. Usage: python3.11 fix_pl.py "keyword" <pl_amount>"""
import json, sys, os

LEDGER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ledger.json")

if len(sys.argv) < 3:
    print("Usage: python3.11 fix_pl.py \"keyword\" <pl_amount>")
    print("  e.g. python3.11 fix_pl.py \"reaves\" 45.34")
    sys.exit(1)

keyword = sys.argv[1].lower()
pl = float(sys.argv[2])

with open(LEDGER) as f:
    ledger = json.load(f)

found = 0
for mid, pos in ledger.items():
    title = pos.get("title", "").lower()
    if keyword in title:
        old_pl = pos.get("resolved_pl", "N/A")
        pos["resolved_pl"] = pl
        print(f"  Fixed: {pos.get('title','')[:55]}")
        print(f"    Old P&L: ${old_pl}  →  New P&L: ${pl:+.2f}")
        found += 1

if found:
    with open(LEDGER, "w") as f:
        json.dump(ledger, f, indent=2)
    print(f"\n  Saved {found} fix(es).")
else:
    print(f"  No match for '{keyword}'")
