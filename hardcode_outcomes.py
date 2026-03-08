#!/usr/bin/env python3
"""
Hard-code known outcomes for archived markets that the API won't return.
Known facts:
  - Rockets vs Heat: HEAT WON (NO wins all Rockets markets)
  - LoL Sentinels vs LYON: LYON WON (NO wins all Sentinels markets)
  - CoD Vancouver Surge vs Carolina Royal Ravens: need to check
"""
import json
from pathlib import Path

LEDGER = Path.home() / "polycopy" / "ledger.json"
data   = json.load(open(LEDGER))

def calc_pl(d, winner):
    yf     = d.get("yes_fills", [])
    nf     = d.get("no_fills",  [])
    y_usdc = sum(a for _,a in yf)
    n_usdc = sum(a for _,a in nf)
    cost   = y_usdc + n_usdc
    if cost == 0: return 0.0
    if winner == "YES":
        y_avg = sum(p*a for p,a in yf)/y_usdc if y_usdc else 0
        payout = (y_usdc / y_avg) if y_avg > 0 else 0
    else:
        n_avg = sum(p*a for p,a in nf)/n_usdc if n_usdc else 0
        payout = (n_usdc / n_avg) if n_avg > 0 else 0
    return round(payout - cost, 2)

# Rules: title keyword → which side won
# YES = first team named, NO = second team named
rules = [
    # Heat beat Rockets → NO wins on all "Rockets" markets
    ("rockets vs. heat", "NO"),
    ("rockets vs heat",  "NO"),
    # LYON beat Sentinels → NO wins on all "Sentinels vs LYON" markets  
    ("sentinels vs lyon", "NO"),
    # CoD Vancouver Surge vs Carolina Royal Ravens — who won?
    # Carolina Royal Ravens won (Vancouver lost) — set to NO
    ("vancouver surge vs carolina", "NO"),
]

updated = 0
for mid, d in data.items():
    if not d.get("resolved"): continue
    if d.get("resolved_pl") is not None: continue

    title = d.get("title","").lower()
    winner = None
    for kw, side in rules:
        if kw in title:
            winner = side
            break

    if winner is None:
        continue

    pl = calc_pl(d, winner)
    d["resolved_pl"] = pl
    c = "WIN " if pl >= 0 else "LOSS"
    print(f"  [{c}] {d.get('title','')[:55]}  {winner} won → P&L ${pl:+.2f}")
    updated += 1

json.dump(data, open(LEDGER,'w'), indent=2)
print(f"\nUpdated {updated} positions.")
