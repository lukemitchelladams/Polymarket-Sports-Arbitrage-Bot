#!/usr/bin/env python3
"""
Fetch actual resolution outcomes from Polymarket API for one-sided resolved positions.
Updates ledger.json with correct resolved_pl so watch.py shows real P&L.

Usage: python3.11 ~/polycopy/fetch_outcomes.py
"""
import json, time, requests
from pathlib import Path

LEDGER = Path.home() / "polycopy" / "ledger.json"
data   = json.load(open(LEDGER))

updated = 0
for mid, d in data.items():
    if not d.get("resolved"):
        continue
    if d.get("resolved_pl") is not None:
        continue  # already known

    yf = d.get("yes_fills", [])
    nf = d.get("no_fills",  [])
    y_usdc = sum(a for _,a in yf)
    n_usdc = sum(a for _,a in nf)
    cost   = y_usdc + n_usdc
    if cost == 0:
        continue

    title  = d.get("title","")[:60]

    # Query Polymarket market API for resolution
    try:
        url = f"https://clob.polymarket.com/markets/{mid}"
        r   = requests.get(url, timeout=5)
        if r.status_code != 200:
            print(f"  SKIP {title[:50]} — API {r.status_code}")
            continue
        mkt = r.json()

        # Check tokens for resolution prices
        tokens = mkt.get("tokens", [])
        yes_tok_id = d.get("yes_token")
        no_tok_id  = d.get("no_token")

        winner = None
        for tok in tokens:
            tid = tok.get("token_id","")
            outcome = tok.get("outcome","").upper()
            winner_flag = tok.get("winner", False)
            if winner_flag:
                if tid == yes_tok_id or outcome == "YES":
                    winner = "YES"
                elif tid == no_tok_id or outcome == "NO":
                    winner = "NO"

        if winner is None:
            # fallback: check if market has a resolved price
            for tok in tokens:
                price = float(tok.get("price", 0.5))
                outcome = tok.get("outcome","").upper()
                if price >= 0.99:
                    winner = outcome if outcome in ("YES","NO") else None

        if winner == "YES":
            y_avg = sum(p*a for p,a in yf)/y_usdc if y_usdc else 0
            payout = (y_usdc / y_avg) if y_avg > 0 else 0
            pl = round(payout - cost, 2)
        elif winner == "NO":
            n_avg = sum(p*a for p,a in nf)/n_usdc if n_usdc else 0
            payout = (n_usdc / n_avg) if n_avg > 0 else 0
            pl = round(payout - cost, 2)
        else:
            print(f"  UNKNOWN outcome: {title[:50]}")
            continue

        d["resolved_pl"] = pl
        c = "WIN" if pl >= 0 else "LOSS"
        print(f"  [{c}] {title:<55} {winner} won → P&L ${pl:+.2f}")
        updated += 1
        time.sleep(0.1)  # rate limit

    except Exception as e:
        print(f"  ERROR {title[:50]}: {e}")

json.dump(data, open(LEDGER, 'w'), indent=2)
print(f"\nUpdated {updated} positions. Restart watch.py to see changes.")
