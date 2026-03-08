"""
PolyArb — Data Accumulator
============================
Run this daily to keep building the trade_data.csv dataset.
The more data we have, the more accurately we can calibrate the bot.

Run manually:   python accumulate.py
Run on schedule: Add to cron: 0 23 * * * cd ~/polycopy && source venv/bin/activate && python accumulate.py

What it does:
  1. Loads existing trade_data.csv
  2. Fetches only NEW transactions since last run (efficient, no duplicates)
  3. Appends to the CSV and re-runs analysis
  4. Prints updated sizing recommendations for keys.env
"""

import os
import csv
import time
import requests
from datetime import datetime, timezone
from collections import defaultdict
import statistics

API_KEY = os.getenv("POLYGONSCAN_API_KEY", "63RY7B52BH2WVZ89NZ2MEC3AVS8UTJQE1U")
WALLET  = "0x2005d16a84ceefa912d4e380cd32e7ff827875ea"
USDC    = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CSV_FILE = "trade_data.csv"

def fetch_new_transactions(since_timestamp=0):
    """Fetch only transactions newer than since_timestamp."""
    all_txns = []
    page = 1

    print(f"Fetching transactions since {datetime.fromtimestamp(since_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M') if since_timestamp else 'beginning'}...")

    while True:
        r = requests.get("https://api.etherscan.io/v2/api", params={
            "chainid":         137,
            "module":          "account",
            "action":          "tokentx",
            "address":         WALLET,
            "contractaddress": USDC,
            "page":            page,
            "offset":          100,
            "sort":            "desc",
            "apikey":          API_KEY
        }, timeout=15)

        batch = r.json().get("result", [])
        if not batch:
            break

        outgoing = [t for t in batch if t["from"].lower() == WALLET.lower()]

        # Stop if we've gone past our since_timestamp
        if since_timestamp > 0:
            new_ones = [t for t in outgoing if int(t["timeStamp"]) > since_timestamp]
            if len(new_ones) < len(outgoing):
                all_txns.extend(new_ones)
                print(f"  Page {page}: {len(new_ones)} new (reached existing data)")
                break
            all_txns.extend(new_ones)
        else:
            all_txns.extend(outgoing)

        print(f"  Page {page}: {len(outgoing)} transactions (total new: {len(all_txns)})")
        if len(batch) < 100:
            break
        page += 1
        time.sleep(0.25)

    return all_txns

def load_existing():
    """Load existing CSV. Returns (rows, max_timestamp)."""
    if not os.path.exists(CSV_FILE):
        return [], 0

    rows = []
    max_ts = 0
    with open(CSV_FILE) as f:
        for row in csv.DictReader(f):
            rows.append(row)
            ts = int(row.get("timestamp") or 0)
            if ts > max_ts:
                max_ts = ts

    print(f"Loaded {len(rows)} existing transactions (latest: {datetime.fromtimestamp(max_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M') if max_ts else 'none'})")
    return rows, max_ts

def save_csv(rows, new_txns):
    """Append new transactions to CSV."""
    existing_hashes = {r.get("hash", "") for r in rows}
    added = 0

    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not rows:  # Write header if new file
            writer.writerow(["timestamp", "datetime", "amount_usdc", "to", "hash"])

        for t in new_txns:
            h = t["hash"][-12:]
            if h in existing_hashes:
                continue
            amt = int(t["value"]) / 1e6
            dt  = datetime.fromtimestamp(int(t["timeStamp"]), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            writer.writerow([t["timeStamp"], dt, f"{amt:.6f}", t["to"][-12:], h])
            added += 1

    print(f"Added {added} new transactions to {CSV_FILE}")
    return added

def analyze(rows):
    """Full analysis of all accumulated data."""
    if not rows:
        print("No data to analyze.")
        return

    amounts = [float(r["amount_usdc"]) for r in rows if float(r.get("amount_usdc") or 0) > 0]

    print(f"\n{'='*60}")
    print(f"  ACCUMULATED STRATEGY ANALYSIS")
    print(f"  Total transactions: {len(amounts)}")
    print(f"{'='*60}")

    amounts_sorted = sorted(amounts)
    print(f"\n  Min     : ${min(amounts):.2f}")
    print(f"  Max     : ${max(amounts):.2f}")
    print(f"  Median  : ${statistics.median(amounts):.2f}")
    print(f"  Mean    : ${statistics.mean(amounts):.2f}")
    if len(amounts) > 1:
        print(f"  Stdev   : ${statistics.stdev(amounts):.2f}")

    # Size distribution
    buckets = defaultdict(int)
    order = ["<$1","$1-5","$5-10","$10-25","$25-50","$50-100","$100-500","$500-1K","$1K+"]
    for a in amounts:
        if   a < 1:    k = "<$1"
        elif a < 5:    k = "$1-5"
        elif a < 10:   k = "$5-10"
        elif a < 25:   k = "$10-25"
        elif a < 50:   k = "$25-50"
        elif a < 100:  k = "$50-100"
        elif a < 500:  k = "$100-500"
        elif a < 1000: k = "$500-1K"
        else:          k = "$1K+"
        buckets[k] += 1

    print(f"\n  Size distribution:")
    for k in order:
        if k in buckets:
            pct = round(buckets[k] / len(amounts) * 100)
            bar = "█" * (pct // 3)
            print(f"    {k:12s} {bar} {buckets[k]} ({pct}%)")

    # Paired arb events
    by_ts = defaultdict(list)
    for r in rows:
        by_ts[r["timestamp"]].append(float(r.get("amount_usdc") or 0))

    pairs = {ts: amts for ts, amts in by_ts.items() if len(amts) >= 2}
    pair_totals = [sum(amts) for amts in pairs.values()]
    pair_totals.sort()

    if pair_totals:
        print(f"\n  Arb pair events : {len(pair_totals)}")
        print(f"  Median pair cost: ${statistics.median(pair_totals):.2f}")
        print(f"  Mean pair cost  : ${statistics.mean(pair_totals):.2f}")

    # Daily activity
    daily = defaultdict(list)
    for r in rows:
        day = r.get("datetime", "")[:10]
        daily[day].append(float(r.get("amount_usdc") or 0))

    print(f"\n  Daily activity (last 14 days):")
    for day in sorted(daily.keys(), reverse=True)[:14]:
        txns = daily[day]
        print(f"    {day}  {len(txns):5d} trades  total=${sum(txns):10,.2f}  avg=${sum(txns)/len(txns):7.2f}")

    # Sizing recommendations
    med = statistics.median(amounts)
    print(f"\n{'─'*60}")
    print(f"  BOT CALIBRATION RECOMMENDATIONS")
    print(f"{'─'*60}")
    print(f"\n  Based on {len(amounts)} transactions:")
    print(f"  His median single trade: ${med:.2f}")
    print(f"\n  Suggested keys.env settings for different bankrolls:")
    print(f"  {'Bankroll':>12}  {'BASE_UNIT':>10}  {'MAX_BET':>10}  {'Est. trades/hr':>14}")
    print(f"  {'─'*12}  {'─'*10}  {'─'*10}  {'─'*14}")
    for bankroll in [500, 1000, 2500, 5000, 10000]:
        scale    = bankroll / 1000
        base     = round(10 * scale, 2)
        max_bet  = round(50 * scale, 2)
        print(f"  ${bankroll:>11,}  ${base:>9.2f}  ${max_bet:>9.2f}  {'~20-40':>14}")

    print(f"\n{'='*60}\n")

def main():
    print("PolyArb Data Accumulator")
    print(f"Target: {WALLET}\n")

    # Load existing data
    existing_rows, last_timestamp = load_existing()

    # Fetch new transactions
    new_txns = fetch_new_transactions(since_timestamp=last_timestamp)

    if new_txns:
        added = save_csv(existing_rows, new_txns)
        print(f"Dataset updated: {len(existing_rows) + added} total transactions")
    else:
        print("No new transactions found.")

    # Reload and analyze
    all_rows = []
    if os.path.exists(CSV_FILE):
        with open(CSV_FILE) as f:
            all_rows = list(csv.DictReader(f))

    analyze(all_rows)

if __name__ == "__main__":
    main()
