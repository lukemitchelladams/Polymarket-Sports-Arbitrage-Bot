#!/usr/bin/env python3
"""
RN1 DEEP ANALYSIS
=================
Processes the trade data pulled by rn1_history_puller.py.
Extracts exact strategy parameters we can copy.

Run after the puller:
    python3 rn1_deep_analysis.py

Reads from: ~/rn1_analysis/rn1_all_trades.csv
"""

import csv
import json
import os
from collections import defaultdict
from datetime import datetime, timezone, timedelta

OUTPUT_DIR = os.path.expanduser("~/rn1_analysis")
CSV_PATH = os.path.join(OUTPUT_DIR, "rn1_all_trades.csv")
REPORT_PATH = os.path.join(OUTPUT_DIR, "rn1_strategy_report.txt")


def load_trades():
    """Load the unified trades CSV."""
    trades = []
    with open(CSV_PATH, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                row["price"] = float(row.get("price") or 0)
                row["size"] = float(row.get("size") or 0)
                row["cost"] = float(row.get("cost") or 0)
                row["shares"] = float(row.get("shares") or 0)
                row["epoch"] = float(row.get("epoch") or 0)
                trades.append(row)
            except:
                pass
    return trades


def analyze_bankroll_growth(trades):
    """Reconstruct approximate bankroll over time."""
    print("\n" + "=" * 70)
    print("BANKROLL GROWTH RECONSTRUCTION")
    print("=" * 70)
    
    # Group by week
    weekly = defaultdict(lambda: {"buys": 0, "volume": 0, "trades": 0, "markets": set()})
    
    for t in trades:
        if t["epoch"] <= 0: continue
        dt = datetime.fromtimestamp(t["epoch"], tz=timezone.utc)
        # ISO week
        week = dt.strftime("%Y-W%W")
        weekly[week]["trades"] += 1
        weekly[week]["volume"] += t["cost"]
        weekly[week]["markets"].add(t.get("market_id", ""))
    
    print(f"\n  {'Week':10s} {'Trades':>8s} {'Volume':>12s} {'Markets':>8s} {'Implied BRL':>12s}")
    print(f"  {'─'*55}")
    
    cumulative_volume = 0
    for week in sorted(weekly.keys()):
        d = weekly[week]
        cumulative_volume += d["volume"]
        # Rough bankroll estimate: daily volume / 3 (RN1 turns over ~3x bankroll/day)
        # Weekly volume / 21 (3x/day * 7 days)
        implied_bankroll = d["volume"] / 21 if d["volume"] > 0 else 0
        print(f"  {week:10s} {d['trades']:>8,d} ${d['volume']:>11,.0f} "
              f"{len(d['markets']):>8d} ${implied_bankroll:>11,.0f}")
    
    return weekly


def analyze_strategy_phases(trades):
    """Identify how his strategy evolved over time."""
    print("\n" + "=" * 70)
    print("STRATEGY PHASE ANALYSIS")
    print("=" * 70)
    
    # Group by month and analyze price distribution
    monthly = defaultdict(list)
    for t in trades:
        if t["epoch"] <= 0 or t["price"] <= 0: continue
        dt = datetime.fromtimestamp(t["epoch"], tz=timezone.utc)
        month = dt.strftime("%Y-%m")
        monthly[month].append(t)
    
    print(f"\n  {'Month':8s} {'n':>6s} {'Vol':>10s} "
          f"{'<0.10':>7s} {'0.10-0.30':>9s} {'0.30-0.70':>9s} {'0.70-0.90':>9s} {'>0.90':>7s} "
          f"{'AvgSize':>8s}")
    print(f"  {'─'*80}")
    
    for month in sorted(monthly.keys()):
        ts = monthly[month]
        n = len(ts)
        vol = sum(t["cost"] for t in ts)
        
        p_very_low = sum(1 for t in ts if t["price"] <= 0.10) / n * 100
        p_low = sum(1 for t in ts if 0.10 < t["price"] <= 0.30) / n * 100
        p_mid = sum(1 for t in ts if 0.30 < t["price"] <= 0.70) / n * 100
        p_high = sum(1 for t in ts if 0.70 < t["price"] <= 0.90) / n * 100
        p_very_high = sum(1 for t in ts if t["price"] > 0.90) / n * 100
        
        avg_size = vol / n if n > 0 else 0
        
        print(f"  {month:8s} {n:>6,d} ${vol:>9,.0f} "
              f"{p_very_low:>6.1f}% {p_low:>8.1f}% {p_mid:>8.1f}% {p_high:>8.1f}% {p_very_high:>6.1f}% "
              f"${avg_size:>7,.1f}")


def extract_strategy_params(trades):
    """Extract the exact parameters we need to copy."""
    print("\n" + "=" * 70)
    print("EXTRACTABLE STRATEGY PARAMETERS")
    print("=" * 70)
    
    # Only look at recent 30 days (his current mature strategy)
    if not trades:
        return {}
    
    recent_cutoff = max(t["epoch"] for t in trades if t["epoch"] > 0) - (30 * 86400)
    recent = [t for t in trades if t["epoch"] > recent_cutoff and t["price"] > 0]
    
    if not recent:
        print("  No recent trades with price data")
        return {}
    
    total_vol = sum(t["cost"] for t in recent)
    n = len(recent)
    
    print(f"\n  Analyzing last 30 days: {n:,d} trades, ${total_vol:,.0f} volume")
    
    # Price bucket analysis
    buckets = {
        "penny (0.01-0.08)": [t for t in recent if 0.005 < t["price"] <= 0.08],
        "low (0.08-0.20)": [t for t in recent if 0.08 < t["price"] <= 0.20],
        "mid-low (0.20-0.40)": [t for t in recent if 0.20 < t["price"] <= 0.40],
        "mid (0.40-0.60)": [t for t in recent if 0.40 < t["price"] <= 0.60],
        "mid-high (0.60-0.80)": [t for t in recent if 0.60 < t["price"] <= 0.80],
        "high (0.80-0.90)": [t for t in recent if 0.80 < t["price"] <= 0.90],
        "near-certain (0.90-0.98)": [t for t in recent if 0.90 < t["price"] <= 0.98],
    }
    
    print(f"\n  VOLUME BY PRICE BUCKET (last 30 days):")
    for bucket_name, bucket_trades in buckets.items():
        bvol = sum(t["cost"] for t in bucket_trades)
        pct = bvol / total_vol * 100 if total_vol > 0 else 0
        avg_size = bvol / len(bucket_trades) if bucket_trades else 0
        print(f"  {bucket_name:25s}: {len(bucket_trades):>6,d} trades | "
              f"${bvol:>12,.0f} ({pct:>5.1f}%) | avg=${avg_size:>8,.2f}")
    
    # Time-of-day analysis
    print(f"\n  HOURLY VOLUME (UTC, last 30 days):")
    hourly = defaultdict(float)
    for t in recent:
        dt = datetime.fromtimestamp(t["epoch"], tz=timezone.utc)
        hourly[dt.hour] += t["cost"]
    
    for h in range(24):
        vol = hourly.get(h, 0)
        pct = vol / total_vol * 100 if total_vol > 0 else 0
        bar = "█" * int(pct / 2)
        print(f"  {h:02d}:00 ${vol:>12,.0f} ({pct:>5.1f}%) {bar}")
    
    # Day of week
    print(f"\n  DAILY VOLUME (last 30 days):")
    daily = defaultdict(float)
    daily_count = defaultdict(int)
    for t in recent:
        dt = datetime.fromtimestamp(t["epoch"], tz=timezone.utc)
        day = dt.strftime("%A")
        daily[day] += t["cost"]
        daily_count[day] += 1
    
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    for d in day_order:
        vol = daily.get(d, 0)
        n_day = daily_count.get(d, 0)
        print(f"  {d:12s}: ${vol:>12,.0f} | {n_day:>6,d} trades")
    
    # Extract recommended parameters
    params = {
        "total_30d_volume": total_vol,
        "total_30d_trades": n,
        "avg_trade_size": total_vol / n if n > 0 else 0,
        "near_certain_pct": sum(t["cost"] for t in buckets["near-certain (0.90-0.98)"]) / total_vol * 100 if total_vol > 0 else 0,
        "penny_pct": sum(t["cost"] for t in buckets["penny (0.01-0.08)"]) / total_vol * 100 if total_vol > 0 else 0,
        "mid_pct": sum(t["cost"] for t in buckets["mid (0.40-0.60)"]) / total_vol * 100 if total_vol > 0 else 0,
    }
    
    print(f"\n  RECOMMENDED BOT PARAMETERS (scaled to $300 bankroll):")
    scale = 300 / (total_vol / 30)  # our daily budget vs his
    print(f"  Scale factor: {scale:.6f} (our daily budget / his)")
    print(f"  NC trade size: ${max(1, params['avg_trade_size'] * scale * 2):.2f}")
    print(f"  Penny trade size: ${max(0.50, params['avg_trade_size'] * scale * 0.3):.2f}")
    print(f"  Seed size: ${max(1, params['avg_trade_size'] * scale):.2f}")
    
    return params


def main():
    if not os.path.exists(CSV_PATH):
        print(f"ERROR: {CSV_PATH} not found!")
        print("Run rn1_history_puller.py first.")
        return
    
    print("=" * 70)
    print("  RN1 DEEP STRATEGY ANALYSIS")
    print("=" * 70)
    
    trades = load_trades()
    print(f"  Loaded {len(trades):,d} trades")
    
    weekly = analyze_bankroll_growth(trades)
    analyze_strategy_phases(trades)
    params = extract_strategy_params(trades)
    
    # Save report
    print(f"\n  Analysis complete. Key files in {OUTPUT_DIR}/")


if __name__ == "__main__":
    main()
