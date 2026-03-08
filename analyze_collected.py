#!/usr/bin/env python3
"""
Analyze Collected Data — Threshold Win Rate Analysis
=====================================================

Reads from collector_data/sports.db (populated by collector.py).

Answers: "When a price for an outcome hits X for Y minutes, what are the
odds that team wins/loses?"

Run: python3 analyze_collected.py
"""

import os
import sys
import sqlite3
from collections import defaultdict

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "collector_data", "sports.db")

# Thresholds to test
HAMMER_THRESHOLDS = [0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90]
SUSTAIN_MINUTES = [1, 2, 3, 4, 5, 6, 8, 10]

TRIM_THRESHOLDS = [0.20, 0.25, 0.30, 0.35, 0.40, 0.45]
TRIM_MINUTES = [1, 2, 3, 4, 5]

HAMMER_SIZE = 15.0


def connect():
    if not os.path.exists(DB_FILE):
        print(f"Database not found: {DB_FILE}")
        print("Run collector.py first to collect data.")
        sys.exit(1)
    return sqlite3.connect(DB_FILE)


def get_stats(conn):
    """Print database stats."""
    c = conn.cursor()
    n_markets = c.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    n_resolved = c.execute("SELECT COUNT(*) FROM markets WHERE resolved_at > 0").fetchone()[0]
    n_ticks = c.execute("SELECT COUNT(*) FROM ticks").fetchone()[0]
    print(f"Database: {n_markets} markets, {n_resolved} resolved, {n_ticks:,} price ticks")
    return n_resolved


def get_resolved_markets(conn):
    """Get all resolved markets with their info."""
    c = conn.cursor()
    rows = c.execute("""
        SELECT mid, title, typ, sport, tok_no, tok_yes, vol24, winner
        FROM markets WHERE resolved_at > 0 AND winner != ''
    """).fetchall()
    return [{"mid": r[0], "title": r[1], "typ": r[2], "sport": r[3],
             "tok_no": r[4], "tok_yes": r[5], "vol24": r[6], "winner": r[7]}
            for r in rows]


def get_ticks(conn, mid):
    """Get price ticks for a market, ordered by time."""
    c = conn.cursor()
    rows = c.execute(
        "SELECT ts, no_bid, no_ask, no_depth, yes_bid, yes_ask, yes_depth "
        "FROM ticks WHERE mid=? ORDER BY ts", (mid,)).fetchall()
    return [{"ts": r[0], "no_bid": r[1], "no_ask": r[2], "no_depth": r[3],
             "yes_bid": r[4], "yes_ask": r[5], "yes_depth": r[6]}
            for r in rows]


def check_sustained_above(ticks, side_key, threshold, sustain_secs):
    """Check if a side ever stayed above threshold for sustain_secs.
    Returns (True/False, first_qualify_ts, price_at_qualify).
    """
    above_since = 0
    for tick in ticks:
        price = tick[side_key]
        if price >= threshold:
            if above_since == 0:
                above_since = tick["ts"]
            elapsed = tick["ts"] - above_since
            if elapsed >= sustain_secs:
                return True, above_since, price
        else:
            above_since = 0
    return False, 0, 0


def check_sustained_below(ticks, side_key, threshold, sustain_secs):
    """Check if a side ever stayed below threshold for sustain_secs."""
    below_since = 0
    for tick in ticks:
        price = tick[side_key]
        if 0 < price < threshold:
            if below_since == 0:
                below_since = tick["ts"]
            elapsed = tick["ts"] - below_since
            if elapsed >= sustain_secs:
                return True, below_since, price
        else:
            below_since = 0
    return False, 0, 0


def analyze_hammer(markets, conn):
    """Analyze hammer win rates across all threshold/duration combos."""
    print("\n" + "=" * 90)
    print("HAMMER ANALYSIS: When a side hits X% for Y minutes, how often does it WIN?")
    print("=" * 90)

    # {threshold: {sustain_min: {"wins": N, "losses": N, "by_type": {}, "by_sport": {}}}}
    results = defaultdict(lambda: defaultdict(lambda: {
        "wins": 0, "losses": 0,
        "by_type": defaultdict(lambda: {"w": 0, "l": 0}),
        "by_sport": defaultdict(lambda: {"w": 0, "l": 0}),
        "by_vol": {"high": {"w": 0, "l": 0}, "low": {"w": 0, "l": 0}},
    }))

    for mkt in markets:
        ticks = get_ticks(conn, mkt["mid"])
        if len(ticks) < 3:
            continue

        winner = mkt["winner"]
        vol_cat = "high" if mkt["vol24"] > 50000 else "low"

        for threshold in HAMMER_THRESHOLDS:
            for sustain_min in SUSTAIN_MINUTES:
                sustain_secs = sustain_min * 60
                r = results[threshold][sustain_min]

                # Check YES side
                yes_hit, _, _ = check_sustained_above(ticks, "yes_bid", threshold, sustain_secs)
                if yes_hit:
                    if winner == "YES":
                        r["wins"] += 1
                        r["by_type"][mkt["typ"]]["w"] += 1
                        r["by_sport"][mkt["sport"]]["w"] += 1
                        r["by_vol"][vol_cat]["w"] += 1
                    else:
                        r["losses"] += 1
                        r["by_type"][mkt["typ"]]["l"] += 1
                        r["by_sport"][mkt["sport"]]["l"] += 1
                        r["by_vol"][vol_cat]["l"] += 1

                # Check NO side
                no_hit, _, _ = check_sustained_above(ticks, "no_bid", threshold, sustain_secs)
                if no_hit:
                    if winner == "NO":
                        r["wins"] += 1
                        r["by_type"][mkt["typ"]]["w"] += 1
                        r["by_sport"][mkt["sport"]]["w"] += 1
                        r["by_vol"][vol_cat]["w"] += 1
                    else:
                        r["losses"] += 1
                        r["by_type"][mkt["typ"]]["l"] += 1
                        r["by_sport"][mkt["sport"]]["l"] += 1
                        r["by_vol"][vol_cat]["l"] += 1

    # Print main table
    print(f"\n{'Thresh':>7} | {'Time':>6} | {'N':>5} | {'Wins':>5} | {'Loss':>5} | {'Win%':>6} | {'EV/$15':>8} | {'Verdict':>10}")
    print("-" * 78)

    for threshold in HAMMER_THRESHOLDS:
        for sustain_min in SUSTAIN_MINUTES:
            r = results[threshold][sustain_min]
            total = r["wins"] + r["losses"]
            if total == 0:
                continue
            win_pct = r["wins"] / total * 100

            # EV calculation
            avg_buy = threshold + 0.03  # slippage
            win_profit = (1.0 - avg_buy) * HAMMER_SIZE / avg_buy
            loss_cost = HAMMER_SIZE * 0.65  # assume trim recovers ~35%
            ev = (r["wins"]/total) * win_profit - (r["losses"]/total) * loss_cost

            verdict = "🟢 GO" if ev > 0 and total >= 5 else "🔴 NO" if ev < 0 else "⚪ ?"
            print(f"  {threshold:>5.0%} | {sustain_min:>4}m | {total:>5} | {r['wins']:>5} | {r['losses']:>5} | "
                  f"{win_pct:>5.1f}% | ${ev:>+6.2f} | {verdict}")

        if threshold < HAMMER_THRESHOLDS[-1]:
            print()

    # Breakdown tables
    print("\n" + "=" * 90)
    print("BY SPORT (at 75% threshold, 4 min)")
    print("=" * 90)

    r75_4 = results.get(0.75, {}).get(4, {})
    if r75_4:
        print(f"\n{'Sport':>15} | {'Wins':>5} | {'Loss':>5} | {'Win%':>6}")
        print("-" * 42)
        for sport, counts in sorted(r75_4.get("by_sport", {}).items(), key=lambda x: -(x[1]["w"]+x[1]["l"])):
            total = counts["w"] + counts["l"]
            if total == 0: continue
            pct = counts["w"] / total * 100
            print(f"  {sport:>13} | {counts['w']:>5} | {counts['l']:>5} | {pct:>5.1f}%")

    print("\n" + "=" * 90)
    print("BY MARKET TYPE (at 75% threshold, 4 min)")
    print("=" * 90)

    if r75_4:
        print(f"\n{'Type':>15} | {'Wins':>5} | {'Loss':>5} | {'Win%':>6}")
        print("-" * 42)
        for typ, counts in sorted(r75_4.get("by_type", {}).items(), key=lambda x: -(x[1]["w"]+x[1]["l"])):
            total = counts["w"] + counts["l"]
            if total == 0: continue
            pct = counts["w"] / total * 100
            print(f"  {typ:>13} | {counts['w']:>5} | {counts['l']:>5} | {pct:>5.1f}%")

    print("\n" + "=" * 90)
    print("BY VOLUME (at 75% threshold, 4 min)")
    print("=" * 90)

    if r75_4:
        print(f"\n{'Volume':>15} | {'Wins':>5} | {'Loss':>5} | {'Win%':>6}")
        print("-" * 42)
        for vol, counts in r75_4.get("by_vol", {}).items():
            total = counts["w"] + counts["l"]
            if total == 0: continue
            pct = counts["w"] / total * 100
            label = ">$50k/day" if vol == "high" else "<$50k/day"
            print(f"  {label:>13} | {counts['w']:>5} | {counts['l']:>5} | {pct:>5.1f}%")

    return results


def analyze_trim(markets, conn):
    """Analyze trim accuracy across thresholds."""
    print("\n" + "=" * 90)
    print("TRIM ANALYSIS: When a side drops below X% for Y min, how often is it the LOSER?")
    print("(Higher % = trim is correctly cutting losers)")
    print("=" * 90)

    print(f"\n{'Thresh':>7} | {'Time':>6} | {'N':>5} | {'Correct':>7} | {'Wrong':>6} | {'Accuracy':>8}")
    print("-" * 55)

    for threshold in TRIM_THRESHOLDS:
        for sustain_min in TRIM_MINUTES:
            sustain_secs = sustain_min * 60
            correct = 0; wrong = 0

            for mkt in markets:
                ticks = get_ticks(conn, mkt["mid"])
                if len(ticks) < 3:
                    continue

                winner = mkt["winner"]

                # YES drops below → is it actually the loser?
                yes_below, _, _ = check_sustained_below(ticks, "yes_bid", threshold, sustain_secs)
                if yes_below:
                    if winner != "YES":
                        correct += 1
                    else:
                        wrong += 1

                # NO drops below
                no_below, _, _ = check_sustained_below(ticks, "no_bid", threshold, sustain_secs)
                if no_below:
                    if winner != "NO":
                        correct += 1
                    else:
                        wrong += 1

            total = correct + wrong
            if total == 0:
                continue
            pct = correct / total * 100
            print(f"  {threshold:>5.0%} | {sustain_min:>4}m | {total:>5} | {correct:>7} | {wrong:>6} | {pct:>6.1f}%")

        if threshold < TRIM_THRESHOLDS[-1]:
            print()


def find_best_config(results):
    """Find the optimal hammer configuration."""
    print("\n" + "=" * 90)
    print("OPTIMAL CONFIGURATIONS")
    print("=" * 90)

    best_ev = -999
    best = None

    for threshold in HAMMER_THRESHOLDS:
        for sustain_min in SUSTAIN_MINUTES:
            r = results[threshold][sustain_min]
            total = r["wins"] + r["losses"]
            if total < 10:  # need decent sample
                continue
            win_pct = r["wins"] / total

            avg_buy = threshold + 0.03
            win_profit = (1.0 - avg_buy) * HAMMER_SIZE / avg_buy
            loss_cost = HAMMER_SIZE * 0.65
            ev = win_pct * win_profit - (1 - win_pct) * loss_cost

            if ev > best_ev:
                best_ev = ev
                best = {
                    "threshold": threshold, "sustain_min": sustain_min,
                    "wins": r["wins"], "losses": r["losses"],
                    "win_pct": win_pct * 100, "ev": ev, "total": total
                }

    if best:
        print(f"\n  🏆 BEST HAMMER CONFIG:")
        print(f"     Threshold: {best['threshold']:.0%}")
        print(f"     Sustain:   {best['sustain_min']} minutes")
        print(f"     Win rate:  {best['win_pct']:.1f}% ({best['wins']}W-{best['losses']}L)")
        print(f"     EV/trade:  ${best['ev']:+.2f} per ${HAMMER_SIZE:.0f}")
        print(f"     Sample:    {best['total']} events")
    else:
        print("\n  Not enough data yet for optimal config. Keep collecting.")


def main():
    print("=" * 90)
    print("  POLYMARKET THRESHOLD ANALYSIS — COLLECTED DATA")
    print("=" * 90)

    conn = connect()
    n_resolved = get_stats(conn)

    if n_resolved < 5:
        print(f"\n⚠️  Only {n_resolved} resolved markets. Need at least 5 for analysis.")
        print("Keep collector.py running. Check back in a day or two.\n")
        conn.close()
        sys.exit(0)

    markets = get_resolved_markets(conn)
    print(f"\nAnalyzing {len(markets)} resolved markets...\n")

    # Run analyses
    hammer_results = analyze_hammer(markets, conn)
    analyze_trim(markets, conn)
    find_best_config(hammer_results)

    print("\n" + "=" * 90)
    print("Done! Re-run as more data accumulates for better accuracy.")
    print("=" * 90 + "\n")

    conn.close()


if __name__ == "__main__":
    main()
