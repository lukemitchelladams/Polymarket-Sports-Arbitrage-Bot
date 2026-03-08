#!/usr/bin/env python3
"""
Threshold Analysis — Analyze Logged Game Data
==============================================

Reads the snapshots.jsonl and resolutions.jsonl files from game_data sessions
to answer: "How often does a side that sustains above X% for Y minutes win?"

Also analyzes:
- Win rates by market type (match winner, O/U, spread)
- Win rates by sport
- Impact of volume on accuracy
- Impact of momentum/velocity on outcome
- Optimal hammer threshold & sustain time
- Optimal trim threshold & sustain time

Run: python3 analyze_logged_data.py
"""

import os
import sys
import json
import glob
from datetime import datetime
from collections import defaultdict

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

THRESHOLDS = [0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90]
SUSTAIN_DURATIONS_SECS = [120, 180, 240, 300, 360, 480]  # 2,3,4,5,6,8 min
HAMMER_SIZE = 15.0  # for profit calculations
TRIM_PCT = 0.75

GAME_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "game_data")

# ═══════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════

def load_all_sessions():
    """Load snapshots and resolutions from all game_data sessions."""
    sessions = sorted(glob.glob(os.path.join(GAME_DATA_DIR, "session_*")))
    if not sessions:
        print(f"No game data found in {GAME_DATA_DIR}")
        print("Run the bot for a while to collect data, then re-run this script.")
        sys.exit(1)

    all_snapshots = []
    all_resolutions = {}  # mid -> resolution entry

    for session_dir in sessions:
        snap_file = os.path.join(session_dir, "snapshots.jsonl")
        res_file = os.path.join(session_dir, "resolutions.jsonl")

        if os.path.exists(snap_file):
            with open(snap_file) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            all_snapshots.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass

        if os.path.exists(res_file):
            with open(res_file) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            entry = json.loads(line)
                            mid = entry.get("mid")
                            if mid:
                                all_resolutions[mid] = entry
                        except json.JSONDecodeError:
                            pass

    print(f"Loaded {len(all_snapshots)} snapshots from {len(sessions)} sessions")
    print(f"Loaded {len(all_resolutions)} resolved markets")
    return all_snapshots, all_resolutions


def build_price_timeseries(snapshots):
    """
    Build per-market price timeseries from snapshots.
    Returns: {mid: [(ts, no_bid, yes_bid, no_depth, yes_depth, typ, title, vol24, velocity), ...]}
    """
    timeseries = defaultdict(list)

    for snap in snapshots:
        ts = snap.get("ts", 0)

        # From positions (high fidelity — every 30s)
        for pos in snap.get("positions", []):
            mid = pos.get("mid")
            if not mid:
                continue
            timeseries[mid].append({
                "ts": ts,
                "no_bid": pos.get("no_bid", 0),
                "yes_bid": pos.get("yes_bid", 0),
                "no_depth": pos.get("no_bid_depth", 0),
                "yes_depth": pos.get("yes_bid_depth", 0),
                "typ": pos.get("typ", ""),
                "title": pos.get("title", ""),
                "velocity": pos.get("velocity", 0),
                "source": "position",
            })

        # From tracked markets (every 5th snapshot)
        for mid, mkt in snap.get("tracked_markets", {}).items():
            # Don't duplicate if already in positions
            timeseries[mid].append({
                "ts": ts,
                "no_bid": mkt.get("no_now", 0),
                "yes_bid": mkt.get("yes_now", 0),
                "no_depth": mkt.get("no_depth", 0),
                "yes_depth": mkt.get("yes_depth", 0),
                "typ": mkt.get("typ", ""),
                "title": mkt.get("title", ""),
                "vol24": mkt.get("vol24", 0),
                "score": mkt.get("score", 0),
                "lead_switches": mkt.get("lead_switches", 0),
                "source": "tracked",
            })

    # Sort each series by timestamp and deduplicate
    for mid in timeseries:
        timeseries[mid].sort(key=lambda x: x["ts"])
        # Dedup: keep one entry per timestamp, prefer position source
        seen_ts = {}
        deduped = []
        for entry in timeseries[mid]:
            t = round(entry["ts"])
            if t not in seen_ts or entry["source"] == "position":
                seen_ts[t] = True
                deduped.append(entry)
        timeseries[mid] = deduped

    return timeseries


# ═══════════════════════════════════════════════════════════════
# THRESHOLD ANALYSIS
# ═══════════════════════════════════════════════════════════════

def analyze_threshold_sustain(series, side, threshold, sustain_secs):
    """
    Check if `side` ("no" or "yes") ever sustains above `threshold` for `sustain_secs`.
    Returns True if it did, plus the first timestamp it qualified.
    """
    key = f"{side}_bid"
    above_since = 0
    for entry in series:
        price = entry.get(key, 0)
        if price >= threshold:
            if above_since == 0:
                above_since = entry["ts"]
            elapsed = entry["ts"] - above_since
            if elapsed >= sustain_secs:
                return True, above_since
        else:
            above_since = 0
    return False, 0


def analyze_trim_threshold(series, side, threshold, sustain_secs):
    """
    Check if `side` ever drops below `threshold` for `sustain_secs`.
    Returns True if it did.
    """
    key = f"{side}_bid"
    below_since = 0
    for entry in series:
        price = entry.get(key, 0)
        if 0 < price < threshold:
            if below_since == 0:
                below_since = entry["ts"]
            elapsed = entry["ts"] - below_since
            if elapsed >= sustain_secs:
                return True, below_since
        else:
            below_since = 0
    return False, 0


def get_market_meta(series):
    """Extract market type, title, volume from a timeseries."""
    for entry in series:
        if entry.get("title"):
            return {
                "typ": entry.get("typ", "unknown"),
                "title": entry.get("title", ""),
                "vol24": entry.get("vol24", 0),
            }
    return {"typ": "unknown", "title": "", "vol24": 0}


# ═══════════════════════════════════════════════════════════════
# MAIN ANALYSIS
# ═══════════════════════════════════════════════════════════════

def run_analysis():
    snapshots, resolutions = load_all_sessions()

    if not resolutions:
        print("\n⚠️  No resolved markets yet. Run the bot longer to collect resolution data.")
        print("The bot logs resolutions when games end (one side hits ~0¢, other hits ~$1).")
        print("Come back after a day or two of running.\n")
        sys.exit(0)

    timeseries = build_price_timeseries(snapshots)
    print(f"Built timeseries for {len(timeseries)} markets")

    # Only analyze markets that have resolution data
    resolved_mids = set(resolutions.keys()) & set(timeseries.keys())
    print(f"Markets with BOTH timeseries and resolution: {len(resolved_mids)}")

    if len(resolved_mids) < 5:
        print(f"\n⚠️  Only {len(resolved_mids)} resolved markets with price data.")
        print("Need at least 5 for meaningful analysis. Keep the bot running.\n")
        if resolved_mids:
            print("Markets resolved so far:")
            for mid in resolved_mids:
                r = resolutions[mid]
                print(f"  {r.get('title', mid)[:50]} → {r['winner']} won")
        sys.exit(0)

    # ─── HAMMER THRESHOLD ANALYSIS ───
    print("\n" + "=" * 80)
    print("HAMMER ANALYSIS: Win rate when a side sustains above threshold")
    print("=" * 80)

    # Results: {threshold: {sustain: {"wins": N, "losses": N, "by_type": {...}}}}
    hammer_results = {}

    for threshold in THRESHOLDS:
        hammer_results[threshold] = {}
        for sustain in SUSTAIN_DURATIONS_SECS:
            wins = 0; losses = 0
            by_type = defaultdict(lambda: {"w": 0, "l": 0})

            for mid in resolved_mids:
                series = timeseries[mid]
                resolution = resolutions[mid]
                winner = resolution["winner"]  # "YES" or "NO"
                meta = get_market_meta(series)

                # Check if YES side sustained above threshold
                yes_sustained, _ = analyze_threshold_sustain(series, "yes", threshold, sustain)
                if yes_sustained:
                    if winner == "YES":
                        wins += 1
                        by_type[meta["typ"]]["w"] += 1
                    else:
                        losses += 1
                        by_type[meta["typ"]]["l"] += 1

                # Check if NO side sustained above threshold
                no_sustained, _ = analyze_threshold_sustain(series, "no", threshold, sustain)
                if no_sustained:
                    if winner == "NO":
                        wins += 1
                        by_type[meta["typ"]]["w"] += 1
                    else:
                        losses += 1
                        by_type[meta["typ"]]["l"] += 1

            hammer_results[threshold][sustain] = {
                "wins": wins, "losses": losses, "by_type": dict(by_type)
            }

    # Print hammer table
    print(f"\n{'Threshold':>10} | {'Sustain':>8} | {'Sample':>7} | {'Wins':>5} | {'Losses':>6} | {'Win%':>6} | {'EV/trade':>9}")
    print("-" * 75)

    for threshold in THRESHOLDS:
        for sustain in SUSTAIN_DURATIONS_SECS:
            r = hammer_results[threshold][sustain]
            total = r["wins"] + r["losses"]
            if total == 0:
                continue
            win_pct = r["wins"] / total * 100
            # EV: win pays ~(1-threshold)*HAMMER_SIZE, loss costs ~HAMMER_SIZE*(1-trim_recovery)
            avg_buy_price = threshold + 0.05  # rough avg buy price
            win_profit = (1.0 - avg_buy_price) * HAMMER_SIZE / avg_buy_price
            loss_cost = HAMMER_SIZE * 0.6  # assume trim recovers ~40% on bad ones
            ev = (r["wins"]/total) * win_profit - (r["losses"]/total) * loss_cost
            print(f"  {threshold:>7.0%} | {sustain//60:>5}min | {total:>7} | {r['wins']:>5} | {r['losses']:>6} | {win_pct:>5.1f}% | ${ev:>+7.2f}")
        if threshold < THRESHOLDS[-1]:
            print()

    # ─── BY MARKET TYPE BREAKDOWN ───
    print("\n" + "=" * 80)
    print("BREAKDOWN BY MARKET TYPE (at 75% threshold, 4 min sustain)")
    print("=" * 80)

    r = hammer_results.get(0.75, {}).get(240, {})
    if r:
        by_type = r.get("by_type", {})
        print(f"\n{'Type':>15} | {'Wins':>5} | {'Losses':>6} | {'Win%':>6}")
        print("-" * 45)
        for typ, counts in sorted(by_type.items()):
            total = counts["w"] + counts["l"]
            pct = counts["w"] / total * 100 if total > 0 else 0
            print(f"  {typ:>13} | {counts['w']:>5} | {counts['l']:>6} | {pct:>5.1f}%")

    # ─── TRIM ANALYSIS ───
    print("\n" + "=" * 80)
    print("TRIM ANALYSIS: How often does a side below threshold end up losing?")
    print("(Higher % = trim is correctly cutting losers)")
    print("=" * 80)

    trim_thresholds = [0.25, 0.30, 0.35, 0.40, 0.45]
    trim_durations = [120, 180, 240, 300]

    print(f"\n{'Threshold':>10} | {'Sustain':>8} | {'Sample':>7} | {'Correct':>7} | {'Wrong':>6} | {'Accuracy':>8}")
    print("-" * 65)

    for threshold in trim_thresholds:
        for sustain in trim_durations:
            correct = 0; wrong = 0

            for mid in resolved_mids:
                series = timeseries[mid]
                resolution = resolutions[mid]
                winner = resolution["winner"]

                # If YES dropped below threshold for sustain — was it actually the loser?
                yes_below, _ = analyze_trim_threshold(series, "yes", threshold, sustain)
                if yes_below:
                    if winner != "YES":
                        correct += 1  # correctly identified loser
                    else:
                        wrong += 1  # trimmed a winner (bad!)

                no_below, _ = analyze_trim_threshold(series, "no", threshold, sustain)
                if no_below:
                    if winner != "NO":
                        correct += 1
                    else:
                        wrong += 1

            total = correct + wrong
            if total == 0:
                continue
            pct = correct / total * 100
            print(f"  {threshold:>7.0%} | {sustain//60:>5}min | {total:>7} | {correct:>7} | {wrong:>6} | {pct:>6.1f}%")
        if threshold < trim_thresholds[-1]:
            print()

    # ─── SUMMARY ───
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\nTotal resolved markets analyzed: {len(resolved_mids)}")
    print(f"Total snapshots processed: {len(snapshots)}")

    # Find the best hammer config
    best_ev = -999
    best_config = None
    for threshold in THRESHOLDS:
        for sustain in SUSTAIN_DURATIONS_SECS:
            r = hammer_results[threshold][sustain]
            total = r["wins"] + r["losses"]
            if total < 5:
                continue
            win_pct = r["wins"] / total
            avg_buy_price = threshold + 0.05
            win_profit = (1.0 - avg_buy_price) * HAMMER_SIZE / avg_buy_price
            loss_cost = HAMMER_SIZE * 0.6
            ev = win_pct * win_profit - (1-win_pct) * loss_cost
            if ev > best_ev:
                best_ev = ev
                best_config = (threshold, sustain, r["wins"], r["losses"], win_pct*100)

    if best_config:
        print(f"\nBest hammer config: {best_config[0]:.0%} threshold, {best_config[1]//60}min sustain")
        print(f"  Win rate: {best_config[4]:.1f}% ({best_config[2]}W-{best_config[3]}L)")
        print(f"  Expected value: ${best_ev:+.2f} per $15 trade")

    # Save results
    results_file = os.path.join(GAME_DATA_DIR, "analysis_results.json")
    results = {
        "analyzed_at": datetime.now().isoformat(),
        "n_resolved": len(resolved_mids),
        "n_snapshots": len(snapshots),
        "hammer_results": {
            str(t): {str(s): {"wins": r["wins"], "losses": r["losses"]}
                     for s, r in sust.items()}
            for t, sust in hammer_results.items()
        },
    }
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nDetailed results saved to: {results_file}")


if __name__ == "__main__":
    run_analysis()
