#!/usr/bin/env python3
"""
Polymarket Threshold Analysis — Sports Markets
===============================================

Analyzes recently resolved sports markets to answer:
"How often does a side that hits 75¢+ and stays there for 4+ minutes
end up winning (resolving to $1)?"

Approach:
1. Fetch recently resolved sports markets (basketball, tennis, soccer, hockey)
2. For each market, get minute-level price history
3. Scan for moments where a side crossed above thresholds (70¢, 75¢, 80¢, 85¢)
4. Check if that side sustained above threshold for given durations (2min, 4min, 6min, 8min)
5. Check if side ultimately resolved to $1 (winner) or $0 (loser)
6. Calculate win rates and expected profit/loss per trade

Author: Threshold Analysis Tool
Date: 2026-03-07
"""

import os
import sys
import time
import json
import requests
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
import concurrent.futures

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

# Analysis parameters
THRESHOLDS = [0.70, 0.75, 0.80, 0.85]
SUSTAIN_DURATIONS_MINUTES = [2, 4, 6, 8]
HAMMER_SIZE = 15  # $15 assumed trade size
LOOKBACK_DAYS = 14  # how far back to fetch resolved markets
MAX_MARKETS = 200  # maximum resolved markets to analyze
MAX_WORKERS = 10  # parallel API calls

# Sports categories to analyze
SPORTS_KEYWORDS = [
    "basketball", "nba", "ncaab",
    "tennis", "atp", "wta",
    "soccer", "football", "premier league", "nfl", "nhl",
    "hockey", "mlb", "nba"
]

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
# HTTP UTILITIES
# ═══════════════════════════════════════════════════════════════

_session = None

def get_session():
    """Get persistent HTTP session with connection pooling."""
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({
            'User-Agent': 'PolymarketAnalysis/1.0'
        })
    return _session

def api_get(url: str, timeout: int = 15, retries: int = 3) -> Optional[dict]:
    """Fetch JSON from API with retries and exponential backoff."""
    sess = get_session()
    for attempt in range(retries + 1):
        try:
            resp = sess.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            if attempt < retries:
                wait = 2 ** attempt
                log.debug(f"  API error (attempt {attempt+1}/{retries+1}), retrying in {wait}s: {e}")
                time.sleep(wait)
            else:
                log.warning(f"  API failed after {retries+1} attempts: {e}")
                return None

# ═══════════════════════════════════════════════════════════════
# MARKET FETCHING
# ═══════════════════════════════════════════════════════════════

def is_sports_market(market: dict) -> bool:
    """Check if market is a sports market (match winner or O/U)."""
    title = (market.get("title") or "").lower()
    description = (market.get("description") or "").lower()
    tags = (market.get("tags") or [])
    if isinstance(tags, str):
        tags_str = tags.lower()
    else:
        tags_str = " ".join(str(t).lower() for t in tags)

    # Check if any sports keyword is in title, description, or tags
    combined = f"{title} {description} {tags_str}"
    for keyword in SPORTS_KEYWORDS:
        if keyword in combined:
            return True
    return False

def fetch_resolved_sports_markets() -> List[dict]:
    """
    Fetch recently resolved sports markets from Gamma API.
    Returns list of market dicts with id, title, tokens, resolutionTime, etc.
    """
    log.info(f"Fetching resolved sports markets (lookback: {LOOKBACK_DAYS} days)...")

    markets = []
    offset = 0

    while len(markets) < MAX_MARKETS:
        url = f"{GAMMA}/markets?limit=200&offset={offset}&closed=true"

        data = api_get(url)
        if not data:
            log.warning(f"  Failed to fetch markets at offset {offset}")
            break

        if isinstance(data, dict):
            market_list = data.get("data", data.get("markets", []))
        else:
            market_list = data if isinstance(data, list) else []

        if not market_list:
            log.info(f"  No more markets at offset {offset}")
            break

        for market in market_list:
            # Filter for sports markets
            if not is_sports_market(market):
                continue

            # Check resolution time (only include markets resolved in lookback window)
            resolution_str = market.get("resolutionTime") or market.get("resolvedTime")
            if resolution_str:
                try:
                    res_time = datetime.fromisoformat(resolution_str.replace("Z", "+00:00"))
                    lookback = datetime.now(res_time.tzinfo) - timedelta(days=LOOKBACK_DAYS)
                    if res_time < lookback:
                        continue  # too old
                except (ValueError, TypeError):
                    pass

            # Check that market has the required fields
            condition_id = market.get("id") or market.get("condition_id")
            tokens = market.get("tokens") or market.get("clobTokenIds", [])
            if not condition_id or not tokens or len(tokens) < 2:
                continue

            markets.append(market)
            if len(markets) >= MAX_MARKETS:
                break

        offset += 200
        if len(market_list) < 200:
            break

    log.info(f"✓ Fetched {len(markets)} resolved sports markets")
    return markets

# ═══════════════════════════════════════════════════════════════
# PRICE HISTORY FETCHING
# ═══════════════════════════════════════════════════════════════

def fetch_price_history(condition_id: str, token_id: str,
                        start_time: Optional[str] = None) -> List[Tuple[float, datetime]]:
    """
    Fetch minute-level price history for a specific token.
    Returns list of (price, timestamp) tuples sorted by time.
    """
    # Try gamma-api prices endpoint
    url = f"{GAMMA}/prices-history?market={condition_id}&fidelity=1m&interval=full"

    data = api_get(url)
    if not data:
        return []

    # Parse response — format varies but typically a list of price snapshots
    history = []

    if isinstance(data, dict):
        # Try various possible response structures
        prices = data.get("prices", data.get("data", []))
        if not isinstance(prices, list):
            prices = [prices] if prices else []
    elif isinstance(data, list):
        prices = data
    else:
        prices = []

    for item in prices:
        if not isinstance(item, dict):
            continue

        try:
            price = float(item.get("price") or item.get("mid") or 0)
            if price <= 0:
                continue

            # Parse timestamp
            ts_str = item.get("timestamp") or item.get("time")
            if not ts_str:
                continue

            # Handle various timestamp formats
            try:
                if "T" in ts_str:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                else:
                    # Unix timestamp
                    ts = datetime.fromtimestamp(float(ts_str))
            except (ValueError, TypeError):
                continue

            history.append((price, ts))

        except (ValueError, TypeError, KeyError):
            continue

    # Sort by timestamp
    history.sort(key=lambda x: x[1])

    return history

# ═══════════════════════════════════════════════════════════════
# THRESHOLD ANALYSIS
# ═══════════════════════════════════════════════════════════════

def find_threshold_breaches(history: List[Tuple[float, datetime]],
                            threshold: float) -> List[Tuple[datetime, datetime]]:
    """
    Find all times when price broke above threshold.
    Returns list of (breach_time, end_time) for each breach.
    Each breach ends when price drops below threshold or history ends.
    """
    breaches = []
    in_breach = False
    breach_start = None

    for price, ts in history:
        if price >= threshold:
            if not in_breach:
                breach_start = ts
                in_breach = True
        else:
            if in_breach:
                breaches.append((breach_start, ts))
                in_breach = False

    # If still in breach at end of history, close it
    if in_breach and history:
        breaches.append((breach_start, history[-1][1]))

    return breaches

def check_sustained_threshold(history: List[Tuple[float, datetime]],
                              threshold: float,
                              min_duration_minutes: int) -> bool:
    """
    Check if price ever stays above threshold for at least min_duration_minutes.
    Returns True if any breach lasts long enough.
    """
    breaches = find_threshold_breaches(history, threshold)

    for start, end in breaches:
        duration = (end - start).total_seconds() / 60
        if duration >= min_duration_minutes:
            return True

    return False

def get_market_resolution(market: dict) -> Optional[int]:
    """
    Get market resolution outcome (0 or 1).
    Try to infer from prices or explicit field.
    Returns 1 if YES won, 0 if NO won, None if unclear.
    """
    # Check if market has explicit resolution
    outcome = market.get("outcome") or market.get("result")
    if outcome:
        outcome_str = str(outcome).lower()
        if outcome_str in ["yes", "1", "true"]:
            return 1
        elif outcome_str in ["no", "0", "false"]:
            return 0

    # Try to infer from final prices if available
    # (In real Polymarket data, resolved markets have prices at 0 or 1)
    tokens = market.get("tokens") or []
    if isinstance(tokens, list) and len(tokens) >= 2:
        # If we had end-of-market prices, could use them
        # For now, return None (needs to be looked up separately)
        pass

    return None

# ═══════════════════════════════════════════════════════════════
# ANALYSIS PIPELINE
# ═══════════════════════════════════════════════════════════════

def analyze_single_market(market: dict) -> Optional[dict]:
    """
    Analyze a single resolved market.
    Returns dict with market_id, results for each threshold/duration combo.
    """
    condition_id = market.get("id") or market.get("condition_id")
    title = market.get("title", "Unknown")

    tokens = market.get("tokens") or market.get("clobTokenIds", [])
    if not isinstance(tokens, list) or len(tokens) < 2:
        return None

    # Get resolution outcome
    resolution = get_market_resolution(market)

    # Analyze each token (side of market)
    results = {
        "market_id": condition_id,
        "title": title,
        "resolution": resolution,
        "sides": {}
    }

    for idx, token_id in enumerate(tokens[:2]):  # Only analyze first 2 sides (YES/NO)
        side_name = "YES" if idx == 0 else "NO"

        # Fetch price history
        history = fetch_price_history(condition_id, token_id)
        if not history or len(history) < 2:
            log.debug(f"    {side_name}: No price history")
            continue

        log.debug(f"    {side_name}: {len(history)} price points")

        side_results = {
            "token_id": token_id,
            "price_points": len(history),
            "min_price": min(p for p, _ in history),
            "max_price": max(p for p, _ in history),
            "thresholds": {}
        }

        # Test each threshold/duration combo
        for threshold in THRESHOLDS:
            side_results["thresholds"][threshold] = {}

            for duration_min in SUSTAIN_DURATIONS_MINUTES:
                sustained = check_sustained_threshold(history, threshold, duration_min)
                side_results["thresholds"][threshold][duration_min] = sustained

        results["sides"][side_name] = side_results

    return results if results["sides"] else None

def run_analysis(markets: List[dict]) -> dict:
    """
    Analyze all markets in parallel.
    Returns aggregated results dict.
    """
    log.info(f"Analyzing {len(markets)} markets...")

    analyzed = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(analyze_single_market, m): m for m in markets}

        completed = 0
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                analyzed.append(result)
            completed += 1
            if completed % 20 == 0:
                log.info(f"  Analyzed {completed}/{len(markets)} markets")

    log.info(f"✓ Successfully analyzed {len(analyzed)} markets")

    return aggregate_results(analyzed)

def aggregate_results(analyzed_markets: List[dict]) -> dict:
    """
    Aggregate individual market results into win rate statistics.
    """
    # Track: threshold -> duration -> [win_count, loss_count, total]
    stats = defaultdict(lambda: defaultdict(lambda: [0, 0, 0]))  # wins, losses, total

    for market_result in analyzed_markets:
        resolution = market_result.get("resolution")
        sides = market_result.get("sides", {})

        for side_name, side_data in sides.items():
            # Determine if this side won
            if resolution is None:
                continue  # Can't determine outcome

            # YES = index 0, NO = index 1
            side_won = (resolution == 1 and side_name == "YES") or \
                       (resolution == 0 and side_name == "NO")

            thresholds = side_data.get("thresholds", {})

            for threshold, durations in thresholds.items():
                for duration, sustained in durations.items():
                    if sustained:  # Only count if threshold was actually sustained
                        stats[threshold][duration][2] += 1  # increment total
                        if side_won:
                            stats[threshold][duration][0] += 1  # increment wins
                        else:
                            stats[threshold][duration][1] += 1  # increment losses

    # Convert to readable format
    summary = {}
    for threshold in THRESHOLDS:
        summary[threshold] = {}
        for duration in SUSTAIN_DURATIONS_MINUTES:
            wins, losses, total = stats[threshold][duration]
            win_rate = (wins / total * 100) if total > 0 else 0

            # Expected profit/loss per trade
            # Assume entry at threshold, $15 hammer size
            # If win: profit = (1.00 - threshold) * 15 = 15 * (1 - threshold)
            # If loss: loss = threshold * 15
            ev = 0
            if total > 0:
                win_prob = wins / total
                loss_prob = losses / total
                win_payout = (1.00 - threshold) * HAMMER_SIZE
                loss_cost = threshold * HAMMER_SIZE
                ev = (win_prob * win_payout) - (loss_prob * loss_cost)

            summary[threshold][duration] = {
                "wins": wins,
                "losses": losses,
                "total_samples": total,
                "win_rate_pct": round(win_rate, 2),
                "expected_value_usd": round(ev, 2)
            }

    return summary

# ═══════════════════════════════════════════════════════════════
# OUTPUT FORMATTING
# ═══════════════════════════════════════════════════════════════

def print_results_table(summary: dict):
    """Print results as a clean ASCII table."""

    print("\n" + "="*120)
    print("POLYMARKET THRESHOLD ANALYSIS — WIN RATES & EXPECTED VALUE")
    print("="*120)

    print(f"\nAssumptions:")
    print(f"  - Hammer size: ${HAMMER_SIZE}")
    print(f"  - Entry: At or above threshold price")
    print(f"  - Win payout: $1.00 per share")
    print(f"  - Loss: Entry price per share")
    print(f"  - EV calculation: (win_rate × win_payout) - (loss_rate × loss_cost)")

    print(f"\nThresholds analyzed: {[f'${t:.0%}' for t in THRESHOLDS]}")
    print(f"Sustain durations: {SUSTAIN_DURATIONS_MINUTES} minutes")

    print("\n" + "-"*120)

    # Build table
    for threshold in THRESHOLDS:
        print(f"\n📊 THRESHOLD: ${threshold:.0%}")
        print("-"*120)
        print(f"{'Duration':>12} | {'Samples':>10} | {'Wins':>6} | {'Losses':>7} | {'Win Rate':>12} | {'Expected Value (EV)':>20}")
        print("-"*120)

        for duration in SUSTAIN_DURATIONS_MINUTES:
            row = summary[threshold][duration]
            samples = row["total_samples"]
            wins = row["wins"]
            losses = row["losses"]
            win_rate = row["win_rate_pct"]
            ev = row["expected_value_usd"]

            ev_color = "🟢" if ev > 0 else "🔴" if ev < 0 else "⚪"

            print(f"{duration:>2} minutes   | {samples:>10} | {wins:>6} | {losses:>7} | {win_rate:>11.1f}% | {ev_color} ${ev:>18.2f}")

    print("-"*120)
    print("\nInterpretation:")
    print("  ✓ Win Rate: % of times the side won when threshold was sustained")
    print("  ✓ EV (Expected Value): Average $ profit/loss per $15 bet at that threshold")
    print("  ✓ Positive EV = profitable strategy (on average)")
    print("  ✓ Negative EV = money-losing strategy (on average)")
    print("="*120 + "\n")

def save_detailed_results(summary: dict, analyzed_markets: List[dict], filename: str = "threshold_analysis_results.json"):
    """Save full results to JSON file."""
    output = {
        "timestamp": datetime.now().isoformat(),
        "parameters": {
            "lookback_days": LOOKBACK_DAYS,
            "thresholds": THRESHOLDS,
            "sustain_durations_minutes": SUSTAIN_DURATIONS_MINUTES,
            "hammer_size_usd": HAMMER_SIZE,
            "markets_analyzed": len(analyzed_markets)
        },
        "summary": summary,
        "detailed_markets": analyzed_markets[:50]  # Save first 50 detailed results
    }

    filepath = os.path.join(os.path.dirname(__file__), filename)
    with open(filepath, 'w') as f:
        json.dump(output, f, indent=2, default=str)

    log.info(f"✓ Detailed results saved to {filepath}")

# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main():
    """Main analysis pipeline."""

    log.info("="*120)
    log.info("POLYMARKET THRESHOLD ANALYSIS — SPORTS MARKETS")
    log.info(f"Started: {datetime.now().isoformat()}")
    log.info("="*120)

    try:
        # Step 1: Fetch resolved sports markets
        markets = fetch_resolved_sports_markets()
        if not markets:
            log.error("No markets found. Exiting.")
            return

        # Step 2: Analyze each market
        summary = run_analysis(markets)

        # Step 3: Print results table
        print_results_table(summary)

        # Step 4: Save detailed JSON
        save_detailed_results(summary, [])

        log.info("✓ Analysis complete!")

    except KeyboardInterrupt:
        log.info("Interrupted by user")
    except Exception as e:
        log.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
