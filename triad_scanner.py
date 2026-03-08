#!/usr/bin/env python3
"""
Polymarket Triad Arbitrage Scanner
════════════════════════════════════
RN1's ACTUAL strategy: buy NO on ALL THREE outcomes of soccer matches.

  Team A win-NO + Team B win-NO + Draw-NO = ALWAYS pays $2
  If total cost < $2 → guaranteed profit

This script:
  1. Finds all active soccer match markets
  2. Groups them into triads (Team A win, Team B win, Draw)
  3. Calculates triad cost from live NO ask prices
  4. Identifies profitable triads (cost < $2.00)
  5. Shows the arbitrage spread and optimal sizing

Usage:
  python3.11 triad_scanner.py               # scan once
  python3.11 triad_scanner.py --loop 10     # scan every 10 seconds
  python3.11 triad_scanner.py --min-edge 0  # show all triads including unprofitable
"""

import os
import sys
import json
import time
import re
import argparse
import logging
from datetime import datetime, timezone
from collections import defaultdict
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

CLOB_BASE = "https://clob.polymarket.com"
GAMMA_BASE = "https://gamma-api.polymarket.com"

log = logging.getLogger("triad")

def api_get(url, timeout=10):
    for attempt in range(3):
        try:
            req = Request(url, headers={"User-Agent": "TriadScanner/1.0", "Accept": "application/json"})
            resp = urlopen(req, timeout=timeout)
            return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            if e.code == 429:
                time.sleep(2 ** attempt * 2)
                continue
            return None
        except (URLError, TimeoutError, OSError):
            time.sleep(1)
            continue
    return None


def fetch_all_markets(max_markets=2000):
    """Fetch all active markets from Gamma API."""
    all_markets = []
    offset = 0
    limit = 100

    while len(all_markets) < max_markets:
        url = f"{GAMMA_BASE}/markets?limit={limit}&offset={offset}&active=true&closed=false"
        data = api_get(url, timeout=15)
        if not data or not isinstance(data, list):
            break

        for m in data:
            if not m.get("active") or m.get("closed"):
                continue

            clob_ids = m.get("clobTokenIds", [])
            if isinstance(clob_ids, str):
                try: clob_ids = json.loads(clob_ids)
                except: clob_ids = []

            outcomes = m.get("outcomes", [])
            if isinstance(outcomes, str):
                try: outcomes = json.loads(outcomes)
                except: outcomes = []

            tokens = {}
            if len(clob_ids) >= 2:
                tokens["yes"] = str(clob_ids[0])
                tokens["no"]  = str(clob_ids[1])
            elif len(clob_ids) == 1:
                tokens["yes"] = str(clob_ids[0])

            market = {
                "id": m.get("id", ""),
                "condition_id": m.get("conditionId", m.get("condition_id", "")),
                "title": m.get("question", m.get("title", "")),
                "slug": m.get("slug", ""),
                "category": m.get("category", ""),
                "end_date": m.get("endDate", m.get("end_date_iso", "")),
                "volume": float(m.get("volume", 0) or 0),
                "liquidity": float(m.get("liquidity", 0) or 0),
                "tokens": tokens,
                "outcomes": outcomes,
            }
            all_markets.append(market)

        if len(data) < limit:
            break
        offset += limit
        time.sleep(0.2)

    return all_markets


def normalize_team(name):
    """Normalize team name for matching."""
    t = name.strip().lower()
    # Remove common suffixes
    for suffix in [' fc', ' sc', ' cf', ' afc', ' sfc']:
        if t.endswith(suffix):
            t = t[:-len(suffix)].strip()
    # Remove country suffixes in parentheses
    t = re.sub(r'\s*\(.*?\)\s*', '', t)
    # Remove "SK", "FK", etc at end
    t = re.sub(r'\s+(sk|fk|bk|if)$', '', t)
    return t.strip()


def extract_match_info(title):
    """Extract match information from market title.
    Returns (match_type, team_a, team_b) or None.
    
    Match types: 'win_a', 'win_b', 'draw'
    """
    t = title.strip()
    
    # "Will TEAM win on DATE?" or "Will TEAM win?"
    m = re.match(r'Will (.+?) win(?:\s+on\s+\d{4}-\d{2}-\d{2})?\??$', t, re.I)
    if m:
        team = m.group(1).strip()
        return ("win", team, None)
    
    # "Will TEAM A vs. TEAM B end in a draw?"
    m = re.match(r'Will (.+?)\s+vs\.?\s+(.+?)\s+end in a draw\??$', t, re.I)
    if m:
        team_a = m.group(1).strip()
        team_b = m.group(2).strip()
        return ("draw", team_a, team_b)
    
    # "TEAM A vs. TEAM B" (match winner market)
    m = re.match(r'^(.+?)\s+vs\.?\s+(.+?)$', t, re.I)
    if m:
        team_a = m.group(1).strip()
        team_b = m.group(2).strip()
        # This could be a match-winner market, not team-specific
        return ("match", team_a, team_b)
    
    return None


def group_into_triads(markets):
    """Group markets into triads: Team A win + Team B win + Draw.
    
    Strategy: Use draw markets as anchors (they have both team names),
    then find matching win markets for each team.
    """
    # Index: normalized team name → list of win markets
    win_markets = defaultdict(list)
    draw_markets = []
    match_markets = []  # "Team A vs Team B" format
    other_markets = []
    
    for m in markets:
        info = extract_match_info(m["title"])
        if not info:
            other_markets.append(m)
            continue
        
        mtype = info[0]
        
        if mtype == "win":
            team = info[1]
            norm = normalize_team(team)
            m["_team"] = team
            m["_norm_team"] = norm
            win_markets[norm].append(m)
        elif mtype == "draw":
            team_a, team_b = info[1], info[2]
            m["_team_a"] = team_a
            m["_team_b"] = team_b
            m["_norm_a"] = normalize_team(team_a)
            m["_norm_b"] = normalize_team(team_b)
            draw_markets.append(m)
        elif mtype == "match":
            team_a, team_b = info[1], info[2]
            m["_team_a"] = team_a
            m["_team_b"] = team_b
            match_markets.append(m)
    
    log.info(f"Markets: {len(win_markets)} teams with win markets, "
             f"{len(draw_markets)} draw markets, {len(match_markets)} match markets")
    
    # Build triads: for each draw market, find the two win markets
    triads = []
    
    for draw_m in draw_markets:
        norm_a = draw_m["_norm_a"]
        norm_b = draw_m["_norm_b"]
        
        # Find win markets for both teams
        wins_a = win_markets.get(norm_a, [])
        wins_b = win_markets.get(norm_b, [])
        
        # Try fuzzy matching if exact fails
        if not wins_a:
            for norm_key, wins in win_markets.items():
                if norm_a in norm_key or norm_key in norm_a:
                    wins_a = wins
                    break
        if not wins_b:
            for norm_key, wins in win_markets.items():
                if norm_b in norm_key or norm_key in norm_b:
                    wins_b = wins
                    break
        
        if wins_a and wins_b:
            # Take the most liquid win market for each team
            best_a = max(wins_a, key=lambda m: m.get("volume", 0))
            best_b = max(wins_b, key=lambda m: m.get("volume", 0))
            
            triads.append({
                "team_a": draw_m["_team_a"],
                "team_b": draw_m["_team_b"],
                "win_a": best_a,
                "win_b": best_b,
                "draw": draw_m,
            })
    
    log.info(f"Complete triads found: {len(triads)}")
    return triads, other_markets


def fetch_no_price(market):
    """Fetch the current NO ask price for a market."""
    no_token = market.get("tokens", {}).get("no")
    if not no_token:
        return None, None
    
    url = f"{CLOB_BASE}/book?token_id={no_token}"
    book = api_get(url)
    if not book:
        return None, None
    
    asks = book.get("asks", [])
    if not asks:
        return None, None
    
    # Best (lowest) ask = cheapest NO we can buy
    best_ask = None
    total_depth = 0
    for a in asks:
        try:
            p = float(a.get("price", 0))
            s = float(a.get("size", 0))
            if p > 0 and s > 0:
                if best_ask is None or p < best_ask:
                    best_ask = p
                total_depth += s * p  # USD depth
        except:
            continue
    
    return best_ask, round(total_depth, 2)


def scan_triads(triads, min_edge=0.0):
    """Fetch live prices and calculate arbitrage for each triad."""
    results = []
    
    for i, triad in enumerate(triads):
        if (i + 1) % 10 == 0:
            print(f"  Scanning triad {i+1}/{len(triads)}...", end="\r")
        
        # Fetch NO prices for all three legs
        price_a, depth_a = fetch_no_price(triad["win_a"])
        time.sleep(0.1)
        price_b, depth_b = fetch_no_price(triad["win_b"])
        time.sleep(0.1)
        price_d, depth_d = fetch_no_price(triad["draw"])
        time.sleep(0.1)
        
        if price_a is None or price_b is None or price_d is None:
            continue
        
        total_cost = price_a + price_b + price_d
        payout = 2.00  # guaranteed
        edge = payout - total_cost
        edge_pct = (edge / total_cost) * 100 if total_cost > 0 else 0
        
        # Min depth across legs (bottleneck)
        depths = [d for d in [depth_a, depth_b, depth_d] if d]
        min_depth = min(depths) if depths else 0
        
        result = {
            "team_a": triad["team_a"],
            "team_b": triad["team_b"],
            "price_a_no": price_a,
            "price_b_no": price_b,
            "price_draw_no": price_d,
            "total_cost": round(total_cost, 4),
            "payout": payout,
            "edge": round(edge, 4),
            "edge_pct": round(edge_pct, 2),
            "min_depth_usd": min_depth,
            "market_a_id": triad["win_a"]["id"],
            "market_b_id": triad["win_b"]["id"],
            "market_draw_id": triad["draw"]["id"],
            "token_a_no": triad["win_a"].get("tokens", {}).get("no", ""),
            "token_b_no": triad["win_b"].get("tokens", {}).get("no", ""),
            "token_draw_no": triad["draw"].get("tokens", {}).get("no", ""),
            "title_a": triad["win_a"]["title"],
            "title_b": triad["win_b"]["title"],
            "title_draw": triad["draw"]["title"],
        }
        
        if edge >= min_edge:
            results.append(result)
    
    print(" " * 60, end="\r")  # clear progress
    
    # Sort by edge (best first)
    results.sort(key=lambda r: -r["edge"])
    return results


def display_results(results, top_n=30):
    """Pretty-print triad results."""
    profitable = [r for r in results if r["edge"] > 0]
    breakeven = [r for r in results if -0.02 <= r["edge"] <= 0]
    underwater = [r for r in results if r["edge"] < -0.02]
    
    print(f"\n{'='*80}")
    print(f"TRIAD ARBITRAGE SCAN — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"{'='*80}")
    print(f"\n  Total triads scanned: {len(results)}")
    print(f"  Profitable (edge > 0): {len(profitable)} ✓")
    print(f"  Near break-even (±2¢): {len(breakeven)} ～")
    print(f"  Underwater (edge < -2¢): {len(underwater)} ✗")
    
    if profitable:
        total_edge = sum(r["edge"] for r in profitable)
        avg_edge = total_edge / len(profitable)
        print(f"\n  Avg profitable edge: ${avg_edge:.4f} ({avg_edge/2*100:.2f}%)")
        print(f"  Total edge across all profitable: ${total_edge:.2f}")
    
    # Show top opportunities
    print(f"\n{'─'*80}")
    print(f"  TOP {min(top_n, len(results))} TRIADS (sorted by edge)")
    print(f"{'─'*80}")
    
    for i, r in enumerate(results[:top_n]):
        edge_marker = "✓ PROFIT" if r["edge"] > 0 else ("～ FLAT" if r["edge"] > -0.02 else "✗ LOSS")
        edge_color = "\033[92m" if r["edge"] > 0 else ("\033[93m" if r["edge"] > -0.02 else "\033[91m")
        reset = "\033[0m"
        
        print(f"\n  {i+1}. {r['team_a']} vs {r['team_b']}")
        print(f"     {r['team_a']:30s} NO @ ${r['price_a_no']:.3f}")
        print(f"     {r['team_b']:30s} NO @ ${r['price_b_no']:.3f}")
        print(f"     {'Draw':30s} NO @ ${r['price_draw_no']:.3f}")
        print(f"     ──────────────────────────────────────────")
        print(f"     Total cost: ${r['total_cost']:.4f}  Payout: $2.00  "
              f"{edge_color}Edge: ${r['edge']:+.4f} ({r['edge_pct']:+.2f}%) {edge_marker}{reset}")
        if r["min_depth_usd"] > 0:
            print(f"     Depth: ~${r['min_depth_usd']:.0f} available")
    
    # Summary stats by edge buckets
    print(f"\n{'─'*80}")
    print(f"  EDGE DISTRIBUTION")
    print(f"{'─'*80}")
    
    edges = [r["edge"] for r in results]
    if edges:
        buckets = [
            ("> +$0.10", lambda e: e > 0.10),
            ("+$0.05 to +$0.10", lambda e: 0.05 < e <= 0.10),
            ("+$0.02 to +$0.05", lambda e: 0.02 < e <= 0.05),
            ("+$0.00 to +$0.02", lambda e: 0.00 < e <= 0.02),
            ("-$0.02 to $0.00", lambda e: -0.02 <= e <= 0.00),
            ("-$0.05 to -$0.02", lambda e: -0.05 < e < -0.02),
            ("< -$0.05", lambda e: e <= -0.05),
        ]
        for label, pred in buckets:
            count = len([e for e in edges if pred(e)])
            if count > 0:
                bar = "█" * min(count, 40)
                print(f"  {label:>22s}: {count:>4d} {bar}")
    
    return profitable


def main():
    parser = argparse.ArgumentParser(description="Polymarket Triad Arbitrage Scanner")
    parser.add_argument("--loop", type=int, default=0,
                       help="Scan every N seconds (0 = once)")
    parser.add_argument("--min-edge", type=float, default=-0.10,
                       help="Minimum edge to display (default: -0.10)")
    parser.add_argument("--max-markets", type=int, default=2000,
                       help="Max markets to fetch (default: 2000)")
    parser.add_argument("--output", default="triad_opportunities.json",
                       help="Output file for results")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                       format="%(asctime)s [%(levelname)s] %(message)s",
                       datefmt="%H:%M:%S")
    
    print("="*60)
    print("Polymarket Triad Arbitrage Scanner")
    print("="*60)
    print(f"  Strategy: Team A NO + Team B NO + Draw NO = $2 guaranteed")
    print(f"  Profit when total cost < $2.00")
    print("="*60)
    
    while True:
        # Fetch markets
        print(f"\n  Fetching active markets...")
        markets = fetch_all_markets(args.max_markets)
        print(f"  Found {len(markets)} active markets")
        
        # Group into triads
        triads, other = group_into_triads(markets)
        
        if not triads:
            print("\n  No complete triads found!")
            print("  This could mean:")
            print("    - No active soccer matches right now")
            print("    - Market titles don't match expected patterns")
            print("    - Try increasing --max-markets")
        else:
            # Scan prices
            print(f"\n  Scanning {len(triads)} triads for arbitrage...")
            results = scan_triads(triads, min_edge=args.min_edge)
            
            # Display
            profitable = display_results(results)
            
            # Save results
            with open(args.output, "w") as f:
                json.dump({
                    "scan_time": datetime.now(timezone.utc).isoformat(),
                    "total_triads": len(triads),
                    "profitable": len(profitable),
                    "results": results,
                }, f, indent=2)
            print(f"\n  Results saved: {args.output}")
        
        if args.loop <= 0:
            break
        
        print(f"\n  Next scan in {args.loop} seconds... (Ctrl+C to stop)")
        time.sleep(args.loop)


if __name__ == "__main__":
    main()
