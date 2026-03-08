#!/usr/bin/env python3
"""
RN1 LIVE TRADE FETCHER
=======================
Polls Polymarket's data API for RN1's recent trades and saves them as JSONL.
Designed to run alongside arb_v10.py to capture RN1's activity during live sessions.

Saves to: game_data/rn1_trades.jsonl (append-only)

Usage:
    python3 fetch_rn1.py                  # run continuously (polls every 30s)
    python3 fetch_rn1.py --once           # single pull, then exit
    python3 fetch_rn1.py --hours 6        # pull last N hours of trades
"""

import requests
import json
import time
import os
import sys
import argparse
from datetime import datetime, timezone, timedelta

try:
    from game_scores import GameScoreFetcher
    _game_fetcher = GameScoreFetcher()
    _has_game_scores = True
except ImportError:
    _game_fetcher = None
    _has_game_scores = False

RN1_WALLET = "0x2005d16a84ceefa912d4e380cd32e7ff827875ea"
CLOB_API = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

GAME_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "game_data")
OUTPUT_FILE = os.path.join(GAME_DATA_DIR, "rn1_trades.jsonl")
POLL_INTERVAL = 30  # seconds between polls

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})

# Track what we've already seen to avoid duplicates
_seen_ids = set()


def fetch_recent_trades(hours_back=2):
    """Fetch RN1's recent trades from the Polymarket data API."""
    trades = []

    # Try the activity endpoint (most reliable for recent trades)
    try:
        params = {
            "user": RN1_WALLET,
            "limit": 100,
        }
        r = session.get(f"{DATA_API}/activity", params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                trades.extend(data)
            elif isinstance(data, dict):
                trades.extend(data.get("data", data.get("history", [])))
    except Exception as e:
        print(f"  [activity] Error: {e}")

    # Also try the trades endpoint
    try:
        params = {
            "maker_address": RN1_WALLET,
            "limit": 100,
        }
        r = session.get(f"{CLOB_API}/trades", params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                trades.extend(data)
            elif isinstance(data, dict):
                trades.extend(data.get("data", data.get("trades", [])))
    except Exception as e:
        print(f"  [trades] Error: {e}")

    # Try taker_address too
    try:
        params = {
            "taker_address": RN1_WALLET,
            "limit": 100,
        }
        r = session.get(f"{CLOB_API}/trades", params=params, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                trades.extend(data)
            elif isinstance(data, dict):
                trades.extend(data.get("data", data.get("trades", [])))
    except Exception as e:
        print(f"  [taker_trades] Error: {e}")

    return trades


def resolve_token_to_market(token_id):
    """Try to resolve a token ID to a market title."""
    try:
        r = session.get(f"{GAMMA_API}/markets",
                       params={"clob_token_ids": token_id}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                return data[0].get("question", "")[:60]
    except:
        pass
    return ""


def process_and_save(trades):
    """Deduplicate and save new trades to JSONL."""
    global _seen_ids
    new_count = 0

    os.makedirs(GAME_DATA_DIR, exist_ok=True)

    with open(OUTPUT_FILE, "a") as f:
        for t in trades:
            # Build a unique ID from available fields
            trade_id = (t.get("id") or t.get("tradeId") or
                       f"{t.get('timestamp','')}-{t.get('asset_id','')}-{t.get('price','')}-{t.get('size','')}")

            if trade_id in _seen_ids:
                continue
            _seen_ids.add(trade_id)

            # Normalize the record
            title = t.get("title") or t.get("question") or t.get("market_slug", "")
            record = {
                "fetched_at": time.time(),
                "trade_id": str(trade_id),
                "timestamp": t.get("timestamp") or t.get("created_at") or t.get("match_time", ""),
                "side": t.get("side") or t.get("outcome") or "",
                "price": float(t.get("price") or 0),
                "size": float(t.get("size") or t.get("amount") or 0),
                "asset_id": t.get("asset_id") or t.get("assetId") or t.get("token_id", ""),
                "market": t.get("market") or t.get("condition_id") or t.get("conditionId", ""),
                "title": title,
                "type": t.get("type") or t.get("action") or "trade",
                "maker": (t.get("maker_address") or "").lower(),
                "taker": (t.get("taker_address") or "").lower(),
            }

            # Enrich with live game score if available
            if _has_game_scores and _game_fetcher and title:
                game = _game_fetcher.match_position(trade_id, title)
                if game and game.get("state") in ("in", "post"):
                    record["game_state"] = {
                        "home": game.get("home_short") or game.get("home_name", ""),
                        "away": game.get("away_short") or game.get("away_name", ""),
                        "home_score": game.get("home_score", 0),
                        "away_score": game.get("away_score", 0),
                        "period": game.get("period", 0),
                        "clock": game.get("clock", ""),
                        "detail": game.get("detail", ""),
                    }

            f.write(json.dumps(record) + "\n")
            new_count += 1

    return new_count


def main():
    parser = argparse.ArgumentParser(description="Fetch RN1's live trades")
    parser.add_argument("--once", action="store_true", help="Single pull, then exit")
    parser.add_argument("--hours", type=float, default=2, help="Hours of history to pull")
    args = parser.parse_args()

    print("=" * 60)
    print(f"  RN1 LIVE TRADE FETCHER")
    print(f"  Wallet: {RN1_WALLET[:10]}...{RN1_WALLET[-6:]}")
    print(f"  Output: {OUTPUT_FILE}")
    print(f"  Mode: {'single pull' if args.once else f'continuous (every {POLL_INTERVAL}s)'}")
    print("=" * 60)

    # Load existing IDs to avoid re-writing
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE) as f:
            for line in f:
                try:
                    r = json.loads(line)
                    _seen_ids.add(r.get("trade_id", ""))
                except:
                    pass
        print(f"  Loaded {len(_seen_ids)} existing trade IDs")

    if _has_game_scores:
        print(f"  Game scores: ENABLED (ESPN)")
    else:
        print(f"  Game scores: DISABLED (game_scores.py not found)")

    cycle = 0
    while True:
        cycle += 1
        # Update ESPN scores alongside trade fetches
        if _has_game_scores and _game_fetcher:
            try:
                _game_fetcher.update()
            except Exception:
                pass
        trades = fetch_recent_trades(hours_back=args.hours)
        new_count = process_and_save(trades)

        ts = datetime.now().strftime("%H:%M:%S")
        if new_count > 0:
            print(f"  [{ts}] Cycle {cycle}: {len(trades)} fetched, {new_count} NEW → {OUTPUT_FILE}")
        elif cycle % 10 == 0:  # periodic status every ~5 min
            print(f"  [{ts}] Cycle {cycle}: {len(trades)} fetched, 0 new (total seen: {len(_seen_ids)})")

        if args.once:
            print(f"\n  Done. {len(_seen_ids)} total trades in {OUTPUT_FILE}")
            break

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
