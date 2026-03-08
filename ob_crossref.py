#!/usr/bin/env python3
"""
RN1 Signal Cross-Reference
════════════════════════════
Matches RN1's trade timestamps against collected order book snapshots
to discover WHAT the book looked like when he decided to trade.

This answers: "What market conditions trigger RN1 to buy?"

Usage:
  python3.11 ob_crossref.py \
    --snapshots ob_data/ob_snapshots_*.jsonl \
    --trades rn1_analysis/rn1_enriched_trades.csv \
    --window 30

Output: rn1_signal_discovery.json — the exact OBI, spread, depth, and
price movement patterns that precede his entries.
"""

import os
import sys
import json
import csv
import gzip
import glob
import argparse
import logging
from datetime import datetime, timezone
from collections import defaultdict, Counter

log = logging.getLogger("crossref")


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                        datefmt="%H:%M:%S")


def safe_float(v, default=0.0):
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def safe_int(v, default=0):
    try:
        return int(v)
    except (ValueError, TypeError):
        return default


# ─── LOAD SNAPSHOTS ────────────────────────────────────────────────
def load_snapshots(snapshot_files):
    """Load all JSONL snapshot files into a time-indexed structure.
    Returns dict: token_id_suffix → [(timestamp, snapshot_data), ...]
    """
    log.info(f"Loading snapshots from {len(snapshot_files)} files...")
    
    # Indexed by token ID suffix → list of (ts, data)
    by_token = defaultdict(list)
    total = 0
    
    for filepath in sorted(snapshot_files):
        opener = gzip.open if filepath.endswith(".gz") else open
        mode = "rt" if filepath.endswith(".gz") else "r"
        
        log.info(f"  Reading {os.path.basename(filepath)}...")
        try:
            with opener(filepath, mode, encoding="utf-8", errors="replace") as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        snap = json.loads(line)
                        tid = snap.get("tid", "")
                        ts = snap.get("ts", 0)
                        if tid and ts > 0:
                            by_token[tid].append((ts, snap))
                            total += 1
                    except json.JSONDecodeError:
                        continue
                    
                    if total % 500000 == 0:
                        log.info(f"    Loaded {total:,} snapshots...")
        except Exception as e:
            log.warning(f"  Error reading {filepath}: {e}")
    
    # Sort each token's snapshots by time
    for tid in by_token:
        by_token[tid].sort(key=lambda x: x[0])
    
    log.info(f"Loaded {total:,} snapshots across {len(by_token):,} tokens")
    return by_token


# ─── LOAD RN1 TRADES ──────────────────────────────────────────────
def load_trades(trades_file):
    """Load RN1's enriched trades CSV."""
    log.info(f"Loading trades from {trades_file}...")
    
    trades = []
    
    # Detect delimiter
    with open(trades_file, "r", encoding="utf-8", errors="replace") as f:
        first_line = f.readline()
        delimiter = "\t" if "\t" in first_line else ","
    
    with open(trades_file, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        
        for row in reader:
            # Find timestamp column
            ts = 0
            for col in ["timestamp", "ts", "epoch", "created_at", "time", "timeStamp"]:
                if col in row:
                    val = row[col]
                    ts = safe_float(val)
                    # If it looks like milliseconds, convert
                    if ts > 1e12:
                        ts = ts / 1000.0
                    # If it looks like a date string, parse it
                    if ts == 0 and val:
                        try:
                            dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                            ts = dt.timestamp()
                        except:
                            pass
                    if ts > 0:
                        break
            
            if ts == 0:
                continue
            
            # Find action type
            action = ""
            for col in ["action", "type", "side", "trade_type"]:
                if col in row:
                    action = str(row[col]).upper()
                    break
            
            # Find token ID
            token_id = ""
            for col in ["token_id", "tokenId", "asset_id", "assetId", "outcome_token_id"]:
                if col in row:
                    token_id = str(row[col])
                    break
            
            # Find price
            price = 0
            for col in ["price", "avg_price", "fill_price"]:
                if col in row:
                    price = safe_float(row[col])
                    if price > 0:
                        break
            
            # Find size/cost
            cost = 0
            for col in ["cost", "amount", "size", "usd_value", "total"]:
                if col in row:
                    cost = safe_float(row[col])
                    if cost > 0:
                        break
            
            # Find side (YES/NO)
            side = ""
            for col in ["outcome", "side", "token_side"]:
                if col in row:
                    s = str(row[col]).upper()
                    if s in ("YES", "NO", "0", "1"):
                        side = "YES" if s in ("YES", "0") else "NO"
                        break
            
            # Find title
            title = ""
            for col in ["title", "market_title", "question", "market"]:
                if col in row:
                    title = str(row[col])
                    if title:
                        break
            
            trade = {
                "ts": ts,
                "action": action,
                "token_id": token_id,
                "token_suffix": token_id[-12:] if len(token_id) > 12 else token_id,
                "price": price,
                "cost": cost,
                "side": side,
                "title": title,
            }
            trades.append(trade)
    
    # Sort by timestamp
    trades.sort(key=lambda x: x["ts"])
    
    # Filter to buys only
    buys = [t for t in trades if "BUY" in t["action"] or t["action"] in ("", "TRADE")]
    
    log.info(f"Loaded {len(trades):,} total trades, {len(buys):,} buys")
    return buys


# ─── CROSS-REFERENCE ─────────────────────────────────────────────
def find_nearest_snapshot(token_snapshots, trade_ts, window_sec=30):
    """Find the snapshot closest to (but before) the trade timestamp.
    Returns the snapshot within window_sec before the trade, or None.
    """
    if not token_snapshots:
        return None
    
    # Binary search for the insertion point
    lo, hi = 0, len(token_snapshots) - 1
    best = None
    
    while lo <= hi:
        mid = (lo + hi) // 2
        snap_ts = token_snapshots[mid][0]
        
        if snap_ts <= trade_ts:
            # This snapshot is before or at the trade
            if trade_ts - snap_ts <= window_sec:
                best = token_snapshots[mid]
            lo = mid + 1
        else:
            hi = mid - 1
    
    return best


def crossreference(snapshots_by_token, trades, window_sec=30):
    """Match each RN1 trade to the nearest order book snapshot."""
    log.info(f"Cross-referencing {len(trades):,} trades against snapshots (window: {window_sec}s)...")
    
    matched = 0
    unmatched = 0
    
    # Aggregate stats
    entry_conditions = {
        "obi_at_entry": [],
        "spread_at_entry": [],
        "mid_at_entry": [],
        "bid_depth_1_at_entry": [],
        "bid_depth_3_at_entry": [],
        "ask_depth_1_at_entry": [],
        "total_bid_vol_at_entry": [],
        "total_ask_vol_at_entry": [],
        "price_change_at_entry": [],
        "bid_levels_at_entry": [],
        "ask_levels_at_entry": [],
    }
    
    # By price bucket
    by_price_bucket = defaultdict(lambda: defaultdict(list))
    # By category
    by_category = defaultdict(lambda: defaultdict(list))
    
    matched_trades = []
    
    for trade in trades:
        suffix = trade["token_suffix"]
        token_snaps = snapshots_by_token.get(suffix, [])
        
        if not token_snaps:
            unmatched += 1
            continue
        
        snap = find_nearest_snapshot(token_snaps, trade["ts"], window_sec)
        if not snap:
            unmatched += 1
            continue
        
        matched += 1
        snap_ts, snap_data = snap
        
        obi = snap_data.get("obi", 0)
        spread = snap_data.get("sp", 0)
        mid = snap_data.get("mid", 0)
        chg = snap_data.get("chg", 0)
        bv = snap_data.get("bv", 0)
        av = snap_data.get("av", 0)
        bd1 = snap_data.get("bd1", 0)
        bd3 = snap_data.get("bd3", 0)
        ad1 = snap_data.get("ad1", 0)
        nl = snap_data.get("nl", 0)
        na = snap_data.get("na", 0)
        
        entry_conditions["obi_at_entry"].append(obi)
        entry_conditions["spread_at_entry"].append(spread)
        entry_conditions["mid_at_entry"].append(mid)
        entry_conditions["bid_depth_1_at_entry"].append(bd1)
        entry_conditions["bid_depth_3_at_entry"].append(bd3)
        entry_conditions["ask_depth_1_at_entry"].append(ad1)
        entry_conditions["total_bid_vol_at_entry"].append(bv)
        entry_conditions["total_ask_vol_at_entry"].append(av)
        entry_conditions["price_change_at_entry"].append(chg)
        entry_conditions["bid_levels_at_entry"].append(nl)
        entry_conditions["ask_levels_at_entry"].append(na)
        
        # Bucket by NO price
        price = trade["price"]
        if price < 0.10:
            bucket = "penny"
        elif price < 0.30:
            bucket = "low"
        elif price < 0.50:
            bucket = "mid_low"
        elif price < 0.70:
            bucket = "mid_high"
        elif price < 0.90:
            bucket = "fav"
        else:
            bucket = "nc"
        
        by_price_bucket[bucket]["obi"].append(obi)
        by_price_bucket[bucket]["spread"].append(spread)
        by_price_bucket[bucket]["depth"].append(bv + av)
        by_price_bucket[bucket]["chg"].append(chg)
        
        matched_trades.append({
            "ts": trade["ts"],
            "price": price,
            "cost": trade["cost"],
            "obi": obi,
            "spread": spread,
            "chg": chg,
            "bv": bv,
            "av": av,
        })
    
    log.info(f"Matched: {matched:,} | Unmatched: {unmatched:,} "
             f"({matched/(matched+unmatched)*100:.1f}% match rate)")
    
    return matched, unmatched, entry_conditions, by_price_bucket, matched_trades


def percentiles(arr, pcts=[10, 25, 50, 75, 90]):
    if not arr:
        return {}
    s = sorted(arr)
    n = len(s)
    result = {"count": n, "mean": round(sum(s)/n, 4)}
    for p in pcts:
        idx = int(n * p / 100)
        idx = min(idx, n - 1)
        result[f"p{p}"] = round(s[idx], 4)
    result["min"] = round(s[0], 4)
    result["max"] = round(s[-1], 4)
    return result


# ─── MAIN ─────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Cross-reference RN1 trades with OB snapshots")
    parser.add_argument("--snapshots", nargs="+", required=True,
                       help="Snapshot JSONL files (glob patterns work)")
    parser.add_argument("--trades", required=True,
                       help="RN1 enriched trades CSV")
    parser.add_argument("--window", type=int, default=30,
                       help="Max seconds between snapshot and trade (default: 30)")
    parser.add_argument("--output", default="rn1_signal_discovery.json",
                       help="Output file")
    args = parser.parse_args()
    
    setup_logging()
    
    # Expand glob patterns
    snapshot_files = []
    for pattern in args.snapshots:
        expanded = glob.glob(pattern)
        snapshot_files.extend(expanded)
    snapshot_files = sorted(set(snapshot_files))
    
    if not snapshot_files:
        log.error(f"No snapshot files found matching: {args.snapshots}")
        sys.exit(1)
    
    print("=" * 60)
    print("RN1 Signal Cross-Reference")
    print("=" * 60)
    print(f"  Snapshot files: {len(snapshot_files)}")
    print(f"  Trades file:    {args.trades}")
    print(f"  Window:         {args.window}s")
    print("=" * 60)
    
    # Load data
    snapshots_by_token = load_snapshots(snapshot_files)
    trades = load_trades(args.trades)
    
    if not trades:
        log.error("No trades loaded!")
        sys.exit(1)
    
    # Cross-reference
    matched, unmatched, conditions, by_bucket, matched_trades = \
        crossreference(snapshots_by_token, trades, args.window)
    
    # Build results
    results = {
        "summary": {
            "total_trades": len(trades),
            "matched": matched,
            "unmatched": unmatched,
            "match_rate_pct": round(matched / (matched + unmatched) * 100, 1) if matched + unmatched > 0 else 0,
            "window_sec": args.window,
            "snapshot_files": len(snapshot_files),
            "snapshot_tokens": len(snapshots_by_token),
        },
        "entry_conditions": {},
        "by_price_bucket": {},
        "signal_candidates": {},
    }
    
    # Percentiles for each condition
    for key, values in conditions.items():
        if values:
            results["entry_conditions"][key] = percentiles(values)
    
    # By price bucket
    for bucket, metrics in by_bucket.items():
        results["by_price_bucket"][bucket] = {}
        for metric, values in metrics.items():
            results["by_price_bucket"][bucket][metric] = percentiles(values)
    
    # Signal candidate detection
    # What % of his entries have OBI > 0.5? > 0.6? > 0.7?
    if conditions["obi_at_entry"]:
        obi_vals = conditions["obi_at_entry"]
        n = len(obi_vals)
        results["signal_candidates"]["obi_thresholds"] = {
            "above_0.3": round(len([v for v in obi_vals if v > 0.3]) / n * 100, 1),
            "above_0.4": round(len([v for v in obi_vals if v > 0.4]) / n * 100, 1),
            "above_0.5": round(len([v for v in obi_vals if v > 0.5]) / n * 100, 1),
            "above_0.6": round(len([v for v in obi_vals if v > 0.6]) / n * 100, 1),
            "above_0.7": round(len([v for v in obi_vals if v > 0.7]) / n * 100, 1),
            "below_neg0.3": round(len([v for v in obi_vals if v < -0.3]) / n * 100, 1),
        }
    
    if conditions["spread_at_entry"]:
        sp_vals = conditions["spread_at_entry"]
        n = len(sp_vals)
        results["signal_candidates"]["spread_thresholds"] = {
            "under_0.01": round(len([v for v in sp_vals if v < 0.01]) / n * 100, 1),
            "under_0.02": round(len([v for v in sp_vals if v < 0.02]) / n * 100, 1),
            "under_0.03": round(len([v for v in sp_vals if v < 0.03]) / n * 100, 1),
            "under_0.05": round(len([v for v in sp_vals if v < 0.05]) / n * 100, 1),
            "under_0.10": round(len([v for v in sp_vals if v < 0.10]) / n * 100, 1),
        }
    
    if conditions["price_change_at_entry"]:
        chg_vals = conditions["price_change_at_entry"]
        n = len(chg_vals)
        results["signal_candidates"]["price_change_direction"] = {
            "rising_pct": round(len([v for v in chg_vals if v > 0.001]) / n * 100, 1),
            "falling_pct": round(len([v for v in chg_vals if v < -0.001]) / n * 100, 1),
            "flat_pct": round(len([v for v in chg_vals if abs(v) <= 0.001]) / n * 100, 1),
            "spike_up_3pct": round(len([v for v in chg_vals if v > 0.03]) / n * 100, 1),
            "spike_down_3pct": round(len([v for v in chg_vals if v < -0.03]) / n * 100, 1),
        }
    
    # Save
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)
    
    # Print summary
    print(f"\n{'=' * 60}")
    print("SIGNAL DISCOVERY RESULTS")
    print(f"{'=' * 60}")
    
    print(f"\n  Matched {matched:,} of {len(trades):,} trades ({results['summary']['match_rate_pct']}%)")
    
    ec = results["entry_conditions"]
    if ec.get("obi_at_entry"):
        o = ec["obi_at_entry"]
        print(f"\n  OBI at entry:     median={o['p50']}, mean={o['mean']}")
        print(f"    p10={o['p10']}, p25={o['p25']}, p75={o['p75']}, p90={o['p90']}")
    
    if ec.get("spread_at_entry"):
        s = ec["spread_at_entry"]
        print(f"\n  Spread at entry:  median={s['p50']}, mean={s['mean']}")
    
    if ec.get("price_change_at_entry"):
        c = ec["price_change_at_entry"]
        print(f"\n  Price chg at entry: median={c['p50']}, mean={c['mean']}")
    
    sc = results.get("signal_candidates", {})
    if sc.get("obi_thresholds"):
        print(f"\n  OBI signal strength:")
        for k, v in sc["obi_thresholds"].items():
            print(f"    {k}: {v}% of entries")
    
    if sc.get("spread_thresholds"):
        print(f"\n  Spread tightness:")
        for k, v in sc["spread_thresholds"].items():
            print(f"    {k}: {v}% of entries")
    
    if sc.get("price_change_direction"):
        print(f"\n  Price direction at entry:")
        for k, v in sc["price_change_direction"].items():
            print(f"    {k}: {v}%")
    
    print(f"\n  Full results: {args.output}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
