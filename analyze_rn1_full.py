#!/usr/bin/env python3
"""
RN1 Full History Analyzer
=========================
Run this locally against your big data files (200MB+ and 600MB+).
It will extract the key stats we need for bot calibration and produce
a compact JSON summary you can paste back into Claude.

Usage:
  python analyze_rn1_full.py <file1.csv> [file2.csv]
  
  # Or if it's JSON:
  python analyze_rn1_full.py <file1.json> [file2.json]

It auto-detects CSV vs JSON and figures out the columns.
"""

import sys
import os
import json
import csv
from collections import Counter, defaultdict
from datetime import datetime
from io import StringIO

def detect_format(filepath):
    """Detect if file is CSV or JSON."""
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        first_chars = f.read(100).strip()
        if first_chars.startswith('[') or first_chars.startswith('{'):
            return 'json'
        return 'csv'

def load_file(filepath):
    """Load file as list of dicts."""
    fmt = detect_format(filepath)
    print(f"  Detected format: {fmt}")
    
    if fmt == 'json':
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            data = json.load(f)
        if isinstance(data, dict):
            # Could be {key: [records]} or {key: record}
            for key in data:
                if isinstance(data[key], list) and len(data[key]) > 0:
                    return data[key]
            # Wrap single records
            return [data]
        return data
    else:
        rows = []
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        return rows

def find_columns(sample):
    """Auto-detect column names for key fields."""
    cols = list(sample.keys())
    print(f"  Columns found: {cols}")
    
    mapping = {}
    for col in cols:
        cl = col.lower().strip()
        if cl in ('timestamp', 'ts', 'time', 'created_at', 'date', 'datetime'):
            mapping['timestamp'] = col
        elif cl in ('epoch', 'unix_timestamp', 'unix_ts', 'unix'):
            mapping['epoch'] = col
        elif cl in ('trade_type', 'type', 'action', 'side_type', 'direction'):
            mapping['trade_type'] = col
        elif cl in ('side', 'outcome', 'token_side'):
            mapping['side'] = col
        elif cl in ('price', 'avg_price', 'trade_price', 'fill_price'):
            mapping['price'] = col
        elif cl in ('shares', 'amount', 'quantity', 'size', 'num_shares'):
            mapping['shares'] = col
        elif cl in ('cost', 'total_cost', 'notional', 'dollar_amount'):
            mapping['cost'] = col
        elif cl in ('revenue', 'proceeds', 'payout', 'return'):
            mapping['revenue'] = col
        elif cl in ('title', 'market', 'market_title', 'question', 'event', 'market_name'):
            mapping['title'] = col
        elif cl in ('condition_id', 'market_id', 'id'):
            mapping['market_id'] = col
        elif cl in ('profit', 'pnl', 'profit_loss', 'gain_loss'):
            mapping['profit'] = col
        elif cl in ('resolved', 'settled', 'outcome_resolved'):
            mapping['resolved'] = col
        elif cl in ('hour',):
            mapping['hour'] = col
    
    print(f"  Column mapping: {mapping}")
    return mapping

def safe_float(val, default=0.0):
    try:
        if val is None or val == '':
            return default
        return float(val)
    except (ValueError, TypeError):
        return default

def safe_int(val, default=0):
    try:
        if val is None or val == '':
            return default
        return int(float(val))
    except (ValueError, TypeError):
        return default

def analyze(records, mapping):
    """Analyze trade records and produce summary."""
    results = {
        'total_records': len(records),
        'columns_detected': mapping,
        'sample_record': {k: str(v)[:100] for k, v in records[0].items()} if records else {},
    }
    
    # Extract key fields
    buys = []
    sells = []
    all_trades = []
    
    for r in records:
        trade = {}
        
        # Get trade type
        tt = r.get(mapping.get('trade_type', ''), '').upper()
        if 'BUY' in tt:
            trade['type'] = 'BUY'
        elif 'SELL' in tt:
            trade['type'] = 'SELL'
        else:
            trade['type'] = tt
        
        # Get side
        trade['side'] = r.get(mapping.get('side', ''), '').upper()
        if 'NO' in trade['side']:
            trade['side'] = 'NO'
        elif 'YES' in trade['side']:
            trade['side'] = 'YES'
        
        # Get price
        trade['price'] = safe_float(r.get(mapping.get('price', '')))
        
        # Get cost
        trade['cost'] = safe_float(r.get(mapping.get('cost', '')))
        
        # Get shares
        trade['shares'] = safe_float(r.get(mapping.get('shares', '')))
        
        # Get revenue
        trade['revenue'] = safe_float(r.get(mapping.get('revenue', '')))
        
        # Get profit if available
        trade['profit'] = safe_float(r.get(mapping.get('profit', '')))
        
        # Get title
        trade['title'] = str(r.get(mapping.get('title', ''), ''))[:100]
        
        # Get market ID
        trade['market_id'] = str(r.get(mapping.get('market_id', ''), ''))
        
        # Get timestamp
        epoch = safe_float(r.get(mapping.get('epoch', '')))
        ts_str = r.get(mapping.get('timestamp', ''), '')
        if epoch > 0:
            trade['epoch'] = epoch
        elif ts_str:
            try:
                dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                trade['epoch'] = dt.timestamp()
            except:
                trade['epoch'] = 0
        else:
            trade['epoch'] = 0
        
        # Get hour
        if 'hour' in mapping:
            trade['hour'] = safe_int(r.get(mapping['hour']))
        elif trade['epoch'] > 0:
            trade['hour'] = datetime.utcfromtimestamp(trade['epoch']).hour
        else:
            trade['hour'] = -1
        
        # Resolved?
        trade['resolved'] = r.get(mapping.get('resolved', ''), '')
        
        all_trades.append(trade)
        if trade['type'] == 'BUY':
            buys.append(trade)
        elif trade['type'] == 'SELL':
            sells.append(trade)
    
    # ─── BASIC STATS ───
    results['trade_counts'] = {
        'total': len(all_trades),
        'buys': len(buys),
        'sells': len(sells),
        'other': len(all_trades) - len(buys) - len(sells),
    }
    
    results['side_counts'] = {
        'buy_no': len([t for t in buys if t['side'] == 'NO']),
        'buy_yes': len([t for t in buys if t['side'] == 'YES']),
        'sell_no': len([t for t in sells if t['side'] == 'NO']),
        'sell_yes': len([t for t in sells if t['side'] == 'YES']),
    }
    
    # ─── VOLUME ───
    total_cost = sum(t['cost'] for t in buys)
    total_revenue = sum(t['revenue'] for t in all_trades)
    total_profit = sum(t['profit'] for t in all_trades)
    results['volume'] = {
        'total_cost': round(total_cost, 2),
        'total_revenue': round(total_revenue, 2),
        'total_profit': round(total_profit, 2),
        'avg_trade_cost': round(total_cost / len(buys), 2) if buys else 0,
    }
    
    # ─── PRICE DISTRIBUTION (buys only) ───
    price_buckets = {}
    for label, lo, hi in [
        ('penny_0-10', 0, 0.10), ('low_10-20', 0.10, 0.20),
        ('mid-low_20-40', 0.20, 0.40), ('mid_40-60', 0.40, 0.60),
        ('fav_60-80', 0.60, 0.80), ('NC_80-90', 0.80, 0.90),
        ('heavy-NC_90+', 0.90, 1.01),
    ]:
        bucket = [t for t in buys if lo <= t['price'] < hi]
        if bucket:
            costs = [t['cost'] for t in bucket]
            price_buckets[label] = {
                'count': len(bucket),
                'pct': round(len(bucket)/len(buys)*100, 1),
                'avg_cost': round(sum(costs)/len(costs), 2),
                'med_cost': round(sorted(costs)[len(costs)//2], 2),
                'total_cost': round(sum(costs), 2),
            }
    results['price_distribution'] = price_buckets
    
    # ─── HOURLY DISTRIBUTION ───
    hour_counts = Counter(t['hour'] for t in all_trades if t['hour'] >= 0)
    results['hourly_distribution'] = {str(h): hour_counts.get(h, 0) for h in range(24)}
    
    # ─── SIZE PERCENTILES ───
    buy_costs = sorted(t['cost'] for t in buys if t['cost'] > 0)
    if buy_costs:
        results['size_percentiles'] = {
            'p10': round(buy_costs[int(len(buy_costs)*0.1)], 2),
            'p25': round(buy_costs[int(len(buy_costs)*0.25)], 2),
            'p50': round(buy_costs[int(len(buy_costs)*0.5)], 2),
            'p75': round(buy_costs[int(len(buy_costs)*0.75)], 2),
            'p90': round(buy_costs[int(len(buy_costs)*0.9)], 2),
            'p95': round(buy_costs[int(len(buy_costs)*0.95)], 2),
            'p99': round(buy_costs[int(len(buy_costs)*0.99)], 2),
            'max': round(max(buy_costs), 2),
        }
    
    # ─── WIN RATE ───
    # Try to calculate from resolved trades with profit/revenue
    resolved_buys = [t for t in buys if t['profit'] != 0 or t['revenue'] > 0]
    if resolved_buys:
        wins = [t for t in resolved_buys if t['profit'] > 0 or t['revenue'] > t['cost']]
        losses = [t for t in resolved_buys if t['profit'] < 0 or (t['revenue'] > 0 and t['revenue'] < t['cost'])]
        results['win_rate'] = {
            'resolved_trades': len(resolved_buys),
            'wins': len(wins),
            'losses': len(losses),
            'win_pct': round(len(wins)/len(resolved_buys)*100, 1) if resolved_buys else 0,
            'avg_win': round(sum(t['profit'] for t in wins)/len(wins), 2) if wins else 0,
            'avg_loss': round(sum(t['profit'] for t in losses)/len(losses), 2) if losses else 0,
        }
    
    # ─── MARKET CONCENTRATION ───
    market_trades = defaultdict(list)
    for t in buys:
        key = t['market_id'] or t['title']
        market_trades[key].append(t)
    
    total_markets = len(market_trades)
    single_buy = sum(1 for v in market_trades.values() if len(v) == 1)
    multi_buy = sum(1 for v in market_trades.values() if len(v) > 1)
    
    results['market_concentration'] = {
        'total_markets': total_markets,
        'single_buy_markets': single_buy,
        'multi_buy_markets': multi_buy,
        'avg_buys_per_market': round(len(buys)/total_markets, 1) if total_markets else 0,
    }
    
    # Top markets by cost
    top_markets = sorted(market_trades.items(), 
                        key=lambda x: sum(t['cost'] for t in x[1]), 
                        reverse=True)[:10]
    results['top_markets'] = [
        {
            'title': v[0]['title'][:60],
            'buys': len(v),
            'total_cost': round(sum(t['cost'] for t in v), 2),
        }
        for _, v in top_markets
    ]
    
    # ─── SELL ANALYSIS ───
    if sells:
        sell_prices = [t['price'] for t in sells if t['price'] > 0]
        results['sell_analysis'] = {
            'total_sells': len(sells),
            'sell_rate_pct': round(len(sells)/len(all_trades)*100, 2),
            'avg_sell_price': round(sum(sell_prices)/len(sell_prices), 4) if sell_prices else 0,
            'sell_revenue': round(sum(t['revenue'] for t in sells), 2),
        }
    
    # ─── TIMING (if sorted by epoch) ───
    sorted_trades = sorted(all_trades, key=lambda t: t['epoch'])
    sorted_trades = [t for t in sorted_trades if t['epoch'] > 0]
    if len(sorted_trades) > 10:
        gaps = []
        for i in range(1, len(sorted_trades)):
            gap = sorted_trades[i]['epoch'] - sorted_trades[i-1]['epoch']
            if 0 < gap < 86400 * 30:  # filter obvious date issues
                gaps.append(gap)
        if gaps:
            gaps.sort()
            results['inter_trade_timing'] = {
                'median_gap_sec': round(gaps[len(gaps)//2], 0),
                'mean_gap_sec': round(sum(gaps)/len(gaps), 0),
                'p10_gap_sec': round(gaps[int(len(gaps)*0.1)], 0),
                'p90_gap_sec': round(gaps[int(len(gaps)*0.9)], 0),
                'under_5s_pct': round(len([g for g in gaps if g < 5])/len(gaps)*100, 1),
                'under_60s_pct': round(len([g for g in gaps if g < 60])/len(gaps)*100, 1),
            }
    
    # ─── DATE RANGE ───
    if sorted_trades:
        first_epoch = sorted_trades[0]['epoch']
        last_epoch = sorted_trades[-1]['epoch']
        results['date_range'] = {
            'first': datetime.utcfromtimestamp(first_epoch).isoformat() if first_epoch > 0 else 'unknown',
            'last': datetime.utcfromtimestamp(last_epoch).isoformat() if last_epoch > 0 else 'unknown',
            'span_days': round((last_epoch - first_epoch) / 86400, 0) if first_epoch > 0 and last_epoch > 0 else 0,
        }
    
    return results


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_rn1_full.py <file1> [file2]")
        print("Supports CSV and JSON files (auto-detected)")
        sys.exit(1)
    
    all_results = {}
    
    for filepath in sys.argv[1:]:
        print(f"\n{'='*60}")
        print(f"Analyzing: {filepath}")
        print(f"File size: {os.path.getsize(filepath) / 1024 / 1024:.1f} MB")
        print(f"{'='*60}")
        
        records = load_file(filepath)
        print(f"  Records loaded: {len(records)}")
        
        if not records:
            print("  ERROR: No records found")
            continue
        
        mapping = find_columns(records[0])
        results = analyze(records, mapping)
        
        basename = os.path.basename(filepath)
        all_results[basename] = results
        
        # Print key stats
        print(f"\n  SUMMARY:")
        print(f"  Trades: {results['trade_counts']}")
        print(f"  Sides: {results['side_counts']}")
        print(f"  Volume: {results['volume']}")
        if 'win_rate' in results:
            print(f"  Win Rate: {results['win_rate']}")
        if 'size_percentiles' in results:
            print(f"  Size Percentiles: {results['size_percentiles']}")
        if 'sell_analysis' in results:
            print(f"  Sells: {results['sell_analysis']}")
    
    # Save compact JSON for pasting to Claude
    output_path = 'rn1_analysis_summary.json'
    with open(output_path, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    print(f"\n{'='*60}")
    print(f"Full analysis saved to: {output_path}")
    print(f"File size: {os.path.getsize(output_path) / 1024:.1f} KB")
    print(f"\nPaste the contents of {output_path} into Claude for full calibration!")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
