#!/usr/bin/env python3
"""
RN1 Deep Analyzer — Run locally on your big files
═══════════════════════════════════════════════════
Handles: enriched_trades.csv, full_history.csv, or any trade CSV/JSON.

Produces a compact JSON summary (~15KB) you can paste into Claude.

Usage:
  python3.11 analyze_rn1_deep.py enriched_trades.csv
  python3.11 analyze_rn1_deep.py enriched_trades.csv full_history.csv
  python3.11 analyze_rn1_deep.py *.csv

What it calculates:
  1. Phase analysis (trades 1-1K, 1-2K, 1-5K, 1-10K, all)
  2. Side evolution (when does YES buying start?)
  3. Size scaling trajectory (how does trade size grow?)
  4. Bankroll growth estimation from merge/resolution data
  5. Accumulation patterns (single-buy % over time)
  6. Price distribution per phase
  7. Win rate from merge events (NO merges @ $1 = wins)
  8. Sell behavior (when, at what price, how often)
  9. Inter-trade timing evolution
  10. Daily/weekly volume trajectory
"""

import sys, os, json, csv, re
from collections import Counter, defaultdict
from datetime import datetime, timezone

def safe_float(val, default=0.0):
    try:
        if val is None or val == '': return default
        return float(str(val).replace(',', ''))
    except: return default

def safe_int(val, default=0):
    try:
        if val is None or val == '': return default
        return int(float(str(val)))
    except: return default


# ─── FILE LOADING ───────────────────────────────────────────────────
def load_csv(filepath):
    """Load CSV with auto-detection of delimiter and encoding."""
    print(f"  Loading {filepath}...")
    
    # Try to detect delimiter
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        sample = f.read(5000)
    
    # Check if it's tab-separated or comma-separated
    if sample.count('\t') > sample.count(','):
        delimiter = '\t'
    else:
        delimiter = ','
    
    rows = []
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for i, row in enumerate(reader):
            rows.append(row)
            if i % 100000 == 0 and i > 0:
                print(f"    ...loaded {i:,} rows")
    
    print(f"  Total rows: {len(rows):,}")
    if rows:
        print(f"  Columns: {list(rows[0].keys())}")
        print(f"  Sample row: { {k: str(v)[:50] for k, v in rows[0].items()} }")
    return rows

def load_json(filepath):
    """Load JSON file."""
    print(f"  Loading {filepath}...")
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        data = json.load(f)
    if isinstance(data, list):
        print(f"  Total records: {len(data):,}")
        return data
    elif isinstance(data, dict):
        # Try to find the array of records
        for key in data:
            if isinstance(data[key], list) and len(data[key]) > 0:
                print(f"  Found records under key '{key}': {len(data[key]):,}")
                return data[key]
    return []


# ─── COLUMN DETECTION ──────────────────────────────────────────────
def detect_columns(sample_row):
    """Auto-detect which columns map to what fields."""
    cols = list(sample_row.keys())
    mapping = {}
    
    for col in cols:
        cl = col.lower().strip().replace(' ', '_')
        
        # Timestamp
        if cl in ('timestamp', 'ts', 'time', 'created_at', 'datetime', 'date_time',
                   'execution_time', 'trade_time', 'fill_time'):
            mapping['timestamp'] = col
        
        # Epoch
        elif cl in ('epoch', 'unix_timestamp', 'unix_ts', 'unix', 'timestamp_epoch',
                     'epoch_time', 'time_epoch'):
            mapping['epoch'] = col
        
        # Date / Hour
        elif cl in ('date', 'trade_date'):
            mapping['date'] = col
        elif cl in ('hour', 'trade_hour'):
            mapping['hour'] = col
        
        # Action type
        elif cl in ('trade_type', 'type', 'action', 'side_type', 'direction',
                     'order_type', 'transaction_type', 'tx_type'):
            mapping['action'] = col
        
        # YES/NO side
        elif cl in ('side', 'outcome', 'token_side', 'outcome_side', 'bet_side'):
            mapping['side'] = col
        
        # Price
        elif cl in ('price', 'avg_price', 'trade_price', 'fill_price', 'execution_price',
                     'average_price'):
            mapping['price'] = col
        
        # Shares / Amount
        elif cl in ('shares', 'amount', 'quantity', 'size', 'num_shares', 'fill_size',
                     'asset_amount', 'share_amount'):
            mapping['shares'] = col
        
        # Cost in USDC
        elif cl in ('cost', 'total_cost', 'notional', 'dollar_amount', 'usdc_amount',
                     'value', 'trade_value', 'fill_cost'):
            mapping['cost'] = col
        
        # Revenue / Payout
        elif cl in ('revenue', 'proceeds', 'payout', 'return', 'redemption'):
            mapping['revenue'] = col
        
        # Title / Market
        elif cl in ('title', 'market', 'market_title', 'question', 'event',
                     'market_name', 'description', 'market_question'):
            mapping['title'] = col
        
        # Market ID
        elif cl in ('condition_id', 'market_id', 'id', 'token_id', 'asset_id'):
            if 'market_id' not in mapping:  # prefer condition_id
                mapping['market_id'] = col
        
        # Profit
        elif cl in ('profit', 'pnl', 'profit_loss', 'gain_loss', 'realized_pnl'):
            mapping['profit'] = col
    
    print(f"  Column mapping: {mapping}")
    return mapping


# ─── TRADE NORMALIZATION ──────────────────────────────────────────
def normalize_trades(rows, mapping):
    """Convert raw rows to normalized trade dicts."""
    trades = []
    for r in rows:
        t = {}
        
        # Action
        action_raw = str(r.get(mapping.get('action', ''), '')).upper().strip()
        if 'BUY' in action_raw:
            t['action'] = 'BUY'
        elif 'SELL' in action_raw:
            t['action'] = 'SELL'
        elif 'MERGE' in action_raw:
            t['action'] = 'MERGE'
        else:
            t['action'] = action_raw
        
        # Side
        side_raw = str(r.get(mapping.get('side', ''), '')).upper().strip()
        t['side'] = 'NO' if 'NO' in side_raw else ('YES' if 'YES' in side_raw else side_raw)
        
        # Numeric fields
        t['price'] = safe_float(r.get(mapping.get('price', '')))
        t['shares'] = safe_float(r.get(mapping.get('shares', '')))
        t['cost'] = safe_float(r.get(mapping.get('cost', '')))
        t['revenue'] = safe_float(r.get(mapping.get('revenue', '')))
        t['profit'] = safe_float(r.get(mapping.get('profit', '')))
        
        # If cost is 0 but we have price and shares, compute it
        if t['cost'] == 0 and t['price'] > 0 and t['shares'] > 0:
            t['cost'] = t['price'] * t['shares']
        
        # Title
        t['title'] = str(r.get(mapping.get('title', ''), ''))[:80]
        t['market_id'] = str(r.get(mapping.get('market_id', ''), ''))
        
        # Timestamp
        epoch = safe_float(r.get(mapping.get('epoch', '')))
        ts_str = str(r.get(mapping.get('timestamp', ''), ''))
        if epoch > 1_000_000_000:
            t['epoch'] = epoch
        elif ts_str:
            try:
                dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                t['epoch'] = dt.timestamp()
            except:
                t['epoch'] = 0
        else:
            t['epoch'] = 0
        
        # Hour
        if 'hour' in mapping:
            t['hour'] = safe_int(r.get(mapping['hour']))
        elif t['epoch'] > 0:
            t['hour'] = datetime.utcfromtimestamp(t['epoch']).hour
        else:
            t['hour'] = -1
        
        # Date
        if 'date' in mapping:
            t['date'] = str(r.get(mapping['date'], ''))
        elif t['epoch'] > 0:
            t['date'] = datetime.utcfromtimestamp(t['epoch']).strftime('%Y-%m-%d')
        else:
            t['date'] = ''
        
        trades.append(t)
    
    # Sort by epoch
    trades.sort(key=lambda x: x['epoch'])
    return trades


# ─── PHASE ANALYZER ────────────────────────────────────────────────
def analyze_phase(trades, label="all"):
    """Analyze a slice of trades."""
    result = {'label': label, 'total': len(trades)}
    
    buys = [t for t in trades if t['action'] == 'BUY']
    sells = [t for t in trades if t['action'] == 'SELL']
    merges = [t for t in trades if t['action'] == 'MERGE']
    
    result['counts'] = {
        'buys': len(buys), 'sells': len(sells), 'merges': len(merges),
        'other': len(trades) - len(buys) - len(sells) - len(merges),
    }
    
    # Side split
    buy_no = [t for t in buys if t['side'] == 'NO']
    buy_yes = [t for t in buys if t['side'] == 'YES']
    result['sides'] = {
        'buy_no': len(buy_no), 'buy_no_pct': round(len(buy_no)/len(buys)*100, 1) if buys else 0,
        'buy_yes': len(buy_yes), 'buy_yes_pct': round(len(buy_yes)/len(buys)*100, 1) if buys else 0,
        'sell_count': len(sells),
        'sell_rate_pct': round(len(sells)/len(trades)*100, 2) if trades else 0,
    }
    
    # Volume
    buy_costs = [t['cost'] for t in buys if t['cost'] > 0]
    result['volume'] = {
        'total_buy_cost': round(sum(buy_costs), 2),
        'avg_cost': round(sum(buy_costs)/len(buy_costs), 2) if buy_costs else 0,
        'total_sell_revenue': round(sum(t['revenue'] for t in sells), 2),
    }
    
    # Size percentiles
    if buy_costs:
        s = sorted(buy_costs)
        result['size_pctiles'] = {
            'p10': round(s[int(len(s)*0.10)], 2),
            'p25': round(s[int(len(s)*0.25)], 2),
            'p50': round(s[int(len(s)*0.50)], 2),
            'p75': round(s[int(len(s)*0.75)], 2),
            'p90': round(s[int(len(s)*0.90)], 2),
            'p95': round(s[int(len(s)*0.95)], 2),
            'p99': round(s[min(int(len(s)*0.99), len(s)-1)], 2),
            'max': round(max(s), 2),
        }
    
    # Price distribution
    price_dist = {}
    for label_b, lo, hi in [
        ('penny_0-10', 0, 0.10), ('low_10-20', 0.10, 0.20),
        ('mid_20-40', 0.20, 0.40), ('mid_40-60', 0.40, 0.60),
        ('fav_60-80', 0.60, 0.80), ('NC_80-90', 0.80, 0.90),
        ('heavyNC_90+', 0.90, 1.01),
    ]:
        bkt = [t for t in buys if lo <= t['price'] < hi]
        if bkt:
            bkt_costs = [t['cost'] for t in bkt]
            price_dist[label_b] = {
                'count': len(bkt),
                'pct': round(len(bkt)/len(buys)*100, 1) if buys else 0,
                'total': round(sum(bkt_costs), 2),
                'avg': round(sum(bkt_costs)/len(bkt_costs), 2),
                'med': round(sorted(bkt_costs)[len(bkt_costs)//2], 2),
            }
    result['price_dist'] = price_dist
    
    # Accumulation
    market_buys = defaultdict(int)
    for t in buys:
        key = t['market_id'] if t['market_id'] else t['title']
        market_buys[key] += 1
    
    counts_dist = Counter(market_buys.values())
    result['accumulation'] = {
        'total_markets': len(market_buys),
        'single_buy_pct': round(sum(1 for c in market_buys.values() if c == 1)/len(market_buys)*100, 1) if market_buys else 0,
        '2_buys': sum(1 for c in market_buys.values() if c == 2),
        '3-5_buys': sum(1 for c in market_buys.values() if 3 <= c <= 5),
        '6-10_buys': sum(1 for c in market_buys.values() if 6 <= c <= 10),
        '10+_buys': sum(1 for c in market_buys.values() if c > 10),
        'max_buys': max(market_buys.values()) if market_buys else 0,
    }
    
    # Date range
    dated = [t for t in trades if t['epoch'] > 0]
    if dated:
        first = min(t['epoch'] for t in dated)
        last = max(t['epoch'] for t in dated)
        result['date_range'] = {
            'first': datetime.utcfromtimestamp(first).strftime('%Y-%m-%d'),
            'last': datetime.utcfromtimestamp(last).strftime('%Y-%m-%d'),
            'span_days': round((last - first) / 86400),
        }
        
        # Daily stats
        daily = defaultdict(lambda: {'buys': 0, 'cost': 0})
        for t in buys:
            if t['date']:
                daily[t['date']]['buys'] += 1
                daily[t['date']]['cost'] += t['cost']
        days_active = len(daily)
        result['daily'] = {
            'active_days': days_active,
            'avg_buys_per_day': round(len(buys)/days_active, 1) if days_active else 0,
            'avg_vol_per_day': round(sum(d['cost'] for d in daily.values())/days_active, 2) if days_active else 0,
        }
    
    # Merge/resolution analysis (win rate proxy)
    if merges:
        no_merges = [t for t in merges if t['side'] == 'NO']
        yes_merges = [t for t in merges if t['side'] == 'YES']
        result['merges'] = {
            'total': len(merges),
            'no_merges': len(no_merges),
            'yes_merges': len(yes_merges),
            'avg_merge_price': round(sum(t['price'] for t in merges)/len(merges), 4) if merges else 0,
            'total_merge_value': round(sum(t['shares'] for t in merges), 2),
        }
    
    # Inter-trade timing
    dated_sorted = sorted(dated, key=lambda t: t['epoch'])
    if len(dated_sorted) > 10:
        gaps = []
        for i in range(1, len(dated_sorted)):
            g = dated_sorted[i]['epoch'] - dated_sorted[i-1]['epoch']
            if 0 < g < 86400 * 7:
                gaps.append(g)
        if gaps:
            gs = sorted(gaps)
            result['timing'] = {
                'median_gap_sec': round(gs[len(gs)//2]),
                'mean_gap_sec': round(sum(gs)/len(gs)),
                'under_1s_pct': round(len([g for g in gs if g < 1])/len(gs)*100, 1),
                'under_5s_pct': round(len([g for g in gs if g < 5])/len(gs)*100, 1),
                'under_60s_pct': round(len([g for g in gs if g < 60])/len(gs)*100, 1),
                'over_1hr_pct': round(len([g for g in gs if g > 3600])/len(gs)*100, 1),
            }
    
    # Hourly distribution
    hours = Counter(t['hour'] for t in buys if t['hour'] >= 0)
    if hours:
        top3 = hours.most_common(3)
        result['peak_hours'] = {str(h): c for h, c in top3}
    
    return result


# ─── EVOLUTION TRACKER ──────────────────────────────────────────────
def track_evolution(all_trades):
    """Track how strategy evolves over trade milestones."""
    buys = [t for t in all_trades if t['action'] == 'BUY']
    
    evolution = {}
    
    # Analyze at milestones
    milestones = [100, 250, 500, 1000, 2000, 5000, 10000, 25000, 50000, 100000]
    for m in milestones:
        if len(buys) >= m:
            slice_buys = buys[:m]
            # Get all trades (including sells/merges) up to the epoch of the m-th buy
            cutoff = slice_buys[-1]['epoch']
            slice_all = [t for t in all_trades if t['epoch'] <= cutoff]
            
            no_pct = len([t for t in slice_buys if t['side'] == 'NO']) / len(slice_buys) * 100
            yes_pct = 100 - no_pct
            costs = sorted(t['cost'] for t in slice_buys if t['cost'] > 0)
            
            # Estimate bankroll from cumulative volume
            cum_vol = sum(t['cost'] for t in slice_buys)
            cum_merges = sum(t['shares'] for t in slice_all if t['action'] == 'MERGE')
            
            evolution[f"at_{m}_buys"] = {
                'total_buys': m,
                'date': slice_buys[-1].get('date', '?'),
                'no_pct': round(no_pct, 1),
                'yes_pct': round(yes_pct, 1),
                'med_cost': round(costs[len(costs)//2], 2) if costs else 0,
                'p90_cost': round(costs[int(len(costs)*0.9)], 2) if costs else 0,
                'p99_cost': round(costs[min(int(len(costs)*0.99), len(costs)-1)], 2) if costs else 0,
                'max_cost': round(max(costs), 2) if costs else 0,
                'cum_volume': round(cum_vol, 2),
                'cum_merge_value': round(cum_merges, 2),
                'single_buy_pct': round(
                    sum(1 for _ in _market_buy_counts(slice_buys).values() if _ == 1) /
                    max(len(_market_buy_counts(slice_buys)), 1) * 100, 1
                ),
                'sells_in_period': len([t for t in slice_all if t['action'] == 'SELL']),
            }
    
    return evolution


def _market_buy_counts(buys):
    counts = defaultdict(int)
    for t in buys:
        key = t['market_id'] if t['market_id'] else t['title']
        counts[key] += 1
    return counts


# ─── BANKROLL ESTIMATION ──────────────────────────────────────────
def estimate_bankroll_trajectory(all_trades):
    """Estimate bankroll over time from costs and merge payouts."""
    sorted_trades = sorted(all_trades, key=lambda t: t['epoch'])
    
    # Track cumulative: deposits - costs + merge payouts + sell revenue
    trajectory = []
    cum_cost = 0
    cum_payouts = 0
    cum_sells = 0
    
    for i, t in enumerate(sorted_trades):
        if t['action'] == 'BUY':
            cum_cost += t['cost']
        elif t['action'] == 'MERGE':
            cum_payouts += t['shares']  # NO merges pay shares × $1
        elif t['action'] == 'SELL':
            cum_sells += t['revenue']
        
        # Record at milestones
        if (i + 1) in [100, 500, 1000, 2000, 5000, 10000, 25000, 50000, 100000, 250000, 500000, 1000000]:
            net_profit = cum_payouts + cum_sells - cum_cost
            trajectory.append({
                'trade_num': i + 1,
                'date': t.get('date', '?'),
                'cum_cost': round(cum_cost, 2),
                'cum_payouts': round(cum_payouts, 2),
                'cum_sells': round(cum_sells, 2),
                'net_profit': round(net_profit, 2),
            })
    
    return trajectory


# ─── MAIN ───────────────────────────────────────────────────────────
def main():
    if len(sys.argv) < 2:
        print("Usage: python3.11 analyze_rn1_deep.py <file1> [file2] ...")
        print("Supports: .csv, .json, .tsv")
        print("\nExample:")
        print("  python3.11 analyze_rn1_deep.py enriched_trades.csv")
        print("  python3.11 analyze_rn1_deep.py enriched_trades.csv full_history.csv")
        sys.exit(1)
    
    all_trades = []
    
    for filepath in sys.argv[1:]:
        if not os.path.exists(filepath):
            print(f"ERROR: File not found: {filepath}")
            continue
        
        fsize = os.path.getsize(filepath) / 1024 / 1024
        print(f"\n{'='*60}")
        print(f"File: {filepath} ({fsize:.1f} MB)")
        print(f"{'='*60}")
        
        ext = os.path.splitext(filepath)[1].lower()
        if ext in ('.json',):
            rows = load_json(filepath)
        else:
            rows = load_csv(filepath)
        
        if not rows:
            print("  No data found!")
            continue
        
        mapping = detect_columns(rows[0])
        trades = normalize_trades(rows, mapping)
        print(f"  Normalized {len(trades):,} trades")
        
        all_trades.extend(trades)
    
    if not all_trades:
        print("\nERROR: No trades loaded from any file!")
        sys.exit(1)
    
    # Sort all trades by epoch
    all_trades.sort(key=lambda t: t['epoch'])
    print(f"\n{'='*60}")
    print(f"TOTAL TRADES ACROSS ALL FILES: {len(all_trades):,}")
    print(f"{'='*60}")
    
    results = {}
    
    # ── Full analysis ──
    print("\nAnalyzing full dataset...")
    results['full'] = analyze_phase(all_trades, "all_trades")
    
    # ── Phase breakdowns ──
    buys_only = [t for t in all_trades if t['action'] == 'BUY']
    
    # Phases by trade number (first N buys + associated sells/merges)
    phase_cuts = [
        (0, 1000, "first_1K_buys"),
        (1000, 2000, "buys_1K-2K"),
        (2000, 5000, "buys_2K-5K"),
        (5000, 10000, "buys_5K-10K"),
        (10000, 50000, "buys_10K-50K"),
        (50000, 999999999, "buys_50K+"),
    ]
    
    for start, end, label in phase_cuts:
        if len(buys_only) > start:
            phase_buys = buys_only[start:min(end, len(buys_only))]
            if not phase_buys:
                continue
            
            # Get all trades in the time window of these buys
            t0 = phase_buys[0]['epoch']
            t1 = phase_buys[-1]['epoch']
            phase_all = [t for t in all_trades if t0 <= t['epoch'] <= t1]
            
            print(f"\nAnalyzing phase: {label} ({len(phase_buys):,} buys)...")
            results[label] = analyze_phase(phase_all, label)
    
    # ── Evolution tracking ──
    print("\nTracking evolution at milestones...")
    results['evolution'] = track_evolution(all_trades)
    
    # ── Bankroll trajectory ──
    print("\nEstimating bankroll trajectory...")
    results['bankroll_trajectory'] = estimate_bankroll_trajectory(all_trades)
    
    # ── First YES trade detection ──
    print("\nFinding first YES trade...")
    yes_buys = [t for t in buys_only if t['side'] == 'YES']
    if yes_buys:
        first_yes = yes_buys[0]
        yes_idx = buys_only.index(first_yes) + 1  # 1-indexed
        results['first_yes'] = {
            'buy_number': yes_idx,
            'date': first_yes.get('date', '?'),
            'price': first_yes['price'],
            'cost': first_yes['cost'],
            'title': first_yes['title'],
            'total_buys_at_that_point': yes_idx,
        }
    else:
        results['first_yes'] = {'buy_number': 'NEVER', 'note': '100% NO through entire history'}
    
    # ── Top 20 markets by volume ──
    print("\nFinding top markets...")
    mkt_vol = defaultdict(lambda: {'cost': 0, 'buys': 0, 'titles': set()})
    for t in buys_only:
        key = t['market_id'] if t['market_id'] else t['title']
        mkt_vol[key]['cost'] += t['cost']
        mkt_vol[key]['buys'] += 1
        mkt_vol[key]['titles'].add(t['title'][:60])
    
    top20 = sorted(mkt_vol.items(), key=lambda x: -x[1]['cost'])[:20]
    results['top_20_markets'] = [
        {
            'title': list(v['titles'])[0] if v['titles'] else '?',
            'buys': v['buys'],
            'total_cost': round(v['cost'], 2),
        }
        for _, v in top20
    ]
    
    # ── Sell analysis ──
    sells = [t for t in all_trades if t['action'] == 'SELL']
    if sells:
        print(f"\nAnalyzing {len(sells):,} sells...")
        sell_prices = [t['price'] for t in sells if t['price'] > 0]
        sell_costs = [t['cost'] for t in sells if t['cost'] > 0]
        results['sell_deep'] = {
            'total': len(sells),
            'rate_pct': round(len(sells)/len(all_trades)*100, 2),
            'no_sells': len([t for t in sells if t['side'] == 'NO']),
            'yes_sells': len([t for t in sells if t['side'] == 'YES']),
            'avg_price': round(sum(sell_prices)/len(sell_prices), 4) if sell_prices else 0,
            'med_price': round(sorted(sell_prices)[len(sell_prices)//2], 4) if sell_prices else 0,
            'price_pctiles': {
                'p10': round(sorted(sell_prices)[int(len(sell_prices)*0.1)], 4),
                'p50': round(sorted(sell_prices)[int(len(sell_prices)*0.5)], 4),
                'p90': round(sorted(sell_prices)[int(len(sell_prices)*0.9)], 4),
            } if len(sell_prices) > 10 else {},
            'total_revenue': round(sum(t['revenue'] for t in sells), 2),
        }
    
    # ── Save output ──
    output_file = 'rn1_deep_analysis.json'
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    fsize_out = os.path.getsize(output_file) / 1024
    
    print(f"\n{'='*60}")
    print(f"ANALYSIS COMPLETE")
    print(f"{'='*60}")
    print(f"Output: {output_file} ({fsize_out:.1f} KB)")
    print(f"\nKey findings:")
    
    full = results.get('full', {})
    sides = full.get('sides', {})
    vol = full.get('volume', {})
    sp = full.get('size_pctiles', {})
    
    print(f"  Total trades: {full.get('total', '?'):,}")
    print(f"  Buys: {full.get('counts', {}).get('buys', '?'):,}  Sells: {full.get('counts', {}).get('sells', '?'):,}  Merges: {full.get('counts', {}).get('merges', '?'):,}")
    print(f"  NO buys: {sides.get('buy_no', '?')} ({sides.get('buy_no_pct', '?')}%)  YES buys: {sides.get('buy_yes', '?')} ({sides.get('buy_yes_pct', '?')}%)")
    print(f"  Total volume: ${vol.get('total_buy_cost', 0):,.2f}")
    print(f"  Median trade: ${sp.get('p50', '?')}  p99: ${sp.get('p99', '?')}  Max: ${sp.get('max', '?')}")
    print(f"  First YES buy: trade #{results.get('first_yes', {}).get('buy_number', '?')}")
    
    if 'evolution' in results:
        print(f"\n  Evolution milestones:")
        for key, ev in results['evolution'].items():
            print(f"    {key}: NO={ev['no_pct']}% YES={ev['yes_pct']}% "
                  f"med=${ev['med_cost']} p99=${ev['p99_cost']} "
                  f"single={ev['single_buy_pct']}% sells={ev['sells_in_period']}")
    
    print(f"\n{'='*60}")
    print(f"PASTE the contents of '{output_file}' into Claude")
    print(f"for full bot calibration against RN1's real data.")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
