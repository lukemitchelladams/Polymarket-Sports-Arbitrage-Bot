"""
PolyCopy — Strategy Analyzer
==============================
Fetches a large sample of trades from the target wallet and tries to
reverse-engineer what their bot is programmed to do.

Analyzes:
  - Trade sizing patterns (fixed? proportional? Kelly? tiered?)
  - Market type preferences (sports, crypto, politics?)
  - Side bias (YES vs NO)
  - Timing patterns (how close to market open/close?)
  - Hedging behavior (does he bet both sides?)
  - Price entry points (does he buy at specific probability ranges?)
  - Win rate by market category
  - Scaling logic (how does bet size relate to market price/liquidity?)

Run: python analyze_strategy.py
Output: strategy_report.txt + prints summary to console
"""

import requests
import json
import time
from datetime import datetime, timezone
from collections import defaultdict
import statistics

TARGET = "0x2005d16a84ceefa912d4e380cd32e7ff827875ea"
DATA   = "https://data-api.polymarket.com"
GAMMA  = "https://gamma-api.polymarket.com"
HDR    = {"Accept": "application/json", "User-Agent": "PolyCopyAnalyzer/1.0"}

def get(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = requests.get(url, headers=HDR, params=params, timeout=15)
            if r.status_code == 429:
                print(f"  Rate limited, waiting {5*(i+1)}s...")
                time.sleep(5 * (i+1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if i < retries - 1:
                time.sleep(2)
            else:
                print(f"  API error: {e}")
    return None

def fetch_all_trades(wallet, max_trades=500):
    """Fetch up to max_trades trades in paginated batches."""
    all_trades = []
    offset = 0
    batch  = 100

    print(f"Fetching trades for {wallet}...")
    while len(all_trades) < max_trades:
        data = get(f"{DATA}/trades", {
            "user":   wallet,
            "limit":  batch,
            "offset": offset,
        })
        if not data or not isinstance(data, list) or not data:
            break
        all_trades.extend(data)
        print(f"  Fetched {len(all_trades)} trades so far...")
        if len(data) < batch:
            break
        offset += len(data)
        time.sleep(0.5)  # be polite to the API

    print(f"  Total trades fetched: {len(all_trades)}\n")
    return all_trades

def fetch_all_positions(wallet):
    """Fetch all positions (open + closed) for win rate analysis."""
    print("Fetching position history...")
    data = get(f"{DATA}/positions", {
        "user":          wallet,
        "sizeThreshold": 0.01,
        "limit":         500,
        "sortBy":        "CASHPNL",
        "sortDirection": "DESC",
    })
    if not data:
        return []
    positions = data if isinstance(data, list) else []
    print(f"  Total positions fetched: {len(positions)}\n")
    return positions

def categorize_market(title):
    """Guess the category from market title keywords."""
    t = title.lower()
    if any(k in t for k in ["nba","nfl","nhl","mlb","soccer","football","basketball",
                              "tennis","golf","ufc","mma","win","vs","match","game",
                              "spread","over/under","score","championship","league",
                              "world cup","premier","lakers","bulls","knicks","heat",
                              "warriors","celtics","nets","suns","mavs","spurs"]):
        return "sports"
    if any(k in t for k in ["btc","eth","bitcoin","ethereum","crypto","sol","xrp",
                              "doge","bnb","price","usd","market cap"]):
        return "crypto"
    if any(k in t for k in ["trump","biden","election","president","congress","senate",
                              "democrat","republican","vote","poll","governor","fed",
                              "fed rate","inflation","gdp","recession"]):
        return "politics/econ"
    if any(k in t for k in ["oscars","grammy","super bowl","award","celebrity",
                              "movie","show","tv","music"]):
        return "culture"
    return "other"

def analyze(trades, positions):
    print("=" * 60)
    print("  STRATEGY ANALYSIS REPORT")
    print(f"  Target: {TARGET}")
    print(f"  Trades analyzed: {len(trades)}")
    print(f"  Positions analyzed: {len(positions)}")
    print("=" * 60)
    lines = []  # for file output

    def log(msg=""):
        print(msg)
        lines.append(msg)

    log("=" * 60)
    log("  STRATEGY ANALYSIS REPORT")
    log(f"  Target: {TARGET}")
    log(f"  Sample: {len(trades)} trades, {len(positions)} positions")
    log("=" * 60)

    # ── Filter to BUY trades only for sizing analysis ─────────────────────────
    buys  = [t for t in trades if (t.get("side") or "").upper() == "BUY"]
    sells = [t for t in trades if (t.get("side") or "").upper() == "SELL"]
    log(f"\n{'─'*60}")
    log(f"  TRADE DIRECTION SPLIT")
    log(f"{'─'*60}")
    log(f"  BUY  trades : {len(buys)}  ({round(len(buys)/max(len(trades),1)*100)}%)")
    log(f"  SELL trades : {len(sells)}  ({round(len(sells)/max(len(trades),1)*100)}%)")

    # ── Sizing analysis ───────────────────────────────────────────────────────
    usdc_sizes = [float(t.get("usdcSize") or 0) for t in buys if float(t.get("usdcSize") or 0) > 0]
    if usdc_sizes:
        usdc_sizes.sort()
        log(f"\n{'─'*60}")
        log(f"  TRADE SIZE ANALYSIS (USDC, BUY trades only)")
        log(f"{'─'*60}")
        log(f"  Min      : ${min(usdc_sizes):.2f}")
        log(f"  Max      : ${max(usdc_sizes):.2f}")
        log(f"  Mean     : ${statistics.mean(usdc_sizes):.2f}")
        log(f"  Median   : ${statistics.median(usdc_sizes):.2f}")
        if len(usdc_sizes) > 1:
            log(f"  Std Dev  : ${statistics.stdev(usdc_sizes):.2f}")

        # Size buckets
        buckets = defaultdict(int)
        for s in usdc_sizes:
            if   s <   10: buckets["<$10"] += 1
            elif s <   50: buckets["$10-50"] += 1
            elif s <  100: buckets["$50-100"] += 1
            elif s <  500: buckets["$100-500"] += 1
            elif s < 1000: buckets["$500-1K"] += 1
            elif s < 5000: buckets["$1K-5K"] += 1
            elif s <10000: buckets["$5K-10K"] += 1
            else:          buckets["$10K+"] += 1

        log(f"\n  Size distribution:")
        for bucket, count in sorted(buckets.items(), key=lambda x: x[0]):
            pct = round(count / len(usdc_sizes) * 100)
            bar = "█" * (pct // 3)
            log(f"    {bucket:12s} {bar} {count} trades ({pct}%)")

        # Check for fixed/round amounts
        round_amounts = [s for s in usdc_sizes if s == round(s) and s > 0]
        round_pct = round(len(round_amounts) / len(usdc_sizes) * 100)
        log(f"\n  Round dollar amounts : {len(round_amounts)}/{len(usdc_sizes)} ({round_pct}%)")

        # Check common exact sizes
        from collections import Counter
        size_counts = Counter(round(s, 0) for s in usdc_sizes)
        top_sizes   = size_counts.most_common(10)
        log(f"\n  Most common trade sizes:")
        for size, count in top_sizes:
            log(f"    ${size:.0f}  →  {count} trades")

    # ── Price entry analysis ──────────────────────────────────────────────────
    prices = [float(t.get("price") or 0) for t in buys if 0 < float(t.get("price") or 0) < 1]
    if prices:
        log(f"\n{'─'*60}")
        log(f"  PRICE ENTRY ANALYSIS (what probability does he buy at?)")
        log(f"{'─'*60}")
        log(f"  Min price  : {min(prices):.3f}  ({round(min(prices)*100)}% implied prob)")
        log(f"  Max price  : {max(prices):.3f}  ({round(max(prices)*100)}% implied prob)")
        log(f"  Mean price : {statistics.mean(prices):.3f}  ({round(statistics.mean(prices)*100)}%)")
        log(f"  Median     : {statistics.median(prices):.3f}  ({round(statistics.median(prices)*100)}%)")

        # Price buckets (what probability range does he prefer?)
        pbuckets = defaultdict(int)
        for p in prices:
            if   p < 0.10: pbuckets["<10% (longshot)"] += 1
            elif p < 0.25: pbuckets["10-25%"] += 1
            elif p < 0.40: pbuckets["25-40%"] += 1
            elif p < 0.60: pbuckets["40-60% (coinflip)"] += 1
            elif p < 0.75: pbuckets["60-75%"] += 1
            elif p < 0.90: pbuckets["75-90% (favorite)"] += 1
            else:          pbuckets[">90% (heavy fav)"] += 1

        log(f"\n  Entry price distribution:")
        for bucket, count in pbuckets.items():
            pct = round(count / len(prices) * 100)
            bar = "█" * (pct // 3)
            log(f"    {bucket:22s} {bar} {count} ({pct}%)")

        # Does size correlate with price? (bigger bets on favorites?)
        if usdc_sizes and len(prices) == len(usdc_sizes):
            fav_sizes  = [s for s, p in zip(usdc_sizes, prices) if p > 0.70]
            dog_sizes  = [s for s, p in zip(usdc_sizes, prices) if p < 0.30]
            if fav_sizes and dog_sizes:
                log(f"\n  Avg bet on favorites (>70%): ${statistics.mean(fav_sizes):.2f}")
                log(f"  Avg bet on longshots (<30%): ${statistics.mean(dog_sizes):.2f}")
                ratio = statistics.mean(fav_sizes) / statistics.mean(dog_sizes)
                if ratio > 1.5:
                    log(f"  → Bets BIGGER on favorites ({ratio:.1f}x more)")
                elif ratio < 0.67:
                    log(f"  → Bets BIGGER on longshots ({1/ratio:.1f}x more)")
                else:
                    log(f"  → Bet size is roughly INDEPENDENT of price")

    # ── Market category analysis ──────────────────────────────────────────────
    log(f"\n{'─'*60}")
    log(f"  MARKET CATEGORY ANALYSIS")
    log(f"{'─'*60}")
    cat_counts  = defaultdict(int)
    cat_volume  = defaultdict(float)
    yes_counts  = defaultdict(int)
    no_counts   = defaultdict(int)

    for t in buys:
        title   = t.get("title") or ""
        outcome = (t.get("outcome") or "").upper()
        size    = float(t.get("usdcSize") or 0)
        cat     = categorize_market(title)
        cat_counts[cat]  += 1
        cat_volume[cat]  += size
        if "YES" in outcome: yes_counts[cat] += 1
        else:                no_counts[cat]  += 1

    for cat in sorted(cat_counts, key=lambda x: cat_counts[x], reverse=True):
        total = cat_counts[cat]
        vol   = cat_volume[cat]
        yes   = yes_counts[cat]
        no    = no_counts[cat]
        pct   = round(total / max(len(buys), 1) * 100)
        log(f"\n  {cat.upper()} ({pct}% of trades, ${vol:,.0f} volume)")
        log(f"    Trades : {total}")
        log(f"    YES    : {yes} ({round(yes/max(total,1)*100)}%)")
        log(f"    NO     : {no} ({round(no/max(total,1)*100)}%)")

    # ── YES vs NO overall ─────────────────────────────────────────────────────
    yes_total = sum(1 for t in buys if "YES" in (t.get("outcome") or "").upper())
    no_total  = sum(1 for t in buys if "NO"  in (t.get("outcome") or "").upper())
    log(f"\n{'─'*60}")
    log(f"  YES vs NO BIAS (overall)")
    log(f"{'─'*60}")
    log(f"  YES bets : {yes_total} ({round(yes_total/max(len(buys),1)*100)}%)")
    log(f"  NO  bets : {no_total} ({round(no_total/max(len(buys),1)*100)}%)")

    # ── Hedging analysis ──────────────────────────────────────────────────────
    # Does he ever bet both YES and NO on the same market?
    log(f"\n{'─'*60}")
    log(f"  HEDGING / ARBITRAGE BEHAVIOR")
    log(f"{'─'*60}")
    market_sides = defaultdict(set)
    for t in buys:
        cid     = t.get("conditionId") or ""
        outcome = (t.get("outcome") or "").upper()
        if cid: market_sides[cid].add(outcome)

    both_sides = {cid: sides for cid, sides in market_sides.items() if len(sides) > 1}
    log(f"  Markets bet on BOTH sides : {len(both_sides)} / {len(market_sides)}")
    if len(both_sides) > 0:
        pct = round(len(both_sides) / max(len(market_sides), 1) * 100)
        log(f"  ({pct}% of markets — {'SIGNIFICANT hedging/arb behavior' if pct > 10 else 'occasional hedging'})")

    # ── Timing analysis ───────────────────────────────────────────────────────
    log(f"\n{'─'*60}")
    log(f"  TIMING ANALYSIS")
    log(f"{'─'*60}")
    timestamps = [int(t.get("timestamp") or 0) for t in trades if t.get("timestamp")]
    if len(timestamps) > 1:
        timestamps.sort(reverse=True)
        # Time gaps between trades
        gaps = [timestamps[i] - timestamps[i+1] for i in range(min(len(timestamps)-1, 200))]
        gaps = [g for g in gaps if 0 < g < 3600]  # filter outliers
        if gaps:
            log(f"  Median gap between trades : {statistics.median(gaps):.1f}s")
            log(f"  Mean gap between trades   : {statistics.mean(gaps):.1f}s")
            log(f"  Min gap                   : {min(gaps):.1f}s")
            rapid = sum(1 for g in gaps if g < 5)
            log(f"  Trades within 5s of prev  : {rapid}/{len(gaps)} ({round(rapid/len(gaps)*100)}%)")
            if statistics.median(gaps) < 10:
                log(f"\n  → HIGH FREQUENCY BOT — median {statistics.median(gaps):.1f}s between trades")
                log(f"    Likely automated, not manual")

        # Hour of day distribution
        hour_counts = defaultdict(int)
        for ts in timestamps:
            hour = datetime.fromtimestamp(ts, tz=timezone.utc).hour
            hour_counts[hour] += 1
        log(f"\n  Most active hours (UTC):")
        top_hours = sorted(hour_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        for hour, count in top_hours:
            log(f"    {hour:02d}:00  →  {count} trades")

    # ── Sizing vs price correlation ───────────────────────────────────────────
    # Check if size = price * constant (i.e., buying fixed number of shares)
    log(f"\n{'─'*60}")
    log(f"  SIZING MODEL DETECTION")
    log(f"{'─'*60}")

    pairs = [(float(t.get("usdcSize") or 0), float(t.get("price") or 0))
             for t in buys
             if float(t.get("usdcSize") or 0) > 0 and 0 < float(t.get("price") or 0) < 1]

    if pairs:
        # Implied shares = usdcSize / price
        implied_shares = [s / p for s, p in pairs]
        implied_shares.sort()

        log(f"  Implied shares per trade (usdcSize / price):")
        log(f"    Min    : {min(implied_shares):.0f}")
        log(f"    Max    : {max(implied_shares):.0f}")
        log(f"    Median : {statistics.median(implied_shares):.0f}")
        log(f"    Mean   : {statistics.mean(implied_shares):.0f}")
        if len(implied_shares) > 1:
            cv = statistics.stdev(implied_shares) / statistics.mean(implied_shares)
            log(f"    CV     : {cv:.2f}  (lower = more consistent share count)")
            if cv < 0.3:
                log(f"\n  → LIKELY FIXED SHARE COUNT strategy")
                log(f"    He buys ~{statistics.median(implied_shares):.0f} shares per trade")
                log(f"    Cost varies with price (cheaper when probability is low)")
            elif cv > 1.5:
                log(f"\n  → VARIABLE sizing — not a fixed share strategy")

        # Check size/price ratio (implied payout target)
        ratios = [s / p for s, p in pairs if p > 0]
        if ratios:
            log(f"\n  Implied payout per trade (usdcSize/price = 'to win'):")
            log(f"    Median : ${statistics.median(ratios):.2f}")
            log(f"    Mean   : ${statistics.mean(ratios):.2f}")

    # ── Win rate from positions ───────────────────────────────────────────────
    if positions:
        log(f"\n{'─'*60}")
        log(f"  WIN RATE FROM RESOLVED POSITIONS")
        log(f"{'─'*60}")
        wins = losses = 0
        cat_wins = defaultdict(int)
        cat_losses = defaultdict(int)

        for p in positions:
            cur_price  = float(p.get("curPrice") or 0)
            outcome    = (p.get("outcome") or "YES").upper()
            redeemable = p.get("redeemable", False)
            title      = p.get("title") or ""
            cat        = categorize_market(title)

            settled_yes = cur_price > 0.96
            settled_no  = cur_price < 0.04
            is_resolved = redeemable or settled_yes or settled_no
            if not is_resolved:
                continue

            if redeemable or (settled_yes and "YES" in outcome) or (settled_no and "NO" in outcome):
                wins += 1
                cat_wins[cat] += 1
            else:
                losses += 1
                cat_losses[cat] += 1

        total = wins + losses
        if total > 0:
            log(f"  Overall win rate : {round(wins/total*100,1)}%  ({wins}W / {losses}L)")
            log(f"\n  Win rate by category:")
            all_cats = set(list(cat_wins.keys()) + list(cat_losses.keys()))
            for cat in sorted(all_cats):
                w = cat_wins[cat]; l = cat_losses[cat]; t = w + l
                if t > 0:
                    log(f"    {cat:20s} {round(w/t*100)}%  ({w}W/{l}L)")

    # ── Strategy summary ──────────────────────────────────────────────────────
    log(f"\n{'─'*60}")
    log(f"  STRATEGY SUMMARY & COPY RECOMMENDATIONS")
    log(f"{'─'*60}")

    if usdc_sizes:
        med_size = statistics.median(usdc_sizes)
        log(f"\n  His median bet size : ${med_size:.2f}")
        for pct in [1, 2, 5, 10, 25]:
            log(f"  Your bet at {pct:3d}%   : ${med_size * pct / 100:.2f}")

    if prices:
        med_price = statistics.median(prices)
        log(f"\n  He tends to buy at : {round(med_price*100)}¢ (implied {round(med_price*100)}% probability)")
        if med_price > 0.70:
            log(f"  → FAVORITE BETTOR — buys high-probability outcomes")
        elif med_price < 0.35:
            log(f"  → LONGSHOT HUNTER — buys low-probability outcomes for big payouts")
        else:
            log(f"  → MID-RANGE — buys near 50/50 markets")

    if timestamps and len(timestamps) > 1:
        gaps = [timestamps[i] - timestamps[i+1] for i in range(min(len(timestamps)-1,100))]
        gaps = [g for g in gaps if 0 < g < 3600]
        if gaps and statistics.median(gaps) < 30:
            log(f"\n  ⚡ HIGH FREQUENCY: trades every ~{statistics.median(gaps):.0f}s on average")
            log(f"     Your copy bot should poll every 2-3 seconds")
            log(f"     You WILL miss some trades — that's unavoidable with polling")
            log(f"     Consider filtering to only copy trades over $X to avoid noise")

    log(f"\n{'─'*60}")
    log(f"  RAW DATA SAMPLE (last 10 trades)")
    log(f"{'─'*60}")
    for t in trades[:10]:
        ts    = datetime.fromtimestamp(int(t.get("timestamp") or 0), tz=timezone.utc).strftime("%H:%M:%S")
        side  = (t.get("side") or "?").upper()
        out   = t.get("outcome") or "?"
        price = float(t.get("price") or 0)
        size  = float(t.get("usdcSize") or 0)
        title = (t.get("title") or "?")[:45]
        log(f"  {ts}  {side:4s}  {out:3s}  ${size:8.2f}  @{price:.3f}  {title}")

    log(f"\n{'='*60}")
    log(f"  Analysis complete. Full report saved to strategy_report.txt")
    log(f"{'='*60}")

    # Save to file
    with open("strategy_report.txt", "w") as f:
        f.write("\n".join(lines))
    print(f"\nReport saved to strategy_report.txt")

    return lines

if __name__ == "__main__":
    print(f"PolyCopy Strategy Analyzer")
    print(f"Target: {TARGET}\n")

    trades    = fetch_all_trades(TARGET, max_trades=500)
    positions = fetch_all_positions(TARGET)

    if not trades:
        print("No trades found. Check the wallet address and your internet connection.")
    else:
        analyze(trades, positions)
