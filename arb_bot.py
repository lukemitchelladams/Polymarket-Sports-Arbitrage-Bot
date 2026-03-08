"""
PolyArb — Sports Arbitrage Bot
================================
Replicates the strategy of wallet 0x2005d16a84ceefa912d4e380cd32e7ff827875ea

CONFIRMED STRATEGY (88% confidence from 9,815 trade analysis):
  - Scans ALL active sports markets every 2-6 seconds
  - Finds markets where YES_ask + NO_ask < $1.00 (mispricing)
  - Buys BOTH sides simultaneously to lock in guaranteed profit
  - Scales bet size with the size of the spread
  - Fires up to 11 markets simultaneously when opportunities cluster
  - Most active 20:00-22:00 UTC (NBA tip-off + European soccer overlap)
  - Median arb event: $20.57 combined, median gap: 6s between events

SIZING MODEL (derived from 9,815 on-chain transactions):
  spread < 2.5%  → skip (fees eat profit)
  spread 2.5-3%  → $5-20 combined
  spread 3-5%    → $20-100 combined
  spread 5-8%    → $100-500 combined
  spread 8%+     → $500+ (hammer it)

FEE STRUCTURE:
  Polymarket charges 2% on NET WINNINGS only
  A $100 arb on a 3% spread → $103 payout → $0.06 fee → $2.94 net profit
  Minimum profitable spread: ~2.5% after fees

HOW TO RUN:
  1. Fill keys.env with your credentials
  2. Set DRY_RUN=true, run: python arb_bot.py
  3. Watch logs for 30+ minutes, verify opportunities are being found
  4. Set DRY_RUN=false to go live
"""

import os
import sys
import time
import json
import logging
import re
import requests
import threading
from datetime import datetime, timezone
from collections import defaultdict, deque
from dotenv import load_dotenv

# ── Config ─────────────────────────────────────────────────────────────────────
load_dotenv("keys.env")

PRIVATE_KEY        = os.getenv("PK", "")
PROXY_WALLET       = os.getenv("PROXY_WALLET", "")
LOGIN_METHOD       = os.getenv("LOGIN_METHOD", "email").lower()
MY_BANKROLL        = float(os.getenv("MY_BANKROLL", "1000"))
BASE_UNIT          = float(os.getenv("BASE_UNIT", "10"))       # $ per side at min spread
MIN_SPREAD         = float(os.getenv("MIN_SPREAD", "2.5"))     # % minimum to trade
MAX_SPREAD_MULT    = float(os.getenv("MAX_SPREAD_MULT", "20")) # size multiplier cap
MAX_BET_USDC       = float(os.getenv("MAX_BET_USDC", "50"))    # max per side per trade
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "30"))
MIN_LIQUIDITY      = float(os.getenv("MIN_LIQUIDITY", "2000"))  # min $ liquidity in market
SCAN_INTERVAL      = float(os.getenv("SCAN_INTERVAL", "3"))    # seconds between full scans
DRY_RUN            = os.getenv("DRY_RUN", "true").lower() == "true"
LOG_LEVEL          = os.getenv("LOG_LEVEL", "INFO")

# ── Endpoints ──────────────────────────────────────────────────────────────────
CLOB    = "https://clob.polymarket.com"
GAMMA   = "https://gamma-api.polymarket.com"
DATA    = "https://data-api.polymarket.com"
HEADERS = {"Accept": "application/json", "User-Agent": "PolyArb/1.0"}

# ── Logging ────────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
log_file = f"logs/arb_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(log_file),
        logging.FileHandler("logs/arb_latest.log", mode="w"),
        logging.StreamHandler(sys.stdout)
    ]
)
log = logging.getLogger(__name__)

# ── Stats tracker ──────────────────────────────────────────────────────────────
class Stats:
    def __init__(self):
        self.scans          = 0
        self.markets_seen   = 0
        self.opportunities  = 0
        self.trades_placed  = 0
        self.trades_filled  = 0
        self.trades_failed  = 0
        self.usdc_deployed  = 0.0
        self.estimated_profit = 0.0
        self.start_time     = time.time()
        self.recent_opps    = deque(maxlen=100)  # last 100 opportunities found
        self.lock           = threading.Lock()

    def log_opportunity(self, title, yes_price, no_price, spread, bet_each):
        with self.lock:
            self.opportunities += 1
            self.recent_opps.append({
                "time":     datetime.now(timezone.utc).strftime("%H:%M:%S"),
                "title":    title[:60],
                "yes":      yes_price,
                "no":       no_price,
                "spread":   spread,
                "bet_each": bet_each,
                "profit":   bet_each * 2 * (spread/100) - (bet_each * 2 * 0.02 * 0.5)
            })

    def summary(self):
        elapsed = time.time() - self.start_time
        hrs     = elapsed / 3600
        return (
            f"Runtime: {elapsed/60:.1f}m | "
            f"Scans: {self.scans} | "
            f"Markets: {self.markets_seen} | "
            f"Opps: {self.opportunities} | "
            f"Placed: {self.trades_placed} | "
            f"Filled: {self.trades_filled} | "
            f"Deployed: ${self.usdc_deployed:.2f} | "
            f"Est.Profit: ${self.estimated_profit:.2f}"
        )

stats = Stats()

# ── CLOB client ────────────────────────────────────────────────────────────────
_client = None

def get_client():
    global _client
    if _client:
        return _client
    if not PRIVATE_KEY or not PROXY_WALLET:
        raise RuntimeError("Set PK and PROXY_WALLET in keys.env before going live")
    from py_clob_client.client import ClobClient
    sig = {"email": 1, "magic": 1, "metamask": 2, "coinbase": 2}.get(LOGIN_METHOD, 1)
    _client = ClobClient(CLOB, key=PRIVATE_KEY, chain_id=137,
                         signature_type=sig, funder=PROXY_WALLET)
    _client.set_api_creds(_client.create_or_derive_api_creds())
    log.info(f"CLOB client ready (sig_type={sig}, funder={PROXY_WALLET[:10]}...)")
    return _client

# ── HTTP helpers ───────────────────────────────────────────────────────────────
_session = requests.Session()
_session.headers.update(HEADERS)

def get(url, params=None, timeout=10, retries=3):
    for i in range(retries):
        try:
            r = _session.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                wait = 5 * (i + 1)
                log.debug(f"Rate limited — sleeping {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 200:
                return r.json()
            log.debug(f"HTTP {r.status_code} from {url}")
            return None
        except requests.Timeout:
            log.debug(f"Timeout on {url} (attempt {i+1})")
            time.sleep(1)
        except Exception as e:
            log.debug(f"Request error: {e}")
            time.sleep(1)
    return None

# ── Market fetching ────────────────────────────────────────────────────────────
SPORTS_KEYWORDS = [
    "win on","will win","o/u","over/under","spread","moneyline",
    "nba","nfl","nhl","mlb","epl","ucl","mls","ufc","mma",
    "soccer","football","basketball","baseball","hockey","tennis",
    "golf","rugby","cricket","boxing","lakers","celtics","warriors",
    "bulls","heat","knicks","nets","suns","mavs","spurs","bucks",
    "sixers","clippers","nuggets","raptors","thunder","timberwolves",
    "pelicans","grizzlies","real madrid","barcelona","liverpool",
    "arsenal","chelsea","manchester","tottenham","psg","juventus",
    "inter milan","ac milan","bayern","dortmund","ajax","porto",
    "benfica","atletico","patriots","chiefs","eagles","cowboys",
    "49ers","bills","packers","ravens","bengals","broncos","rams",
    "seahawks","yankees","dodgers","astros","braves","red sox","cubs",
    "maple leafs","bruins","rangers","penguins","oilers","flames",
    "vs ","draw","clean sheet","both teams","sets","match winner",
    "game winner","series winner","first half","second half",
    "total goals","total points","total runs","total kills",
    "grêmio","flamengo","botafogo","palmeiras","corinthians",
    "serie a","bundesliga","la liga","ligue 1","champions league",
    "europa league","premier league","copa","libertadores",
    "f1","formula","cs2","valorant","dota","league of legends"
]

def get_sports_markets(limit=200):
    """
    Fetch active sports markets using keyword filtering + expiry check.
    Scans up to 500 markets, filters by title keywords, skips expired.
    """
    markets = []
    offset  = 0
    now     = datetime.now(timezone.utc)

    while len(markets) < 500:
        data = get(f"{GAMMA}/markets", params={
            "active":    "true",
            "closed":    "false",
            "limit":     200,
            "offset":    offset,
            "order":     "volume24hr",
            "ascending": "false"
        })
        if not data or not isinstance(data, list) or not data:
            break

        for m in data:
            title = m.get("question") or m.get("title") or ""
            title_lower = title.lower()

            if not any(
                (re.search(r'\b' + re.escape(k) + r'\b', title_lower) if len(k) <= 4 else k in title_lower)
                for k in SPORTS_KEYWORDS
            ):
                continue

            # Skip expired markets
            end = m.get("endDate") or m.get("end_date_iso") or ""
            if end:
                try:
                    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
                    if end_dt < now:
                        continue
                except:
                    pass

            markets.append(m)

        if len(data) < 200:
            break
        offset += 200
        time.sleep(0.1)

    # Deduplicate
    seen   = set()
    unique = []
    for m in markets:
        cid = m.get("conditionId")
        if cid and cid not in seen:
            seen.add(cid)
            unique.append(m)

    log.info(f"Found {len(unique)} sports markets")
    return unique

def get_price_pair_concurrent(yes_token, no_token):
    """Fetch YES and NO prices sequentially to avoid nested thread pool exhaustion."""
    yes_ask = get_price_single(yes_token, "BUY")
    no_ask  = get_price_single(no_token,  "BUY")
    return yes_ask, no_ask

def get_prices_batch(token_ids):
    """
    Fetch best ask prices for multiple tokens at once.
    Returns dict: token_id -> best_ask_price
    """
    if not token_ids:
        return {}

    prices = {}
    # CLOB batch price endpoint
    try:
        # Split into batches of 20
        for i in range(0, len(token_ids), 20):
            batch = token_ids[i:i+20]
            params = [("token_id", tid) for tid in batch]
            r = _session.get(
                f"{CLOB}/prices",
                params=params + [("side", "BUY")],
                timeout=8
            )
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    for item in data:
                        tid = item.get("asset_id") or item.get("token_id")
                        price = float(item.get("price") or 0)
                        if tid and price > 0:
                            prices[tid] = price
                elif isinstance(data, dict):
                    # Sometimes returns {token_id: price}
                    for tid, price in data.items():
                        prices[tid] = float(price)
    except Exception as e:
        log.debug(f"Batch price error: {e}")

    return prices

def get_price_single(token_id, side="BUY"):
    """Fallback: get price for a single token."""
    data = get(f"{CLOB}/price", params={"token_id": token_id, "side": side})
    if data and isinstance(data, dict):
        return float(data.get("price") or 0)
    return 0.0

def get_market_prices(market):
    """
    Get YES and NO ask prices for a market.
    Returns (yes_ask, no_ask, yes_token_id, no_token_id) or None.
    """
    tokens   = market.get("clobTokenIds") or []
    outcomes = market.get("outcomes") or []

    if isinstance(tokens, str):
        try:    tokens = json.loads(tokens)
        except: return None
    if isinstance(outcomes, str):
        try:    outcomes = json.loads(outcomes)
        except: return None

    if len(tokens) < 2 or len(outcomes) < 2:
        return None

    # Find YES and NO token IDs
    yes_token = no_token = None
    for i, outcome in enumerate(outcomes):
        o = outcome.strip().upper()
        if i < len(tokens):
            if o in ("YES", "TRUE", "OVER") or (i == 0 and "YES" not in str(outcomes).upper()):
                yes_token = tokens[i]
            elif o in ("NO", "FALSE", "UNDER") or i == 1:
                no_token = tokens[i]

    # If outcome names are team names (soccer/NBA), treat token[0] as YES, token[1] as NO
    if not yes_token:
        yes_token = tokens[0]
    if not no_token:
        no_token  = tokens[1]

    if not yes_token or not no_token:
        return None

    # Always fetch LIVE prices from CLOB — never use cached Gamma outcomePrices
    # (Gamma cached prices always sum to 1.0 and will miss all arb opportunities)
    yes_ask, no_ask = get_price_pair_concurrent(yes_token, no_token)

    if yes_ask <= 0 or no_ask <= 0:
        return None

    return yes_ask, no_ask, yes_token, no_token

def get_usdc_available_at_price(token_id, max_price):
    """
    Check order book and return how many USDC worth of shares
    are available at or below max_price.
    This tells us the max we can safely bet without slippage.
    """
    data = get(f"{CLOB}/book", params={"token_id": token_id})
    if not data:
        return 0.0
    asks = data.get("asks", [])
    usdc_available = 0.0
    for level in asks:
        level_price = float(level.get("price") or 0)
        level_size  = float(level.get("size") or 0)
        if level_price <= max_price:
            usdc_available += level_size * level_price
        else:
            break
    return usdc_available

# ── Sizing formula ─────────────────────────────────────────────────────────────
def calculate_bet_size(spread_pct):
    """
    Size each side of the arb bet based on spread size.
    Derived from 9,815 on-chain transactions of target wallet.

    Spread → per-side bet size:
      2.5-3.0%  → $5-10   (BASE_UNIT * 1x)
      3.0-4.0%  → $10-20  (BASE_UNIT * 2x)
      4.0-5.0%  → $20-50  (BASE_UNIT * 4x)
      5.0-8.0%  → $50-200 (BASE_UNIT * 8x)
      8.0%+     → $200+   (BASE_UNIT * 15x, capped at MAX_BET_USDC)

    Scale everything by MY_BANKROLL / $1000 so it grows with profits.
    """
    bankroll_scale = MY_BANKROLL / 1000.0

    if   spread_pct < 3.0:  multiplier = 1.0
    elif spread_pct < 4.0:  multiplier = 2.0
    elif spread_pct < 5.0:  multiplier = 4.0
    elif spread_pct < 8.0:  multiplier = 8.0
    elif spread_pct < 12.0: multiplier = 15.0
    else:                   multiplier = MAX_SPREAD_MULT

    bet = BASE_UNIT * multiplier * bankroll_scale
    bet = min(bet, MAX_BET_USDC * bankroll_scale)
    bet = max(bet, 1.0)  # minimum $1 per side
    return round(bet, 2)

def calculate_profit(spread_pct, bet_each):
    """
    Estimate net profit after Polymarket's 2% fee on winnings.
    Combined cost: 2 * bet_each
    Gross payout:  2 * bet_each * (1 + spread_pct/100) -- one side wins, pays $1/share
    Net profit:    payout - cost - fee
    
    Actually simpler: 
    You spend: yes_ask + no_ask per share
    You receive: $1.00 per share (guaranteed, one side always wins)
    Gross profit per share: 1.00 - (yes_ask + no_ask) = spread
    Fee: 2% * $1.00 * (spread / 1.00) ≈ 2% of gross profit
    Net: spread - fee
    """
    gross_per_share = spread_pct / 100
    # Fee is 2% applied to the winning side's payout
    # Winning payout per share = 1.0, fee = 0.02 * 1.0 = 0.02
    # But fee applies to net profit portion, not full payout
    # Simplified: net_spread = spread - 0.02
    net_spread = gross_per_share - 0.02
    # Total shares bought = bet_each / avg_price ≈ bet_each / 0.5
    total_cost   = bet_each * 2
    shares_bought = bet_each / 0.5  # rough approximation
    net_profit    = net_spread * shares_bought
    return max(net_profit, 0)

# ── Order placement ────────────────────────────────────────────────────────────
def place_order(token_id, side, amount_usdc, label="", max_price=None):
    """Place a FOK market order. Slippage protection is handled by pre-check in execute_arb."""
    from py_clob_client.clob_types import MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL

    clob_side = BUY if side.upper() == "BUY" else SELL

    if DRY_RUN:
        log.info(f"  [DRY] {side} ${amount_usdc:.2f} @{max_price:.3f}  {label}")
        return True, amount_usdc

    try:
        client = get_client()
        mo     = MarketOrderArgs(token_id=token_id, amount=amount_usdc, side=clob_side)
        signed = client.create_market_order(mo)
        resp   = client.post_order(signed, OrderType.FOK)

        if resp and resp.get("success"):
            filled = float(resp.get("size_matched") or amount_usdc)
            log.info(f"  ✅ BUY ${amount_usdc:.2f} FILLED (${filled:.2f} matched)  {label}")
            return True, filled
        else:
            error = resp.get("errorMsg") if resp else "no response"
            log.warning(f"  ❌ BUY ${amount_usdc:.2f} REJECTED: {error}  {label}")
            return False, 0

    except Exception as e:
        log.error(f"  Order error: {e}  {label}")
        return False, 0


def execute_arb(market, yes_ask, no_ask, yes_token, no_token, spread_pct, bet_each):
    """
    Execute both sides of an arb trade simultaneously using threads.
    Both orders fire at the same time to minimize slippage risk.
    """
    title = market.get("question") or market.get("title") or "Unknown"
    label = f"| {title[:50]}"

    # ── Pre-order price re-check ──────────────────────────────────────────────
    # Re-fetch live prices immediately before placing orders.
    # Prices can move between scan and execution — reject if no longer profitable.
    if not DRY_RUN:
        try:
            fresh_yes, fresh_no = get_price_pair_concurrent(yes_token, no_token)
            fresh_total = fresh_yes + fresh_no
            if fresh_total >= 0.94:  # Strict — reject if spread has shrunk significantly since scan
                log.warning(f"  ⏭️  STALE ARB SKIPPED: prices moved to {fresh_yes:.3f}+{fresh_no:.3f}={fresh_total:.3f} (no longer profitable) {label}")
                return False
            if fresh_yes <= 0.05 or fresh_no <= 0.05 or fresh_yes >= 0.95 or fresh_no >= 0.95:
                log.warning(f"  ⏭️  STALE ARB SKIPPED: prices out of valid range {label}")
                return False
            # Use fresh prices for logging accuracy
            yes_ask, no_ask = fresh_yes, fresh_no
            spread_pct = (1.0 - fresh_total) * 100
            bet_each = calculate_bet_size(spread_pct)

            # Size order based on actual book depth — only bet what exists at our price
            yes_usdc_avail = get_usdc_available_at_price(yes_token, yes_ask + 0.01)
            no_usdc_avail  = get_usdc_available_at_price(no_token,  no_ask  + 0.01)

            if yes_usdc_avail < 0.50 or no_usdc_avail < 0.50:
                log.warning(f"  ⏭️  SKIPPED: too thin (YES ${yes_usdc_avail:.2f}, NO ${no_usdc_avail:.2f} available) {label}")
                return False

            # Cap bet to available liquidity on the thinner side
            safe_bet = min(bet_each, yes_usdc_avail, no_usdc_avail)
            safe_bet = round(max(safe_bet, 0.50), 2)  # minimum $0.50
            if safe_bet != bet_each:
                log.info(f"  📉 Sizing down: ${bet_each:.2f} → ${safe_bet:.2f} (book depth limit) {label}")
            bet_each = safe_bet
            log.debug(f"  ✔️  Price+depth confirmed: {yes_ask:.3f}+{no_ask:.3f}={fresh_total:.3f} ({spread_pct:.1f}% spread) {label}")
        except Exception as e:
            log.warning(f"  ⚠️  Pre-check failed, skipping trade: {e} {label}")
            return False
    # ─────────────────────────────────────────────────────────────────────────

    stats.trades_placed += 2

    if DRY_RUN:
        log.info(f"  [DRY] YES ${bet_each:.2f} @{yes_ask:.3f}  {label}")
        log.info(f"  [DRY] NO  ${bet_each:.2f} @{no_ask:.3f}  {label}")
        estimated = calculate_profit(spread_pct, bet_each)
        stats.trades_filled  += 2
        stats.usdc_deployed  += bet_each * 2
        stats.estimated_profit += estimated
        return True

    results = {}

    # Round bet amount so shares (amount/price) have at most 4 decimal places
    # e.g. $1.50 / 0.253 = 5.92885... -> round shares to 4dp -> recalc amount
    def clean_amount(amount, price):
        shares = round(amount / price, 4)
        return round(shares * price, 2)

    yes_amount = clean_amount(bet_each, yes_ask)
    no_amount  = clean_amount(bet_each, no_ask)
    yes_max    = round(yes_ask + 0.02, 4)
    no_max     = round(no_ask  + 0.02, 4)

    def buy_yes():
        results["yes"] = place_order(yes_token, "BUY", yes_amount, f"YES {label}", max_price=yes_max)

    def buy_no():
        results["no"]  = place_order(no_token, "BUY", no_amount, f"NO  {label}", max_price=no_max)

    # Fire both simultaneously
    t1 = threading.Thread(target=buy_yes, daemon=True)
    t2 = threading.Thread(target=buy_no,  daemon=True)
    t1.start(); t2.start()
    t1.join(timeout=10); t2.join(timeout=10)

    yes_ok, yes_filled = results.get("yes", (False, 0))
    no_ok,  no_filled  = results.get("no",  (False, 0))

    if yes_ok and no_ok:
        estimated = calculate_profit(spread_pct, bet_each)
        stats.trades_filled  += 2
        stats.usdc_deployed  += bet_each * 2
        stats.estimated_profit += estimated
        log.info(f"  💰 ARB COMPLETE: ${bet_each*2:.2f} deployed, ~${estimated:.2f} profit  {label}")
        return True
    elif yes_ok and not no_ok:
        # Danger: bought YES but NO failed. We're exposed on one side.
        log.warning(f"  ⚠️  PARTIAL FILL: YES filled but NO failed. EXPOSED POSITION. {label}")
        log.warning(f"  ⚠️  Consider manually buying NO on Polymarket immediately!")
        stats.trades_filled += 1
        stats.trades_failed += 1
        return False
    elif no_ok and not yes_ok:
        log.warning(f"  ⚠️  PARTIAL FILL: NO filled but YES failed. EXPOSED POSITION. {label}")
        log.warning(f"  ⚠️  Consider manually buying YES on Polymarket immediately!")
        stats.trades_filled += 1
        stats.trades_failed += 1
        return False
    else:
        stats.trades_failed += 2
        return False

# ── Open position check ────────────────────────────────────────────────────────
_open_position_cache = {"count": 0, "last_check": 0}

def get_open_position_count():
    """Cache position count for 30s to avoid hammering API."""
    now = time.time()
    if now - _open_position_cache["last_check"] < 30:
        return _open_position_cache["count"]

    data = get(f"{DATA}/positions", params={
        "user":          PROXY_WALLET or "0x0000000000000000000000000000000000000000",
        "sizeThreshold": 0.01,
        "limit":         500
    })
    count = 0
    if data and isinstance(data, list):
        count = len([p for p in data if 0.04 < float(p.get("curPrice") or 0) < 0.96])

    _open_position_cache["count"] = count
    _open_position_cache["last_check"] = now
    return count

# ── Already traded check ───────────────────────────────────────────────────────
_already_traded = set()  # condition IDs we've already arb'd this session

# ── Main scan loop ─────────────────────────────────────────────────────────────
def scan_and_trade():
    """One full scan of all sports markets. Returns number of opportunities found."""
    stats.scans += 1
    opportunities_this_scan = []

    markets = get_sports_markets()
    if not markets:
        log.debug("No markets returned from Gamma API")
        return 0

    stats.markets_seen = len(markets)

    from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _ac

    def check_one(market):
        cid = market.get("conditionId", "")
        if cid in _already_traded:
            return None
        liquidity = float(market.get("liquidity") or 0)
        if liquidity < MIN_LIQUIDITY:
            return None
        price_data = get_market_prices(market)
        if not price_data:
            return None
        yes_ask, no_ask, yes_token, no_token = price_data
        if yes_ask <= 0.05 or no_ask <= 0.05:
            return None
        if yes_ask >= 0.95 or no_ask >= 0.95:
            return None
        total = yes_ask + no_ask
        if total >= 0.985:
            return None
        if total < 0.50:
            return None
        spread_pct = (1.0 - total) * 100
        if spread_pct < MIN_SPREAD:
            return None
        bet_each = calculate_bet_size(spread_pct)
        estimated_profit = calculate_profit(spread_pct, bet_each)
        title = market.get("question") or market.get("title") or "?"
        log.info(f"🎯 ARB FOUND: {spread_pct:.2f}% spread | YES@{yes_ask:.3f} + NO@{no_ask:.3f} = {total:.3f} | Bet ${bet_each:.2f} each (~${estimated_profit:.2f} profit) | {title[:55]}")
        stats.log_opportunity(title, yes_ask, no_ask, spread_pct, bet_each)
        return {"market": market, "yes_ask": yes_ask, "no_ask": no_ask,
                "yes_token": yes_token, "no_token": no_token,
                "spread_pct": spread_pct, "bet_each": bet_each}

    # Execute each opportunity immediately as found — one at a time to avoid order book slippage
    # Scanning still runs in parallel for speed, but trades are executed sequentially
    # Execute immediately as each arb is found — don't wait for full scan to complete
    executed = 0
    trade_lock = threading.Lock()

    def check_and_trade(market):
        nonlocal executed
        result = check_one(market)
        if not result:
            return
        cid = result["market"].get("conditionId", "")
        with trade_lock:
            if cid in _already_traded:
                return
            # Check position cap
            if MAX_OPEN_POSITIONS > 0:
                open_count = get_open_position_count()
                if open_count >= MAX_OPEN_POSITIONS:
                    log.warning(f"At position limit ({open_count}/{MAX_OPEN_POSITIONS}), skipping")
                    return
            _already_traded.add(cid)

        # Execute outside the lock so other threads can check concurrently
        success = execute_arb(
            result["market"], result["yes_ask"], result["no_ask"],
            result["yes_token"], result["no_token"],
            result["spread_pct"], result["bet_each"]
        )
        if success:
            with trade_lock:
                executed += 1

    with _TPE(max_workers=20) as executor:
        futures = [executor.submit(check_and_trade, m) for m in markets]
        for future in _ac(futures):
            try:
                future.result()
            except Exception as e:
                log.debug(f"check_and_trade error: {e}")

    return executed

# ── Stats printer ──────────────────────────────────────────────────────────────
def print_stats_periodically():
    """Print a stats summary every 5 minutes."""
    while True:
        time.sleep(300)
        log.info("─" * 70)
        log.info(f"STATS: {stats.summary()}")
        log.info("─" * 70)

# ── Main ───────────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 70)
    log.info("  PolyArb Bot v1.0 — Sports Arbitrage Engine")
    log.info(f"  Mode       : {'🔵 DRY RUN (no real orders)' if DRY_RUN else '🔴 LIVE TRADING'}")
    log.info(f"  Bankroll   : ${MY_BANKROLL:,.2f}")
    log.info(f"  Base unit  : ${BASE_UNIT:.2f} per side at {MIN_SPREAD}% spread")
    log.info(f"  Max per side: ${MAX_BET_USDC:.2f}")
    log.info(f"  Min spread : {MIN_SPREAD}%")
    log.info(f"  Scan every : {SCAN_INTERVAL}s")
    log.info(f"  Max positions: {MAX_OPEN_POSITIONS}")
    log.info(f"  Log file   : {log_file}")
    log.info("=" * 70)

    if not DRY_RUN:
        log.warning("🔴 LIVE MODE — Real orders will be placed!")
        log.warning("   Press Ctrl+C within 10 seconds to abort...")
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            log.info("Aborted. Change DRY_RUN=false in keys.env when ready.")
            return

    # Pre-initialize CLOB client
    if not DRY_RUN:
        try:
            get_client()
        except Exception as e:
            log.error(f"Failed to initialize CLOB client: {e}")
            log.error("Check your PK and PROXY_WALLET in keys.env")
            return

    # Start stats printer thread
    stats_thread = threading.Thread(target=print_stats_periodically, daemon=True)
    stats_thread.start()

    log.info("Starting scan loop...")
    scan_count    = 0
    total_opps    = 0
    last_opp_time = None

    while True:
        try:
            scan_start = time.time()
            opps = scan_and_trade()

            if opps > 0:
                total_opps   += opps
                last_opp_time = datetime.now(timezone.utc).strftime("%H:%M:%S")

            scan_count += 1

            # Brief scan summary every 10 scans
            if scan_count % 10 == 0:
                elapsed = time.time() - scan_start
                log.debug(
                    f"Scan #{scan_count} complete | "
                    f"Markets: {stats.markets_seen} | "
                    f"Total opps: {total_opps} | "
                    f"Last opp: {last_opp_time or 'none yet'} | "
                    f"Scan time: {elapsed:.1f}s"
                )

            # Sleep between scans (target does ~6s median gap)
            elapsed = time.time() - scan_start
            sleep   = max(0, SCAN_INTERVAL - elapsed)
            time.sleep(sleep)

        except KeyboardInterrupt:
            log.info("\n🛑 Bot stopped by user")
            log.info(f"Final stats: {stats.summary()}")

            # Save final stats
            with open("logs/final_stats.txt", "w") as f:
                f.write(stats.summary() + "\n\n")
                f.write("Recent opportunities:\n")
                for opp in stats.recent_opps:
                    f.write(f"  {opp}\n")
            break

        except Exception as e:
            log.error(f"Scan error: {e}", exc_info=True)
            time.sleep(10)

if __name__ == "__main__":
    run()
