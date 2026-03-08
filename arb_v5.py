"""
PolyArb v5 — RN1 99.9% Replica Bot (NO-ONLY Mode)
======================================================
Replicates strategy of 0x2005d16a84ceefa912d4e380cd32e7ff827875ea

CRITICAL FINDING FROM BLUEPRINT (1,098,591 trades):
  RN1 buys ONLY NO tokens. 100.0% NO, 0.0% YES.
  He NEVER builds two-sided same-market arbs.
  His edge: directional NO buying across all price ranges + cross-event Dutch.

ARCHITECTURE: WebSocket-first (falls back to REST polling)
  - Subscribes to wss://ws-subscriptions-clob.polymarket.com/ws/market
  - Receives price_change events pushed in real-time (ms latency vs 5s polling)
  - Maintains a rolling 60s price history per token for signal detection

STRATEGIES (from 1,098,591 trade extraction):
  1. CROSS-DUTCH   -- NO+NO on different outcomes in same event (draw wins both)
  2. DIRECTIONAL   -- Buy NO across full price range (0.01-0.97), NO ONLY
  3. SPIKE_DETECT  -- Price moved >3% in 10s: buy NO on momentum
  4. OB_IMBALANCE  -- Bid size >> Ask size on NO side
  5. NEAR_CERTAIN  -- NO at 0.80-0.97 (14% of his trades)
  6. FAVORED_SIDE  -- NO at 0.60-0.80 (21% of his trades)
  7. CORE_MID      -- NO at 0.15-0.60 (57% of his trades)
  8. PENNY_SCOOP   -- NO at 0.01-0.10 (7.5% of his trades)

HOW TO RUN:
  pip3 install websocket-client  # one-time
  python3 arb_v5.py              # dry run
  python3 arb_v5.py --live       # live trading (requires keys.env)
"""

import os, sys, time, json, logging, argparse, threading, requests
from datetime import datetime, timezone, timedelta
from collections import defaultdict, deque
from typing import Optional, Dict, List
from dotenv import load_dotenv

load_dotenv("keys.env")

# ─────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────
PRIVATE_KEY      = os.getenv("PK", "")
PROXY_WALLET     = os.getenv("PROXY_WALLET", "")
MY_BANKROLL      = float(os.getenv("BANKROLL", "1000"))

# ─────────────────────────────────────────────────────────────────────
# RN1 REPLICATION CONFIG — $1,000 BANKROLL
# Derived from 1,098,591 trades, 234 days, $1K→$5M
#
# *** CRITICAL: 100% NO TRADES. RN1 NEVER BUYS YES. ***
# Side distribution: YES=0 (0.0%), NO=1,027,713 (100.0%)
#
# His Jul '25 at ~$853: 183 trades/day, $2.6K/day vol, 3x turnover
#   Avg trade: $13.99 (1.64% bankroll), Median: $9.40, Max: $255
#
# His distribution (1M+ buys, ALL NO tokens):
#   0.01-0.10:  7.5% trades,  $855K vol  → PENNY
#   0.10-0.20:  8.7% trades,  $2.5M vol  → LOW RANGE
#   0.20-0.60: 48.0% trades, $40.7M vol  → CORE MID
#   0.60-0.80: 21.0% trades, $25.8M vol  → FAVORED SIDE
#   0.80-0.97: 12.2% trades, $18.5M vol  → NEAR-CERTAIN
#   0.97-1.00:  2.2% trades,  $3.5M vol  → ULTRA-CERTAIN
# ─────────────────────────────────────────────────────────────────────

# ═══ NO-ONLY MODE: RN1 buys ONLY NO tokens. Never YES. ═══
NO_ONLY_MODE = True  # The single most important config flag

# ── ARB ENGINE (guaranteed profit — always priority #1) ──
ENTRY_THRESHOLD  = float(os.getenv("ENTRY_THRESHOLD", "0.98"))
MIN_SPREAD_PCT   = float(os.getenv("MIN_SPREAD_PCT", "2.0"))

# ── PHASE 1 SEEDS (his 0.10-0.60 zone = 57% of trades) ──
PHASE1_MID_MIN   = float(os.getenv("PHASE1_MID_MIN", "0.15"))
PHASE1_MID_MAX   = float(os.getenv("PHASE1_MID_MAX", "0.60"))
PHASE1_OTHER_MAX = float(os.getenv("PHASE1_OTHER_MAX", "0.90"))
HARVEST_THRESHOLD= float(os.getenv("HARVEST_THRESHOLD", "0.85"))

# ── TRADE SIZING ($1K bankroll = his exact Jul '25 starting point) ──
# His Jul avg trade: $13.99 (1.64% bankroll), median $9.40, max $255
# At $1K proportional: avg ~$16.40, median ~$11, max ~$300
BASE_TRADE_SIZE  = float(os.getenv("BASE_TRADE_SIZE", "8.0"))      # his actual startup median at ~$1K-$8K bankroll
MAX_TRADE_SIZE   = float(os.getenv("MAX_TRADE_SIZE", "70.0"))      # 7% max single fill
MAX_POSITION_PER_MARKET = float(os.getenv("MAX_POSITION", "100.0"))  # 10% per market max
MAX_ONE_SIDE = float(os.getenv("MAX_ONE_SIDE", "80.0"))            # 8% one-side cap (NO only)
MAX_PER_EVENT_PCT = float(os.getenv("MAX_EVENT_PCT", "0.20"))      # 20% max across all sub-markets of same event
# E.g. "Timberwolves vs. Nuggets: Spread", "Timberwolves vs. Nuggets: O/U" share event "Timberwolves vs. Nuggets"

# ── SIGNAL THRESHOLDS ──
SPIKE_PCT_THRESH = float(os.getenv("SPIKE_PCT", "0.03"))
SPIKE_WINDOW_SECS= int(os.getenv("SPIKE_WINDOW", "10"))
OB_IMBALANCE_MIN = float(os.getenv("OB_IMBALANCE", "0.6"))
OB_MAX_SPREAD    = float(os.getenv("OB_MAX_SPREAD", "0.04"))

SCAN_INTERVAL    = float(os.getenv("SCAN_INTERVAL", "3.0"))
MAX_MARKETS      = int(os.getenv("MAX_MARKETS", "300"))
ORDER_EXPIRY_SECS= int(os.getenv("ORDER_EXPIRY", "120"))
MARKET_REFRESH_MINS = int(os.getenv("MARKET_REFRESH", "3"))
SPIKE_COOLDOWN   = 20.0
OBI_COOLDOWN     = 15.0

# ── ENDGAME (manage existing positions) ──
ENDGAME_MAX_PRICE  = float(os.getenv("ENDGAME_MAX_PRICE", "0.15"))
ENDGAME_TRADE_SIZE = float(os.getenv("ENDGAME_SIZE", "20.0"))     # 2% bankroll

# ── NEAR-CERTAIN HARVESTING (his 0.80-1.00 = 14% of trades, 11.7% of startup vol) ──
# At $1K: run 20-30 NC positions, each $5-15
NC_MIN_PRICE      = float(os.getenv("NC_MIN_PRICE", "0.80"))
NC_MAX_PRICE      = float(os.getenv("NC_MAX_PRICE", "0.97"))
NC_TRADE_SIZE     = float(os.getenv("NC_TRADE_SIZE", "10.0"))      # 1% bankroll per trade
NC_MAX_EXPOSURE   = float(os.getenv("NC_MAX_EXPOSURE", "0.10"))    # 10% = $100 cap
NC_COOLDOWN       = float(os.getenv("NC_COOLDOWN", "60.0"))        # 60s per market (was 120)
NC_MIN_VOLUME     = float(os.getenv("NC_MIN_VOLUME", "200"))

# ── PENNY SCOOP (his 0.01-0.10 = 7.5% of trades, 8.4% at startup) ──
PENNY_MAX_PRICE   = float(os.getenv("PENNY_MAX_PRICE", "0.10"))
PENNY_TRADE_SIZE  = float(os.getenv("PENNY_TRADE_SIZE", "3.00"))  # 0.3% bankroll (was 0.2%)
PENNY_MAX_TOTAL   = float(os.getenv("PENNY_MAX_TOTAL", "0.10"))   # 10% = $100 cap (was 8%)
PENNY_COOLDOWN    = float(os.getenv("PENNY_COOLDOWN", "60.0"))    # 60s per market (was 120)

# ── FAVORED SIDE (his 0.60-0.80 = 21% of trades, 18.4% at startup) ──
FAV_MIN_PRICE     = float(os.getenv("FAV_MIN_PRICE", "0.60"))
FAV_MAX_PRICE     = float(os.getenv("FAV_MAX_PRICE", "0.80"))
FAV_TRADE_SIZE    = float(os.getenv("FAV_TRADE_SIZE", "10.0"))    # 1% bankroll (was 0.4%)
FAV_MAX_EXPOSURE  = float(os.getenv("FAV_MAX_EXPOSURE", "0.20"))  # 20% = $200 cap (was 25%)
FAV_COOLDOWN      = float(os.getenv("FAV_COOLDOWN", "60.0"))      # 60s per market (was 120)
FAV_MIN_VOLUME    = float(os.getenv("FAV_MIN_VOLUME", "300"))

# ── CORE MID (his 0.15-0.60 = 57% of trades, ~61.5% at startup) ──
# This is the BULK of RN1's activity — directional NO buys in the mid-range
MID_MIN_PRICE     = float(os.getenv("MID_MIN_PRICE", "0.15"))
MID_MAX_PRICE     = float(os.getenv("MID_MAX_PRICE", "0.60"))
MID_TRADE_SIZE    = float(os.getenv("MID_TRADE_SIZE", "12.0"))    # 1.2% bankroll
MID_MAX_EXPOSURE  = float(os.getenv("MID_MAX_EXPOSURE", "0.50"))  # 50% = $500 cap (biggest bucket, his 48%)
MID_COOLDOWN      = float(os.getenv("MID_COOLDOWN", "45.0"))      # 45s per market (was 90, core signal)
MID_MIN_VOLUME    = float(os.getenv("MID_MIN_VOLUME", "100"))

# ── HOURLY WEIGHTING (exact from blueprint hourly_pattern, 1M+ trades) ──
# Peak: 18-21 UTC (34% of all trades). Dead: 5-8 UTC (5%).
# Weight = hour's % of trades / average (4.17%). Controls per-market cooldown divisor.
HOURLY_WEIGHTS = {
    0: 0.95,  1: 1.01,  2: 1.07,  3: 0.83,  4: 0.51,  5: 0.36,
    6: 0.28,  7: 0.25,  8: 0.31,  9: 0.45, 10: 0.49, 11: 0.57,
   12: 0.83, 13: 0.95, 14: 1.08, 15: 1.41, 16: 1.53, 17: 1.57,
   18: 1.80, 19: 1.88, 20: 2.20, 21: 2.28, 22: 0.73, 23: 0.64
}

def _get_hourly_weight() -> float:
    """Return current hour's trade frequency multiplier."""
    utc_hour = datetime.now(timezone.utc).hour
    return HOURLY_WEIGHTS.get(utc_hour, 1.0)

# ── SELL LOGIC (blueprint: 2.1% sell rate, avg $0.30, median $0.21) ──
# He sells losing positions that dropped well below entry. Rare but real.
# ── SELL / BATCH CLEANUP CONFIG ──────────────────────────────────────
# RN1 actual: 21,637 sells in 1M+ trades = 2.1% of buys
# Median sell price: $0.2077, avg: $0.3019
# Total sell revenue: $17.6M — not negligible, meaningful loss recovery
SELL_ENABLED              = True
BATCH_CLEANUP_INTERVAL_HR = float(os.getenv("CLEANUP_INTERVAL", "6.0"))  # check every 6hr
SELL_LOSER_THRESH         = float(os.getenv("SELL_LOSER", "0.25"))        # sell NO if value dropped below this (RN1 median sell: $0.21)
SELL_WINNER_THRESH        = float(os.getenv("SELL_WINNER", "0.95"))       # take profit if NO value above this
SELL_MIN_AGE_HR           = float(os.getenv("SELL_MIN_AGE", "2.0"))       # min 2hr before considering sell
SELL_STALE_DAYS           = float(os.getenv("SELL_STALE", "3.0"))         # sell if event date passed 3+ days ago

# ── STARTUP RAMP — DISABLED ──────────────────────────────────────
# Deep analysis: RN1 did 111 buys/day from DAY 1 across 90 markets.
# No ramp needed — he was a bot from the start.
STARTUP_RAMP_TRADES    = int(os.getenv("RAMP_TRADES", "0"))     # 0 = disabled
STARTUP_RAMP_MIN_MULT  = float(os.getenv("RAMP_MIN", "1.0"))   # 1.0 = full speed
_startup_trade_count   = 0

# ── GLOBAL RATE LIMITER ─────────────────────────────────────────────
# RN1 at startup: 111 buys/day across 90 markets = ~5/hr new markets,
# but heavy re-buys (avg 11 per market). Median gap 38s between trades.
# We need to allow similar throughput while staying safe at $1K.
GLOBAL_MAX_FILLS_PER_MIN = int(os.getenv("MAX_FILLS_MIN", "20"))   # 143 rate rejects at 10/min; signals burst during scans
GLOBAL_MAX_NEW_POS_PER_HR = int(os.getenv("MAX_NEW_HR", "60"))   # RN1 hit 90 markets in 9 days; bursts need headroom
MAX_DEPLOY_PCT = float(os.getenv("MAX_DEPLOY_PCT", "0.90"))      # 90% max — 3.0x daily turnover means fast capital recycling
_global_fill_times: list = []   # timestamps of recent fills
_global_new_pos_times: list = []  # timestamps of new position entries

def _global_rate_ok(is_new_position: bool = False) -> bool:
    """Check if we're within global rate limits."""
    now = time.time()
    
    # Clean old entries
    cutoff_1m = now - 60
    cutoff_1h = now - 3600
    while _global_fill_times and _global_fill_times[0] < cutoff_1m:
        _global_fill_times.pop(0)
    while _global_new_pos_times and _global_new_pos_times[0] < cutoff_1h:
        _global_new_pos_times.pop(0)
    
    # Check fills/minute
    if len(_global_fill_times) >= GLOBAL_MAX_FILLS_PER_MIN:
        return False
    
    # Check new positions/hour
    if is_new_position and len(_global_new_pos_times) >= GLOBAL_MAX_NEW_POS_PER_HR:
        return False
    
    return True

def _startup_multiplier() -> float:
    """Returns 0.3 → 1.0 scaling multiplier based on trade count."""
    if _startup_trade_count >= STARTUP_RAMP_TRADES:
        return 1.0
    progress = _startup_trade_count / STARTUP_RAMP_TRADES
    return STARTUP_RAMP_MIN_MULT + (1.0 - STARTUP_RAMP_MIN_MULT) * progress

# ── CONVICTION SIZING ───────────────────────────────────────────────
# RN1 deep analysis (first 1K buys): p50=$8, p90=$17, p99=$39, max=$86
# His startup sizes were MUCH smaller than his later sizes
# Conviction triggers: high OBI + high liquidity + specific price ranges
CONVICTION_OBI_THRESH  = float(os.getenv("CONV_OBI", "0.70"))   # OBI > 70% for conviction
CONVICTION_SIZE_MULT   = float(os.getenv("CONV_MULT", "3.0"))   # 3x normal for conviction (was 5x)
CONVICTION_COOLDOWN    = float(os.getenv("CONV_CD", "600.0"))   # 10 min between conviction bets
_last_conviction = {}

# ── RN1 PRICE-SIZE CURVE (RECALIBRATED TO STARTUP DATA) ──
# His actual startup medians (first 1K buys at ~$1K-$8K bankroll):
#   penny: $1.04, low: $2.60, mid20-40: $5.20, mid40-60: $9.60
#   fav: $13.20, NC80-90: $17.00, heavyNC: $18.20
# At $1K bankroll with BASE_TRADE_SIZE=$8, this produces:
#   penny: $0.32-$1.04, mid: $5.20-$9.60, fav: $13.20, nc: $17-$18
RN1_PRICE_SIZE_MULT = {
    0.01: 0.04, 0.03: 0.08, 0.06: 0.10, 0.09: 0.13,   # penny: med $1.04
    0.12: 0.20, 0.17: 0.28, 0.22: 0.45, 0.27: 0.55,   # low: med $2.60
    0.32: 0.60, 0.37: 0.65, 0.42: 0.85, 0.47: 1.05,   # mid: med $5.20-$9.60
    0.52: 1.10, 0.57: 1.20, 0.62: 1.40, 0.67: 1.55,   # upper mid + fav starts
    0.72: 1.65, 0.77: 1.75, 0.82: 2.00, 0.87: 2.13,   # fav + NC: med $13-$17
    0.92: 2.20, 0.97: 2.28                              # heavy NC: med $18.20
}

def _rn1_size_for_price(price: float, conviction: bool = False) -> float:
    """Return trade size scaled to RN1's actual STARTUP price-size curve.
    Uses median costs from his first 1K buys, normalized to BASE_TRADE_SIZE.
    RN1 startup: p50=$8, p90=$17, p99=$39 — tight distribution.
    conviction=True gives 3x size for high-OBI opportunities."""
    buckets = sorted(RN1_PRICE_SIZE_MULT.keys())
    best = buckets[0]
    for b in buckets:
        if abs(b - price) < abs(best - price):
            best = b
    mult = RN1_PRICE_SIZE_MULT[best]
    base = BASE_TRADE_SIZE * mult
    
    # Apply startup ramp (0.3x → 1.0x over first 50 trades)
    base *= _startup_multiplier()
    
    # Apply conviction multiplier for high-conviction entries
    if conviction:
        base *= CONVICTION_SIZE_MULT
    
    return round(base, 2)

# STALE PRICE GATE: allow Phase 2 completion up to 5% negative combined
# (live prices lag reality — RN1 enters at slight negative spread intentionally)
STALE_PRICE_GATE   = float(os.getenv("STALE_PRICE_GATE", "1.02"))  # allow 2% negative — RN1 does this

# FOCUS MODE: max markets with open positions at once. Once full, no new
# Phase 1 entries — concentrate on managing what we already hold.
MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "90"))   # RN1 startup: 90 markets in first 9 days
MAX_PER_EVENT      = int(os.getenv("MAX_PER_EVENT", "15"))    # generous count; dollar-based event cap in _fill() handles real limiting
MIN_VOLUME         = float(os.getenv("MIN_VOLUME", "0"))   # off — was causing no fills  # min $500 traded — filters illiquid markets

# BANKROLL UTILIZATION CAP: never deploy more than this fraction of bankroll total.
# At $1000 bankroll, 40% = $400 max total deployed across all markets.
MAX_BANKROLL_UTIL = float(os.getenv("MAX_BANKROLL_UTIL", "1.0"))

# ADDING gate is PRICE-BASED, not time-based. The bot scans every WS tick.
# We only add to a side if the current ask is LOWER than our last fill on
# that side — every new fill must improve our average entry price.
# No cooldown. No waiting. Just: "is this price better than what we have?"

# HAMMER: one side is near-certain (game essentially over).
# Aggressively buy more of our winning side.
HAMMER_PRICE = float(os.getenv("HAMMER_PRICE", "0.85"))

# UPSET HEDGE: after hammering, buy a tiny amount of the cheap collapsing
# side as insurance against a late-game reversal. RN1 pattern.
UPSET_HEDGE_MAX  = float(os.getenv("UPSET_HEDGE_MAX", "0.12"))  # only if price <= this
UPSET_HEDGE_SIZE = float(os.getenv("UPSET_HEDGE_SIZE", "3.0"))  # $3 flat per hedge

DRY_RUN = True

# ─────────────────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────────────────
CLOB   = "https://clob.polymarket.com"
GAMMA  = "https://gamma-api.polymarket.com"
WS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ─────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
LOG_FILE = f"logs/arb_v3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)]
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# HTTP SESSION
# ─────────────────────────────────────────────────────────────────────
import urllib3
urllib3.disable_warnings(urllib3.exceptions.NotOpenSSLWarning)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

_adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=40, max_retries=0)
_session = requests.Session()
_session.headers.update({"Accept": "application/json", "User-Agent": "PolyArb/3.0"})
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)

def http_get(url, params=None, timeout=8, retries=2):
    for i in range(retries):
        try:
            r = _session.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(3 * (i + 1))
        except Exception as e:
            log.debug(f"HTTP {url}: {e}")
            time.sleep(1)
    return None

# ─────────────────────────────────────────────────────────────────────
# TOKEN STATE  (live price + history per token)
# ─────────────────────────────────────────────────────────────────────
class TokenState:
    __slots__ = ("ask","bid","bid_size","ask_size","last_ts","history")
    def __init__(self):
        self.ask = self.bid = self.bid_size = self.ask_size = self.last_ts = 0.0
        self.history: deque = deque(maxlen=200)  # (timestamp, price)

    def update(self, price=None, bid=None, ask=None, bid_size=None, ask_size=None):
        now = time.time()
        if ask is not None and ask > 0:   self.ask = ask
        elif price and not self.ask:      self.ask = price
        if bid is not None and bid > 0:   self.bid = bid
        if bid_size is not None:          self.bid_size = bid_size
        if ask_size is not None:          self.ask_size = ask_size
        p = price or self.ask
        if p > 0: self.history.append((now, p))
        self.last_ts = now

    @property
    def first_ts(self) -> float:
        """Timestamp of oldest recorded price. 0 if no history."""
        return self.history[0][0] if self.history else 0.0

    def price_velocity(self, window_secs=10) -> float:
        """
        Price change over the last window_secs seconds.
        Returns 0.0 if we don't have data older than window_secs
        (prevents false spikes on first-connection warmup).
        """
        if len(self.history) < 2: return 0.0
        now = time.time()
        latest = self.history[-1][1]
        past = None
        for ts, p in reversed(self.history):
            if now - ts >= window_secs:
                past = p; break
        if past is None:
            return 0.0  # no data old enough — still warming up
        return latest - past

    def ob_imbalance(self) -> float:
        total = self.bid_size + self.ask_size
        return (self.bid_size - self.ask_size) / total if total > 1e-6 else 0.0

    def spread(self) -> float:
        return (self.ask - self.bid) if (self.bid > 0 and self.ask > 0) else 0.0

# Global state
token_states: Dict[str, TokenState]  = defaultdict(TokenState)
token_to_market: Dict[str, str]      = {}  # token_id -> market_id
market_registry: Dict[str, dict]     = {}  # market_id -> {title,yes_token,no_token,end_dt}
_registry_lock = threading.Lock()
# Per-signal last-action timestamps  market_id -> float
_last_spike:  Dict[str, float] = defaultdict(float)
_last_obi:    Dict[str, float] = defaultdict(float)
_last_hammer: Dict[str, float] = defaultdict(float)   # HAMMER cooldown per market
_attempted_fills: set = set()  # (market_id, side, price_rounded) — skip if already tried this exact setup

# ── ORDER PARAM CACHE ─────────────────────────────────────────────────
# tick_size, neg_risk, fee_rate almost never change mid-game.
# Cache them per token to eliminate 3 of 4 API calls per order.
_order_param_cache: Dict[str, dict] = {}
_order_param_lock  = threading.Lock()
ORDER_CACHE_TTL    = 300  # seconds — refresh every 5 minutes

def _get_order_params(token_id: str) -> dict:
    """Return cached {tick_size, neg_risk, fee_rate} for token, fetching if stale."""
    now = time.time()
    with _order_param_lock:
        cached = _order_param_cache.get(token_id)
        if cached and now - cached["_ts"] < ORDER_CACHE_TTL:
            return cached
    # Fetch fresh — outside lock so we don't block other threads
    try:
        client = get_clob_client()
        tick   = client.get_tick_size(token_id)
        neg    = client.get_neg_risk(token_id)
        fee    = client.get_fee_rate_charged(token_id)
        params = {"tick_size": tick, "neg_risk": neg, "fee_rate": fee, "_ts": now}
        with _order_param_lock:
            _order_param_cache[token_id] = params
        return params
    except Exception as e:
        log.debug(f"order param fetch err [{token_id[:12]}]: {e}")
        return {}
_last_cross_dutch: Dict[str, float] = defaultdict(float)  # event_key -> last fire time (cross-market NO+NO)
_last_p2: Dict[str, float] = defaultdict(float)              # per-market cooldown for Phase2/ADDING log spam
# Global WS reference: used by register_markets to subscribe new tokens incrementally
_ws_global = None
_ws_subscribed_tokens: set = set()  # tokens already subscribed to WS
# RN1-strategy cooldowns
_last_nc:    Dict[str, float] = defaultdict(float)   # near-certain per-market
_last_penny: Dict[str, float] = defaultdict(float)   # penny scoop per-market
_last_fav:   Dict[str, float] = defaultdict(float)   # favored side per-market
_last_mid:   Dict[str, float] = defaultdict(float)   # core mid per-market
# Per-market mutex: prevents WS thread + position_watcher from evaluating
# the same market simultaneously (the most likely freeze source).
_eval_locks:  Dict[str, threading.Lock] = defaultdict(threading.Lock)

# ─────────────────────────────────────────────────────────────────────
# POSITION LEDGER
# ─────────────────────────────────────────────────────────────────────
class MarketPosition:
    def __init__(self, market_id, title, yes_token, no_token):
        self.market_id = market_id; self.title = title
        self.yes_token = yes_token; self.no_token = no_token
        self.yes_fills = []; self.no_fills = []
        self.open_orders = {}; self.total_deployed = 0.0
        self.created_at = self.last_trade = time.time()
        self.stagnant = False  # True = one-sided seed with no Phase 2 activity, slot freed
        self.resolved = False  # True = market settled, P&L locked, slot freed
        self.resolved_pl = None  # actual P&L at resolution (None = unknown)
        self.pending = False    # True = seeded but game not yet live, slot freed temporarily

    @property
    def yes_total(self): return sum(a for _,a in self.yes_fills)
    @property
    def no_total(self):  return sum(a for _,a in self.no_fills)
    @property
    def yes_avg_price(self):
        t = self.yes_total
        return sum(p*a for p,a in self.yes_fills)/t if t else 0
    @property
    def no_avg_price(self):
        t = self.no_total
        return sum(p*a for p,a in self.no_fills)/t if t else 0
    @property
    def implied_spread(self):
        """
        True spread = guaranteed profit as % of total cost.
        Negative means position loses money on the worse outcome.
        """
        if not self.yes_fills or not self.no_fills: return 0
        total_cost = self.yes_total + self.no_total
        if total_cost <= 0: return 0
        return self.estimated_profit / total_cost

    @property
    def last_yes_price(self):
        """Price of our most recent YES fill. Used to gate ADDING."""
        return self.yes_fills[-1][0] if self.yes_fills else 1.0

    @property
    def last_no_price(self):
        """Price of our most recent NO fill. Used to gate ADDING."""
        return self.no_fills[-1][0] if self.no_fills else 1.0
    @property
    def estimated_profit(self):
        """
        True guaranteed profit = min(payout if YES wins, payout if NO wins) - total_cost.
        Both must be positive for a real arb. If one is negative, that's the real P&L.
        """
        if not self.yes_fills or not self.no_fills: return 0
        total_cost  = self.yes_total + self.no_total
        yes_shares  = self.yes_total / self.yes_avg_price if self.yes_avg_price > 0 else 0
        no_shares   = self.no_total  / self.no_avg_price  if self.no_avg_price  > 0 else 0
        yes_win_pl  = yes_shares - total_cost   # profit if YES resolves $1
        no_win_pl   = no_shares  - total_cost   # profit if NO  resolves $1
        return min(yes_win_pl, no_win_pl)       # guaranteed profit (worst case)

    def summary(self):
        return (
            f"YES:{len(self.yes_fills)} @{self.yes_avg_price:.3f} ${self.yes_total:.0f} | "
            f"NO:{len(self.no_fills)} @{self.no_avg_price:.3f} ${self.no_total:.0f} | "
            f"Spread:{self.implied_spread*100:.1f}% | Est:${self.estimated_profit:.2f}"
        )


LEDGER_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ledger.json")

class PositionLedger:
    def __init__(self):
        self.positions: Dict[str, MarketPosition] = {}
        self.lock = threading.Lock()

    def save(self):
        """Persist ledger to disk after every fill."""
        try:
            data = {}
            for mid, pos in self.positions.items():
                if not pos.yes_fills and not pos.no_fills:
                    continue
                data[mid] = {
                    "title":      pos.title,
                    "yes_token":  pos.yes_token,
                    "no_token":   pos.no_token,
                    "yes_fills":  pos.yes_fills,
                    "no_fills":   pos.no_fills,
                    "total_deployed": pos.total_deployed,
                    "created_at": pos.created_at,
                    "last_trade": pos.last_trade,
                    "stagnant":   pos.stagnant,
                    "resolved":   pos.resolved,
                    "resolved_pl": pos.resolved_pl,
                    "pending":     pos.pending,
                }
            with open(LEDGER_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            log.warning(f"Ledger save failed: {e}")

    def load(self):
        """Reload ledger from disk on startup."""
        if not os.path.exists(LEDGER_FILE):
            return
        try:
            with open(LEDGER_FILE) as f:
                data = json.load(f)
            count = 0
            for mid, d in data.items():
                pos = MarketPosition(mid, d["title"], d["yes_token"], d["no_token"])
                pos.yes_fills      = [tuple(x) for x in d["yes_fills"]]
                pos.no_fills       = [tuple(x) for x in d["no_fills"]]
                pos.total_deployed = d["total_deployed"]
                # Use saved created_at for active_count age calculation.
                # The stagnant hedge timer uses last_trade instead.
                pos.created_at     = d.get("created_at", time.time())
                pos.last_trade     = d.get("last_trade",  time.time())
                pos.stagnant       = d.get("stagnant", False)
                pos.resolved       = d.get("resolved", False)
                pos.resolved_pl    = d.get("resolved_pl", None)
                pos.pending        = d.get("pending", False)
                self.positions[mid] = pos
                count += 1
            total = sum(p.total_deployed for p in self.positions.values())
            log.info(f"Ledger restored: {count} positions, ${total:.2f} deployed")
        except Exception as e:
            log.warning(f"Ledger load failed: {e}")

    def resolve_missing_markets(self):
        """Legacy stub — resolution now handled by _check_resolutions_via_api()."""
        pass

    def get(self, market_id):
        """Return existing position or None."""
        with self.lock:
            return self.positions.get(market_id)

    def get_or_create(self, market_id, title, yes_token, no_token):
        with self.lock:
            if market_id not in self.positions:
                self.positions[market_id] = MarketPosition(market_id, title, yes_token, no_token)
            return self.positions[market_id]

    def record_fill(self, market_id, side, price, amount):
        with self.lock:
            pos = self.positions.get(market_id)
            if not pos: return
            (pos.yes_fills if side == "YES" else pos.no_fills).append((price, amount))
            pos.total_deployed += amount
            pos.last_trade = time.time()
        self.save()  # persist immediately after every fill

    def total_deployed(self):
        """Only count active (non-resolved) positions against bankroll cap."""
        with self.lock:
            return sum(p.total_deployed for p in self.positions.values() if not p.resolved)

    def _total_nolock(self):
        """Total without acquiring lock — only call when lock already held."""
        return sum(p.total_deployed for p in self.positions.values() if not p.resolved)

    def active_count(self) -> int:
        """Count positions with deployed capital that haven't resolved."""
        with self.lock:
            count = 0
            for p in self.positions.values():
                if p.resolved:
                    continue
                if p.total_deployed > 0:
                    count += 1
            return count

    def directional_exposure(self) -> float:
        """Total $ deployed in one-sided near-certain positions (price >= NC_MIN_PRICE).
        Used to cap directional risk separately from arb positions."""
        with self.lock:
            total = 0
            for p in self.positions.values():
                if p.resolved: continue
                has_yes = p.yes_total > 0
                has_no = p.no_total > 0
                if has_yes and has_no: continue  # two-sided = arb, not directional
                if has_yes and p.yes_avg_price >= NC_MIN_PRICE:
                    total += p.yes_total
                elif has_no and p.no_avg_price >= NC_MIN_PRICE:
                    total += p.no_total
            return total

    def penny_exposure(self) -> float:
        """Total $ deployed in penny scoop positions (price <= PENNY_MAX_PRICE)."""
        with self.lock:
            total = 0
            for p in self.positions.values():
                if p.resolved: continue
                has_yes = p.yes_total > 0
                has_no = p.no_total > 0
                if has_yes and has_no: continue
                if has_yes and p.yes_avg_price <= PENNY_MAX_PRICE:
                    total += p.yes_total
                elif has_no and p.no_avg_price <= PENNY_MAX_PRICE:
                    total += p.no_total
            return total

    def favored_exposure(self) -> float:
        """Total $ deployed in favored-side positions (price 0.60-0.80)."""
        with self.lock:
            total = 0
            for p in self.positions.values():
                if p.resolved: continue
                has_yes = p.yes_total > 0
                has_no = p.no_total > 0
                if has_yes and has_no: continue
                if has_yes and FAV_MIN_PRICE <= p.yes_avg_price <= FAV_MAX_PRICE:
                    total += p.yes_total
                elif has_no and FAV_MIN_PRICE <= p.no_avg_price <= FAV_MAX_PRICE:
                    total += p.no_total
            return total

    def mid_exposure(self) -> float:
        """Total $ deployed in core mid positions (price 0.15-0.60)."""
        with self.lock:
            total = 0
            for p in self.positions.values():
                if p.resolved: continue
                has_yes = p.yes_total > 0
                has_no = p.no_total > 0
                if has_yes and has_no: continue
                if has_no and MID_MIN_PRICE <= p.no_avg_price <= MID_MAX_PRICE:
                    total += p.no_total
                elif has_yes and MID_MIN_PRICE <= p.yes_avg_price <= MID_MAX_PRICE:
                    total += p.yes_total
            return total

    def print_all(self):
        with self.lock:
            log.info("=" * 75)
            log.info(f"POSITION LEDGER: {len(self.positions)} markets")
            for _, pos in sorted(self.positions.items()):
                if pos.total_deployed > 0:
                    log.info(f"  {pos.title[:60]}")
                    log.info(f"    {pos.summary()}")
            log.info(f"  TOTAL: ${self._total_nolock():.2f}")
            log.info("=" * 75)

ledger = PositionLedger()

# ─────────────────────────────────────────────────────────────────────
# SESSION STATS
# ─────────────────────────────────────────────────────────────────────
class SessionStats:
    def __init__(self):
        self.start=time.time(); self.ws_events=0; self.markets_seen=0
        self.dutch=0; self.spike=0; self.obi=0; self.orders=0; self.fills=0
        self.near_certain=0; self.penny=0; self.favored=0; self.mid=0; self.sells=0
        # Rejection counters
        self.rej_rate=0; self.rej_size=0; self.rej_deploy=0; self.rej_event=0
    def summary(self):
        m = (time.time()-self.start)/60
        signals = self.dutch+self.spike+self.obi+self.near_certain+self.favored+self.mid+self.penny
        return (f"Runtime:{m:.1f}m | Markets:{self.markets_seen} | WS:{self.ws_events} | "
                f"Dutch:{self.dutch} Spike:{self.spike} OBI:{self.obi} NC:{self.near_certain} "
                f"Fav:{self.favored} Mid:{self.mid} Penny:{self.penny} Sell:{self.sells} | "
                f"Signals:{signals} Fills:{self.fills} | "
                f"Rej[rate:{self.rej_rate} sz:{self.rej_size} deploy:{self.rej_deploy} event:{self.rej_event}] | "
                f"${ledger.total_deployed():.2f}")

stats = SessionStats()

# ─────────────────────────────────────────────────────────────────────
# CLOB CLIENT
# ─────────────────────────────────────────────────────────────────────
_clob_client = None
def get_clob_client():
    global _clob_client
    if _clob_client: return _clob_client
    if not PRIVATE_KEY or PRIVATE_KEY == "YOUR_PRIVATE_KEY_HERE":
        raise RuntimeError("Set PK in keys.env")
    from py_clob_client.client import ClobClient
    _clob_client = ClobClient(CLOB, key=PRIVATE_KEY, chain_id=137,
                               signature_type=1, funder=PROXY_WALLET)
    _clob_client.set_api_creds(_clob_client.create_or_derive_api_creds())
    log.info(f"CLOB ready (wallet:{PROXY_WALLET[:12]}...)")
    return _clob_client

# ─────────────────────────────────────────────────────────────────────
# MARKET DISCOVERY
# ─────────────────────────────────────────────────────────────────────
# Sports tag IDs — fallback if /sports endpoint unavailable
_FALLBACK_TAG_IDS = [
    82, 306, 780, 450, 100351, 1494, 100350, 100100, 100639, 100977, 1234,  # football
    745, 100254, 100149, 28, 101178,                                          # basketball
    899, 100088,                                                               # hockey
    864, 101232, 102123,                                                       # tennis
]
_sports_tag_ids_cache: Optional[List[int]] = None

def _get_sports_tag_ids() -> List[int]:
    """Fetch ALL sports tag IDs from /sports — including esports (CS2, LoL, etc).
    RN1 actively trades esports markets, particularly CS2 match winner + O/U maps."""
    global _sports_tag_ids_cache
    if _sports_tag_ids_cache is not None:
        return _sports_tag_ids_cache
    try:
        data = http_get(f"{GAMMA}/sports", timeout=10)
        if not data or not isinstance(data, list):
            return _FALLBACK_TAG_IDS
        tag_ids = set()
        for s in data:
            sport_name = (s.get("label") or s.get("name") or s.get("slug") or "").lower()
            for tid in (s.get("tags") or "").replace(" ","").split(","):
                if tid.isdigit():
                    tag_ids.add(int(tid))
                    # Auto-detect hockey tags and add to _ACTIVE_FLAG_TAGS
                    if any(kw in sport_name for kw in ["hockey", "nhl", "ice"]):
                        _ACTIVE_FLAG_TAGS.add(int(tid))
        result = list(tag_ids) if tag_ids else _FALLBACK_TAG_IDS
        _sports_tag_ids_cache = result
        log.info(f"Sports tags loaded: {len(result)} tag IDs (all sports incl. esports) — {sorted(result)[:10]}...")
        log.info(f"  Active-flag tags (hockey): {sorted(_ACTIVE_FLAG_TAGS)}")
        return result
    except Exception as e:
        log.warning(f"Could not fetch /sports: {e} — using fallback tag IDs")
        return _FALLBACK_TAG_IDS


# Tags that use active=True instead of live=True for in-progress games
# (Now ALL tags get promoted — kept for reference only)
_ACTIVE_FLAG_TAGS = set()

def _fetch_events_for_tag(tag_id: int) -> List[dict]:
    """Fetch events for one sport tag. Promotes active events as live."""
    results = []
    seen_ids = set()

    # Fetch by multiple sort keys to get breadth
    for sort_key in ["volume", "start_date", "id"]:
        data = http_get(f"{GAMMA}/events", params={
            "tag_id": tag_id, "closed": "false", "limit": "200",
            "order": sort_key, "ascending": "false",
        })
        for e in (data if isinstance(data, list) else []):
            eid = e.get("id") or e.get("slug")
            if eid and eid not in seen_ids:
                seen_ids.add(eid)
                results.append(e)

    # Promote active events with trading activity to live=True
    # Polymarket often doesn't set live=True even for in-progress games
    # (known issue for hockey, but affects NCAAB, esports, soccer too)
    now_utc = datetime.now(timezone.utc)
    promoted = 0
    for e in results:
        if e.get("live"):
            continue
        if not e.get("active"):
            continue
        vol = float(e.get("volume") or 0)
        liq = float(e.get("liquidity") or 0)
        if vol < 50 and liq < 100:
            continue
        e["live"] = True
        promoted += 1

    if promoted:
        log.info(f"  Tag {tag_id}: {len(results)} events, promoted {promoted} active→live")

    return results


def _flatten_event_markets(event: dict) -> List[dict]:
    """Extract individual markets from an event dict (including child events merged in)."""
    return event.get("markets") or []


def fetch_live_markets() -> List[dict]:
    """
    Fetch live sports markets via /events?tag_id API (from sports_server.py).
    Much better than /markets pagination:
      - Uses /sports metadata to get real sport tag IDs
      - Filters to live=True events only (game in progress)
      - Includes esports (CS2, LoL, etc) — RN1 trades these heavily
      - Returns markets with sportsMarketType for smart filtering
    Falls back to /markets pagination if events API returns nothing.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed as cf_completed
    tag_ids = _get_sports_tag_ids()
    seen_event_slugs = set()
    raw_events: List[dict] = []
    _scan_start = time.time()

    # Fetch all sport tags in parallel
    with ThreadPoolExecutor(max_workers=min(30, len(tag_ids))) as ex:
        futures = {ex.submit(_fetch_events_for_tag, tid): tid for tid in tag_ids}
        for f in cf_completed(futures):
            try:
                for ev in f.result():
                    slug = ev.get("slug") or str(ev.get("id",""))
                    if not slug or slug in seen_event_slugs:
                        continue
                    is_live = ev.get("live", False)
                    if not is_live:
                        continue
                    seen_event_slugs.add(slug)
                    raw_events.append(ev)
            except: pass

    # Sort by liquidity+volume (best markets first)
    def _score(e):
        return float(e.get("liquidity") or 0) + float(e.get("volume") or 0) * 2
    raw_events.sort(key=_score, reverse=True)

    # Flatten events → individual markets
    flat_markets: List[dict] = []
    seen_mids = set()
    for ev in raw_events:
        ev_title = ev.get("title","")
        for m in _flatten_event_markets(ev):
            if not m.get("active") or not m.get("acceptingOrders", True):
                continue
            # Skip season-long/futures markets (MVP, championship, etc.)
            # Individual game markets (moneyline, spread, O/U, props) all pass through
            # regardless of endDate — pre-game arbs are valid.
            market_type  = (m.get("sportsMarketType") or m.get("marketType") or "").lower()
            _title_lower = (m.get("question") or m.get("title") or "").lower()
            _futures_kws = ["mvp", "champion", "make the playoffs", "win the world",
                            "world series", "stanley cup", "nba finals", "world cup",
                            "season wins", "2027", "award", "ballon d'or",
                            "win the 2026"]
            if any(kw in _title_lower for kw in _futures_kws):
                continue  # season-long market — skip
            # End-date filter: skip markets > 3 days out
            _end_str = m.get("endDate") or m.get("end_date_iso") or ""
            if _end_str:
                try:
                    from datetime import datetime, timezone
                    _end_dt = datetime.fromisoformat(_end_str.replace("Z", "+00:00"))
                    if (_end_dt - datetime.now(timezone.utc)).total_seconds() > 3 * 86400:
                        continue
                except: pass
            mid = m.get("conditionId") or m.get("id","")
            if not mid or mid in seen_mids:
                continue
            toks = m.get("clobTokenIds") or []
            if isinstance(toks, str):
                try: toks = json.loads(toks)
                except: continue
            if len(toks) < 2: continue
            # Attach event context to each market
            m["_event_title"] = ev_title
            m["_event_live"]  = ev.get("live", False)
            m["_event_score"] = ev.get("score")
            m["_sport_type"]  = m.get("sportsMarketType","")
            seen_mids.add(mid)
            flat_markets.append(m)

    if flat_markets:
        _scan_secs = time.time() - _scan_start
        log.info(f"fetch_live_markets: {len(flat_markets)} markets from {len(raw_events)} live events ({_scan_secs:.1f}s)")
        return flat_markets

    # Fallback: paginate /markets sorted by end_date if events returned nothing
    log.warning("Events API returned 0 live events — falling back to /markets pagination")
    seen, markets, now = set(), [], datetime.now(timezone.utc)
    offset = 0
    while True:
        data = http_get(f"{GAMMA}/markets", params={
            "active":"true","closed":"false",
            "limit":"100","offset":str(offset),
            "order":"end_date_iso","ascending":"true"
        })
        if not data or not isinstance(data, list) or not data:
            break
        hit_cutoff = False
        for m in data:
            mid = m.get("conditionId") or m.get("id","")
            toks = m.get("clobTokenIds") or []
            if isinstance(toks, str):
                try: toks = json.loads(toks)
                except: continue
            if len(toks) < 2: continue
            end_str = m.get("endDate") or m.get("end_date_iso") or ""
            if not end_str: continue
            try:
                end_dt = datetime.fromisoformat(end_str.replace("Z","+00:00"))
                hrs = (end_dt - now).total_seconds() / 3600
                if hrs < 0: continue
                if hrs > 24: hit_cutoff = True; break
            except: continue
            if mid in seen: continue
            seen.add(mid); markets.append(m)
        if hit_cutoff or len(data) < 100 or len(markets) >= MAX_MARKETS:
            break
        offset += 100
    log.info(f"fetch_live_markets (fallback): {len(markets)} markets")
    return markets


def register_markets(markets: List[dict]):
    # Un-pending any positions that just came back into the live feed
    live_ids = {m.get("conditionId") or m.get("id","") for m in markets}
    reactivated = False
    for mid, pos in ledger.positions.items():
        if pos.pending and mid in live_ids:
            pos.pending  = False
            pos.stagnant = False
            log.info(f"REACTIVATED (now live): {pos.title[:55]}")
            reactivated = True
    if reactivated:
        ledger.save()
    _register_markets_impl(markets)
    # Subscribe any NEW tokens to the live WS connection
    _ws_subscribe_new_tokens()

def _ws_subscribe_new_tokens():
    """Send subscription messages for tokens not yet on the WS feed."""
    global _ws_global
    if _ws_global is None:
        return
    with _registry_lock:
        all_tokens = set(token_to_market.keys())
    new_tokens = list(all_tokens - _ws_subscribed_tokens)
    if not new_tokens:
        return
    try:
        for i in range(0, len(new_tokens), 500):
            chunk = new_tokens[i:i+500]
            _ws_global.send(json.dumps({"assets_ids": chunk, "type": "market"}))
            _ws_subscribed_tokens.update(chunk)
        log.info(f"WS incremental subscribe: {len(new_tokens)} new tokens (total: {len(_ws_subscribed_tokens)})")
    except Exception as e:
        log.debug(f"WS incremental subscribe failed: {e}")

def _register_markets_impl(markets: List[dict]):
    with _registry_lock:
        for m in markets:
            mid = m.get("conditionId") or m.get("id","")
            toks = m.get("clobTokenIds") or []
            if isinstance(toks, str):
                try: toks = json.loads(toks)
                except: continue
            if len(toks) < 2: continue
            yes_tok, no_tok = str(toks[0]), str(toks[1])
            # Use event title if available (richer than market question alone)
            title = (m.get("_event_title") or m.get("question") or m.get("title") or "Unknown")
            mkt_q = m.get("question") or m.get("title") or ""
            if mkt_q and mkt_q != title:
                title = f"{title}: {mkt_q}"
            end_str = m.get("endDate") or m.get("end_date_iso") or ""
            end_dt = None
            try: end_dt = datetime.fromisoformat(end_str.replace("Z","+00:00"))
            except: pass
            market_registry[mid] = {
                "title":       title,
                "event_title": m.get("_event_title") or title.split(":")[0],  # for grouping
                "yes_token":  yes_tok,
                "no_token":   no_tok,
                "end_dt":     end_dt,
                "sport_type": m.get("_sport_type",""),   # moneyline/totals/spreads/btts
                "live":       m.get("_event_live", True),
                "score":      m.get("_event_score"),
                "volume":     float(m.get("volume") or 0),
                "liquidity":  float(m.get("liquidity") or 0),
            }
            token_to_market[yes_tok] = mid
            token_to_market[no_tok]  = mid
    stats.markets_seen = len(market_registry)

# ─────────────────────────────────────────────────────────────────────
# REST PRICE FETCH (fallback / initial seeding)
# ─────────────────────────────────────────────────────────────────────
def _best(book, side):
    rows = book.get("bids" if side=="bid" else "asks",[])
    prices = []
    for r in rows:
        try: prices.append(float(r["price"]))
        except: pass
    return (max(prices) if side=="bid" else min(prices)) if prices else 0.0

def _sz(book, side):
    total = 0.0
    for r in (book.get("bids" if side=="bid" else "asks",[]))[:3]:
        try: total += float(r.get("size",0))
        except: pass
    return total

def refresh_prices_rest(market_id: str):
    info = market_registry.get(market_id)
    if not info: return
    for tok in (info["yes_token"], info["no_token"]):
        book = http_get(f"{CLOB}/book", params={"token_id": tok})
        if not book: continue
        ask = _best(book,"ask"); bid = _best(book,"bid")
        token_states[tok].update(
            price=ask, bid=bid, ask=ask,
            bid_size=_sz(book,"bid"), ask_size=_sz(book,"ask")
        )

# ─────────────────────────────────────────────────────────────────────
# SIZING
# ─────────────────────────────────────────────────────────────────────
def calculate_size(yp, np_, pos):
    """
    Size new Dutch entries proportional to the OPPORTUNITY, not a fixed base.
    
    Bigger spread = more capital deployed. Uses continuous scaling so every
    entry is sized differently — just like RN1's observed behavior.
    
    Equal SHARES on both sides so either outcome covers total cost.
    No bankroll caps — ROI improvement gate is the only constraint.
    """
    sp = (1.0 - yp - np_) * 100
    if sp < MIN_SPREAD_PCT: return 0, 0

    # Continuous sizing: fraction of bankroll proportional to spread.
    # spread 3% → ~6%, 5% → ~10%, 10% → ~20%, 20% → ~40%, 50% → ~75%
    deploy_frac = min(0.80, 2.0 * (sp / 100.0) ** 0.75)
    target_cost = MY_BANKROLL * deploy_frac
    target_cost = max(2.0, target_cost)

    # Equal shares: shares = target_cost / (yp + np_)
    total_price = yp + np_
    shares = target_cost / total_price
    yes_cost = round(shares * yp, 2)
    no_cost  = round(shares * np_, 2)

    # Minimum order size
    if yes_cost < 1.0 or no_cost < 1.0:
        shares = max(1.0 / yp, 1.0 / np_)
        yes_cost = round(shares * yp, 2)
        no_cost  = round(shares * np_, 2)
    return yes_cost, no_cost

def _size_marginal(marg, price, other_vol, pos=None, side="YES", yes_ask=0, no_ask=0):
    """
    Optimal ADDING size based on ROI improvement, not fixed tiers.
    
    Strategy:
    1. If position is UNBALANCED: buy light side to equalize shares (maximizes min payout).
    2. If position is BALANCED and spread is positive: add both sides proportionally.
    3. Size is capped by available capital and position limits.
    
    When pos is provided, calculates the exact optimal amount.
    Falls back to spread-proportional sizing when pos is not available.
    """
    if marg < 0.001: return 0.0

    # If we have full position info, calculate optimal size
    if pos is not None and pos.yes_total > 0 and pos.no_total > 0:
        yes_sh = pos.yes_total / pos.yes_avg_price if pos.yes_avg_price > 0 else 0
        no_sh  = pos.no_total  / pos.no_avg_price  if pos.no_avg_price  > 0 else 0
        cost   = pos.yes_total + pos.no_total

        if side == "YES" and yes_sh < no_sh:
            # Light side: buy up to balance point
            target_sh = no_sh - yes_sh
            optimal = target_sh * yes_ask if yes_ask > 0 else target_sh * price
            return max(1.0, round(optimal, 2))
        elif side == "NO" and no_sh < yes_sh:
            target_sh = yes_sh - no_sh
            optimal = target_sh * no_ask if no_ask > 0 else target_sh * price
            return max(1.0, round(optimal, 2))
        else:
            # Balanced or heavy side: size proportional to spread and position size
            spread_frac = max(marg, 0.005)
            raw = cost * spread_frac * 2.0  # scale relative to existing position
            return max(1.0, round(raw, 2))

    # Fallback: spread-proportional sizing (no position context)
    spread_frac = max(marg, 0.005)
    raw = MY_BANKROLL * spread_frac * 1.5
    # Balance cap: don't overshoot the other side
    if other_vol > 0:
        raw = min(raw, other_vol * 1.5)
    return max(1.0, round(raw, 2))

# ─────────────────────────────────────────────────────────────────────
# ORDER PLACEMENT
# ─────────────────────────────────────────────────────────────────────
def place_limit_order(token_id, price, size_usdc, label="", use_fak=False):
    """
    Place order on Polymarket CLOB.
    use_fak=True: market order (FAK - Fill-And-Kill), fastest execution, ~20ms.
                  Guarantees fill at cost of 1-2¢ slippage. Good for Phase 1.
    use_fak=False (default): limit GTC, sits in book. Good for arb completion.
    """
    if size_usdc < 1.00: return None
    if DRY_RUN:
        order_type = "FAK" if use_fak else "LIMIT"
        log.info(f"  [DRY-{order_type}] BUY ${size_usdc:.2f} @{price:.4f}  {label}")
        return f"dry_{token_id[:8]}_{int(time.time()*1000)}"
    try:
        from py_clob_client.order_builder.constants import BUY
        client = get_clob_client()
        if use_fak:
            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            args = MarketOrderArgs(token_id=token_id, amount=size_usdc, side=BUY)
            resp = client.post_order(client.create_market_order(args), OrderType.FAK)
        else:
            from py_clob_client.clob_types import LimitOrderArgs, OrderType
            args = LimitOrderArgs(token_id=token_id, price=price, size=round(size_usdc/price,2), side=BUY)
            resp = client.post_order(client.create_limit_order(args), OrderType.GTC)
        if resp and resp.get("success"):
            oid = resp.get("orderID","")
            tag = "FAK" if use_fak else "GTC"
            if use_fak:
                filled = resp.get("fillAmounts", resp.get("sizeMatched", "?"))
                log.info(f"  {tag} ${size_usdc:.2f}@{price:.4f} filled={filled} {label} [{oid[:12]}]")
            else:
                log.info(f"  {tag} ${size_usdc:.2f}@{price:.4f} {label} [{oid[:12]}]")
            stats.orders += 1
            return oid
        log.warning(f"  Order {'unfilled (no liquidity)' if use_fak else 'failed'}: {resp} {label}")
    except Exception as e:
        log.error(f"  Order err: {e} {label}")
    return None

def _check_roi_improvement(pos, yes_add: float, no_add: float,
                           yes_price: float, no_price: float,
                           label: str = "") -> bool:
    """
    UNIVERSAL ROI GATE: Does adding (yes_add, no_add) at (yes_price, no_price)
    improve the guaranteed worst-case P&L of this position?
    
    For ONE-SIDED positions: checks that completing the other side actually
    creates a profitable arb (combined avg < 1.0), not just any completion.
    
    For TWO-SIDED positions: requires worst-case P&L to improve by a meaningful
    amount relative to the capital spent.
    """
    if not pos or pos.total_deployed < 1.0:
        return True  # brand new — let signals handle

    # ── ONE-SIDED: validate the completion creates a real arb ──
    is_one_sided = (not pos.yes_fills or not pos.no_fills)
    if is_one_sided:
        # We hold one side. Adding the other should create guaranteed profit.
        if pos.yes_fills and no_add > 0:
            # We hold YES, adding NO
            seed_avg = pos.yes_avg_price
            opp_price = no_price
        elif pos.no_fills and yes_add > 0:
            # We hold NO, adding YES
            seed_avg = pos.no_avg_price
            opp_price = yes_price
        else:
            return True  # adding same side (Phase 1 topping up) — let it through

        combined = seed_avg + opp_price
        if combined >= 0.98:
            # No arb here — buying opposite side at this price just locks in a loss
            log.debug(f"  ROI GATE BLOCKED (1-sided): seed@{seed_avg:.3f} + opp@{opp_price:.3f} "
                      f"= {combined:.3f} >= 0.98 | {label[:50]}")
            return False
        # Real arb exists — let it through
        log.info(f"  ROI OK (completing): seed@{seed_avg:.3f} + opp@{opp_price:.3f} "
                 f"= {combined:.3f} | {label[:50]}")
        return True

    # ── TWO-SIDED: require meaningful improvement ──
    cost_now   = pos.yes_total + pos.no_total
    yes_sh_now = pos.yes_total / pos.yes_avg_price if pos.yes_avg_price > 0 else 0
    no_sh_now  = pos.no_total  / pos.no_avg_price  if pos.no_avg_price  > 0 else 0
    current_pl = min(yes_sh_now - cost_now, no_sh_now - cost_now)

    # Simulate combined addition
    new_yes_usdc = pos.yes_total + yes_add
    new_no_usdc  = pos.no_total  + no_add
    new_cost     = new_yes_usdc + new_no_usdc

    if yes_add > 0 and new_yes_usdc > 0:
        new_yes_avg = (pos.yes_avg_price * pos.yes_total + yes_price * yes_add) / new_yes_usdc
    else:
        new_yes_avg = pos.yes_avg_price
    if no_add > 0 and new_no_usdc > 0:
        new_no_avg = (pos.no_avg_price * pos.no_total + no_price * no_add) / new_no_usdc
    else:
        new_no_avg = pos.no_avg_price

    new_yes_sh = new_yes_usdc / new_yes_avg if new_yes_avg > 0 else 0
    new_no_sh  = new_no_usdc  / new_no_avg  if new_no_avg  > 0 else 0
    new_pl     = min(new_yes_sh - new_cost, new_no_sh - new_cost)

    improvement = new_pl - current_pl
    new_capital = yes_add + no_add

    # Must improve AND the improvement must be worth the capital spent
    # Require at least 15% return on the new capital
    min_improvement = new_capital * 0.15

    if new_pl <= current_pl:
        log.debug(f"  ROI GATE BLOCKED: ${current_pl:.2f} → ${new_pl:.2f} (no improvement) | {label[:50]}")
        return False
    if improvement < min_improvement:
        log.debug(f"  ROI GATE BLOCKED: +${improvement:.2f} improvement < ${min_improvement:.2f} min "
                  f"(15% of ${new_capital:.2f}) | {label[:50]}")
        return False

    log.info(f"  ROI OK: ${current_pl:.2f} → ${new_pl:.2f} (+${improvement:.2f}) | {label[:50]}")
    return True


def _extract_event(title: str) -> str:
    """Extract the root event from a market title.
    'Timberwolves vs. Nuggets: Spread: Nuggets (-3.5)' → 'timberwolves vs. nuggets'
    'Will FC Barcelona win on 2026-02-12?' → 'will fc barcelona win on 2026-02-12?'
    'Counter-Strike: Vitality vs Team Falcons (BO3) - BLAST' → 'counter-strike: vitality vs team falcons'
    """
    t = title.strip()
    # For "Team A vs. Team B: Sub-market" format, take before first colon
    # unless the colon is part of "Counter-Strike:" or "LoL:" etc
    game_prefixes = ('counter-strike:', 'cs2:', 'lol:', 'dota 2:', 'valorant:', 'league of legends:')
    tl = t.lower()
    
    # Strip game prefix if present, then split on colon
    prefix = ""
    for gp in game_prefixes:
        if tl.startswith(gp):
            prefix = t[:len(gp)] + " "
            t = t[len(gp):].strip()
            break
    
    # Take everything before the first colon (sub-market separator)
    if ':' in t:
        t = t.split(':')[0].strip()
    
    # Strip tournament suffixes like "(BO3) - BLAST Bounty..."
    for sep in [' - ', ' (BO', ' | ']:
        if sep in t:
            t = t.split(sep)[0].strip()
    
    return (prefix + t).lower().strip()


def _event_exposure(event_key: str, exclude_market_id: str = "") -> float:
    """Sum total deployed across all markets sharing the same event."""
    total = 0.0
    for mid, pos in ledger.positions.items():
        if pos.resolved or mid == exclude_market_id:
            continue
        if hasattr(pos, 'title'):
            pos_event = _extract_event(pos.title)
            if pos_event == event_key:
                total += pos.total_deployed
    return total


def _fill(market_id, side, price, size, token, pos, label):
    global _startup_trade_count
    if size < 1.00:
        stats.rej_size += 1
        return
    # ═══ GLOBAL RATE LIMITER ═══
    is_new = (pos is None) or (pos.total_deployed < 1.0)
    if not _global_rate_ok(is_new_position=is_new):
        stats.rej_rate += 1
        return
    # ═══ TOTAL DEPLOYMENT CAP ═══
    max_deploy = MY_BANKROLL * MAX_DEPLOY_PCT
    current_deploy = sum(p.total_deployed for p in ledger.positions.values()
                         if not p.resolved)
    if current_deploy + size > max_deploy:
        remaining = max_deploy - current_deploy
        if remaining < 1.0:
            stats.rej_deploy += 1
            return  # portfolio full
        size = round(remaining, 2)
    # ═══ EVENT CONCENTRATION CAP ═══
    event_title = ""
    existing_pos = ledger.positions.get(market_id)
    if existing_pos and hasattr(existing_pos, 'title'):
        event_title = existing_pos.title
    elif "|" in label:
        event_title = label.split("|")[-1].strip()
    event_key = _extract_event(event_title)
    if event_key and len(event_key) > 3:
        max_event = MY_BANKROLL * MAX_PER_EVENT_PCT
        event_deployed = _event_exposure(event_key, exclude_market_id=market_id)
        if existing_pos and not existing_pos.resolved:
            event_deployed += existing_pos.total_deployed
        if event_deployed + size > max_event:
            remaining_event = max_event - event_deployed
            if remaining_event < 1.0:
                stats.rej_event += 1
                return
            size = round(remaining_event, 2)
            log.info(f"  EVENT CAP: '{event_key}' ${event_deployed:.0f}/${max_event:.0f} — trimmed to ${size:.2f}")
    # ═══ NO-ONLY SAFETY NET ═══
    if NO_ONLY_MODE and side == "YES":
        log.warning(f"  NO-ONLY BLOCK: attempted YES buy blocked | {label[:40]}")
        return
    # ═══ NEW POSITION CAP ═══
    # RN1 actual: big first entries are common. p90=$215, p99=$1220 at $10K bankroll
    # Early trades (first 100): up to 17% of cumulative volume in single trade
    # Mature (trade 500+): 1-3% of volume per trade but occasional 5-13% conviction bets
    MAX_NEW_FILL = max(MY_BANKROLL * 0.02, 8.0)  # 2% cap for first entry; RN1 median $9 at startup, then accumulated
    if is_new and size > MAX_NEW_FILL:
        log.info(f"  NEW-POS CAP: ${size:.2f} → ${MAX_NEW_FILL:.2f} (first entry) | {label[:40]}")
        size = MAX_NEW_FILL
    # ═══ BUG PROTECTION SAFETY CAP ═══
    MAX_SINGLE_TRADE = MY_BANKROLL * 0.10  # 10% absolute max per trade
    if size > MAX_SINGLE_TRADE:
        log.warning(f"  SAFETY CAP: ${size:.2f} → ${MAX_SINGLE_TRADE:.2f} | {label[:40]}")
        size = round(MAX_SINGLE_TRADE, 2)
    # Skip if we already attempted this exact price — retry only when price moves
    attempt_key = (market_id, side, round(price, 2))
    if not DRY_RUN and attempt_key in _attempted_fills:
        return
    if DRY_RUN:
        place_limit_order(token, price, size, label, use_fak=False)
        ledger.record_fill(market_id, side, price, size)
        stats.fills += 1
        _startup_trade_count += 1
        _global_fill_times.append(time.time())
        if is_new: _global_new_pos_times.append(time.time())
    else:
        # Live mode: use FAK (market order) for guaranteed immediate fill.
        # Records fill optimistically at asked price; actual fill may be
        # 1-2¢ worse due to slippage but position tracking stays accurate.
        oid = place_limit_order(token, price, size, label, use_fak=True)
        if oid:
            ledger.record_fill(market_id, side, price, size)
            stats.fills += 1
            _startup_trade_count += 1
            _global_fill_times.append(time.time())
            if is_new: _global_new_pos_times.append(time.time())
            _attempted_fills.discard(attempt_key)  # success — reset so we can add more
            log.info(f"  LIVE FILL recorded: {side} ${size:.2f}@{price:.4f} {label}")
        else:
            _attempted_fills.add(attempt_key)  # mark tried — skip until price moves

# ─────────────────────────────────────────────────────────────────────
# SIGNAL EVALUATION
# ─────────────────────────────────────────────────────────────────────
def evaluate_market(market_id: str):
    """
    Called on every WS price event or position-watcher REST poll.
    Per-market mutex prevents concurrent evaluation from WS + REST threads.
    """
    # Non-blocking: if this market is already being evaluated, skip.
    lock = _eval_locks[market_id]
    if not lock.acquire(blocking=False):
        return
    try:
        _evaluate_market_inner(market_id)
    finally:
        lock.release()


def _evaluate_market_inner(market_id: str):
    now = time.time()
    info = market_registry.get(market_id)
    if not info: return

    yes_tok, no_tok = info["yes_token"], info["no_token"]
    title           = info["title"]
    ys, ns          = token_states[yes_tok], token_states[no_tok]
    yes_ask, no_ask = ys.ask, ns.ask

    if yes_ask <= 0.005 and no_ask <= 0.005: return  # both dead — truly no market
    if yes_ask >= 0.999 and no_ask >= 0.999: return
    _pos_check = ledger.positions.get(market_id)
    _have_pos  = bool(_pos_check and (_pos_check.yes_fills or _pos_check.no_fills))
    if not _have_pos and now - max(ys.last_ts, ns.last_ts) > 60: return  # staleness only for new entries

    pos     = ledger.get_or_create(market_id, title, yes_tok, no_tok)

    has_yes = bool(pos.yes_fills)
    has_no  = bool(pos.no_fills)
    we_have_position = has_yes or has_no

    # ── RESOLUTION DETECTION ─────────────────────────────────────────────
    # Resolution is handled by _check_resolutions_via_api() which queries the
    # Gamma API every 60s. Price-based detection removed — during live games
    # prices hit 0.98/0.02 on swings without the market being settled.


    # ── FOCUS GATE: don't enter new markets until existing positions resolve ──
    # With 100+ open positions, priority is completing existing arbs, not opening new ones.
    # NO exceptions — even "guaranteed arbs" don't bypass this when we're over the cap.
    if not we_have_position and ledger.active_count() >= MAX_ACTIVE_MARKETS:
        return

    # Player props are allowed — sizing controls prevent oversized bets.
    # Seeds are $2, dutch entries require spread > MIN_SPREAD_PCT.

    # ── FUTURES/SEASON-LONG GATE: block markets that resolve far in the future ──
    if not we_have_position:
        _futures_kws = ["mvp", "champion", "championship", "win the world",
                        "world series", "super bowl", "stanley cup",
                        "nba finals", "world cup", "season wins",
                        "make the playoffs", "2027", "2028",
                        "win the 2026", "ballon d'or"]
        _title_lower = title.lower()
        if any(kw in _title_lower for kw in _futures_kws):
            return  # silently skip futures/season-long markets

        # End-date filter: skip markets resolving more than 3 days from now
        end_dt = info.get("end_dt")
        if end_dt:
            try:
                from datetime import datetime, timezone
                if isinstance(end_dt, str):
                    end_dt = datetime.fromisoformat(end_dt.replace("Z", "+00:00"))
                now_dt = datetime.now(timezone.utc)
                days_until = (end_dt - now_dt).total_seconds() / 86400
                if days_until > 3.0:
                    return  # too far out — skip
            except: pass

    # ── VOLUME GATE: skip illiquid markets for new entries ───────────────
    # Low volume = thin order book = spread may be illusory, fills won't hold.
    # Existing positions always bypass this — we never abandon a live position.
    if not we_have_position:
        vol = info.get("volume", 0) + info.get("liquidity", 0)
        if vol < MIN_VOLUME:
            log.info(f"VOL SKIP: {title[:50]} (vol=${vol:.0f} < ${MIN_VOLUME:.0f})")
            return

    # ── PER-EVENT CAP: max 2 seeds per same event ────────────────────────
    # Prevents the bot from flooding one game with 12+ sub-market positions.
    # Existing positions (both sides filled) don't count — only seeds.
    if not we_have_position:
        event_title = info.get("event_title", "")
        if event_title:
            event_seed_count = sum(
                1 for p in ledger.positions.values()
                if p.title.startswith(event_title[:20])
                and bool(p.yes_fills) != bool(p.no_fills)  # one-sided only
                and not p.stagnant
            )
            if event_seed_count >= MAX_PER_EVENT:
                return  # already have enough seeds on this event

    # ── ENDGAME: manage existing NO positions ────────────────────────
    # Only fires if we already hold a position. Checked first because
    # near-zero prices are time-sensitive.
    if we_have_position:
        endgame_fired = _sig_endgame(market_id, title, yes_tok, no_tok,
                        yes_ask, no_ask, pos, has_yes, has_no, now)
        if endgame_fired:
            if DRY_RUN:
                log.info(f"   POS: YES${pos.yes_total:.0f}@{pos.yes_avg_price:.3f} | "
                         f"NO${pos.no_total:.0f}@{pos.no_avg_price:.3f} | "
                         f"Spread:{pos.implied_spread*100:.1f}%")
            return  # never combine endgame + other signals in same cycle

    # ── CROSS-MARKET DUTCH: NO on both team-win markets (draw pays both) ─
    if not we_have_position:
        _sig_cross_dutch(market_id, now)

    # ── SELL CHECK: cut losing NO positions (his 2.1% sell rate) ─────
    if we_have_position and has_no and SELL_ENABLED:
        if _sig_sell_loser(market_id, title, no_tok, no_ask, pos, now):
            return

    # ── ADDING: add to existing NO positions when price improves ─────
    if we_have_position and has_no:
        if _sig_adding_no(market_id, title, no_tok, no_ask, pos, now):
            return

    # ── For new entries: all signals below are NO-ONLY ─────────────────
    if we_have_position:
        return  # already in this market — endgame/adding handles it

    # Hourly gate: adjust cooldowns based on RN1's hourly pattern
    hw = _get_hourly_weight()

    # ── SPIKE / OBI: DISABLED — OB baseline analysis proved these are noise ──
    # OBI > 0.6 = 55.5% of ALL markets at any time (not selective)
    # Spikes > 2% = 0.01% of 5s windows (almost never happen)
    # These were eating rate limit budget with non-signals.
    # RN1's strategy is coverage-based, not signal-based.

    # ── RN1-STYLE DIRECTIONAL NO SIGNALS (by price range) ────────────
    # Order matches his volume distribution: NC > Favored > Core Mid > Penny
    
    # Near-certain: buy NO at 0.80-0.97
    if _sig_near_certain(market_id, title, yes_tok, no_tok,
                         yes_ask, no_ask, pos, now):
        return

    # Favored side: buy NO at 0.60-0.80
    if _sig_favored_side(market_id, title, yes_tok, no_tok,
                         yes_ask, no_ask, pos, now):
        return

    # Core mid: buy NO at 0.15-0.60 (his biggest bucket — 57% of trades)
    if _sig_core_mid(market_id, title, yes_tok, no_tok,
                     yes_ask, no_ask, pos, now):
        return

    # Penny scoop: buy NO at 0.01-0.10
    _sig_penny_scoop(market_id, title, yes_tok, no_tok,
                     yes_ask, no_ask, pos, now)


def _sig_cross_dutch(triggered_market_id: str, now: float):
    """
    Cross-market Dutch book: buy NO on both team-win markets in the same match.
    Profits regardless of winner — draw pays BOTH NOs.

    Example:
      "Will Al Fayha win?"  NO @ 0.42
      "Will Al Nassr win?"  NO @ 0.45
      Total: 0.87 → 13% spread. Al Fayha wins → NO(Nassr) pays. Al Nassr wins → NO(Fayha) pays.
      DRAW → BOTH pay. Best outcome is the most common football result.
    """
    # Group all markets by event_title
    by_event: Dict[str, list] = defaultdict(list)
    with _registry_lock:
        for mid, info in market_registry.items():
            by_event[info["event_title"]].append((mid, info))

    # Find the event this triggered market belongs to
    triggered_event = None
    with _registry_lock:
        if triggered_market_id in market_registry:
            triggered_event = market_registry[triggered_market_id]["event_title"]
    if not triggered_event:
        return False

    # Respect the active market cap — don't open new positions when over limit
    if ledger.active_count() >= MAX_ACTIVE_MARKETS:
        return False

    markets_in_event = by_event.get(triggered_event, [])
    if len(markets_in_event) < 2:
        return False

    # Cooldown: don't spam this event (10s between cross-dutch checks)
    if now - _last_cross_dutch[triggered_event] < 10.0:
        return False

    # Find all "win" markets — title contains "Will X win" or "Will X S" pattern
    # Collect NO prices for each
    candidates = []
    for mid, info in markets_in_event:
        title = info["title"]
        no_tok = info["no_token"]
        ns = token_states.get(no_tok)
        if ns is None:
            ns = token_states[no_tok]
        no_ask = ns.ask
        if no_ask <= 0:
            continue
        # Only consider mid-range NOs (team has real chance of winning)
        if not (0.25 <= no_ask <= 0.80):
            continue
        # Skip if we already have a position in this market
        pos = ledger.get(mid)
        if pos and pos.total_deployed > 0:
            continue
        candidates.append((mid, info, no_tok, no_ask))

    if len(candidates) < 2:
        return False

    # Find the best NO+NO pair (lowest sum = best spread)
    best_pair = None
    best_sum = 1.0  # must be under 1.0 to be profitable

    for i in range(len(candidates)):
        for j in range(i+1, len(candidates)):
            a_mid, a_info, a_tok, a_no = candidates[i]
            b_mid, b_info, b_tok, b_no = candidates[j]
            total = a_no + b_no
            if total < best_sum:
                best_sum = total
                best_pair = (candidates[i], candidates[j])

    if best_pair is None:
        return False

    (a_mid, a_info, a_tok, a_no), (b_mid, b_info, b_tok, b_no) = best_pair
    spread_pct = (1.0 - best_sum) * 100

    if spread_pct < MIN_SPREAD_PCT:
        return False

    available = MY_BANKROLL
    deploy_frac = min(0.40, 2.0 * (spread_pct / 100.0) ** 0.75)
    sz = max(1.0, round(available * deploy_frac / 2, 2))

    log.info(f"CROSS-DUTCH: {triggered_event[:50]}\n"
             f"   NO({a_info['title'][:35]})@{a_no:.3f}\n"
             f"   NO({b_info['title'][:35]})@{b_no:.3f}\n"
             f"   Sum:{best_sum:.3f} Spread:{spread_pct:.1f}% Size:${sz:.2f} each\n"
             f"   Draw pays BOTH. Any winner pays one NO.")

    _last_cross_dutch[triggered_event] = now
    stats.dutch += 1

    a_pos = ledger.get(a_mid)
    b_pos = ledger.get(b_mid)
    _fill(a_mid, "NO", a_no, sz, a_tok, a_pos, f"XDUTCH-NO|{a_info['title'][:35]}")
    _fill(b_mid, "NO", b_no, sz, b_tok, b_pos, f"XDUTCH-NO|{b_info['title'][:35]}")
    return True


def _sig_adding_no(market_id, title, no_tok, no_ask, pos, now):
    """Add to existing NO position. RN1 REAL DATA (deep analysis):
    - First 1K buys: 90 markets, avg 11 buys/market, max 93 in one market
    - Only 21% single-buy, 79% get multiple buys
    - He ACCUMULATES into winners — buys more as NO price rises (odds moving his way)
    - Median gap between ALL trades: 38 seconds
    Only adds if: position exists, cooldown passed, still looks good, under cap."""
    if not pos.no_fills:
        return False
    
    # Position must be at least 2 minutes old before first add
    pos_age_min = (now - pos.created_at) / 60.0
    if pos_age_min < 2.0:
        return False
    
    # Cooldown between adds: 60 seconds (his median gap was 38s)
    p2_key = market_id + "_adding"
    if now - _last_p2.get(p2_key, 0) < 60.0:
        return False
    
    # Cap by DOLLARS not count — at $1K bankroll, per-market cap handles this
    if pos.total_deployed >= MAX_POSITION_PER_MARKET:
        return False
    
    # Price check — he adds in TWO scenarios:
    # 1. Price moved IN OUR FAVOR (NO got more expensive = market moving toward NO win)
    #    e.g. bought NO@0.50, now NO@0.70 → YES dropped 0.50→0.30. Keep buying!
    # 2. Price stayed roughly the same (within 15% of entry) — steady accumulation
    # BLOCK only if price CRASHED (NO went way down = we're losing)
    price_ratio = no_ask / pos.no_avg_price if pos.no_avg_price > 0 else 1.0
    if price_ratio < 0.50:  # NO price collapsed >50% — we're losing badly, stop adding
        return False
    
    # Size: use normal price-size curve, full size (he didn't shrink re-buys)
    sz = _rn1_size_for_price(no_ask)
    sz = min(sz, MAX_TRADE_SIZE, MAX_POSITION_PER_MARKET - pos.total_deployed)
    if sz < 1.0:
        return False
    
    direction = "↑WINNING" if price_ratio >= 1.05 else ("→FLAT" if price_ratio >= 0.95 else "↓DIP")
    
    _last_p2[p2_key] = now
    log.info(f"ADDING NO: {title[:60]}\n"
             f"   NO@{no_ask:.3f} (entry avg@{pos.no_avg_price:.3f}, buy #{len(pos.no_fills)+1}) "
             f"${sz:.2f} {direction} | Total:${pos.no_total+sz:.0f}")
    stats.dutch += 1
    _fill(market_id, "NO", no_ask, sz, no_tok, pos, f"ADD-NO|{title[:35]}")
    return True


def _sig_dutch(market_id, title, yes_tok, no_tok, yes_ask, no_ask, ys, ns, pos, has_yes, has_no, now):
    """LEGACY: kept for backward compat but NO-ONLY mode routes around this.
    In NO-ONLY mode, same-market arbs are not used. Cross-Dutch handles multi-market NO+NO."""
    if NO_ONLY_MODE:
        return False

    # Phase 2 / Ongoing — DISABLED in NO-ONLY mode
    return False


def _sig_endgame(market_id, title, yes_tok, no_tok, yes_ask, no_ask,
                 pos, has_yes, has_no, now):
    """
    NO-ONLY Endgame management for markets we're already in.

    HAMMER NO: Our NO position is winning (NO price high). Add more aggressively.
    This is the only endgame action in NO-ONLY mode — we never buy YES.
    
    RN1 holds to resolution. No selling, no hedging with opposite side.
    """
    fired = False
    
    no_winning  = no_ask >= HAMMER_PRICE
    
    # ── HAMMER NO: add to our winning NO position ───────────────────
    HAMMER_COOLDOWN = 300.0  # 5 min between hammers (RN1 re-buys aggressively)
    
    if no_winning and has_no:
        hammer_key = market_id + "_no"
        if now - _last_hammer.get(hammer_key, 0) > HAMMER_COOLDOWN:
            # Only hammer if we already have a decent NO position
            if pos.no_total >= 5.0:
                # Size: same as normal, no 1.5x multiplier. Cap tighter.
                hammer_cap = MY_BANKROLL * 0.03  # max 3% bankroll per hammer
                sz = min(_rn1_size_for_price(no_ask), hammer_cap,
                         MAX_POSITION_PER_MARKET - pos.total_deployed)
                if sz >= 1.0:
                    log.info(f"HAMMER NO: {title[:60]}\n"
                             f"   NO@{no_ask:.3f} ${sz:.2f} (winning, adding) | "
                             f"Position: ${pos.no_total:.0f}@{pos.no_avg_price:.3f}")
                    _fill(market_id, "NO", no_ask, sz, no_tok, pos, f"HAMMER-NO|{title[:35]}")
                    _last_hammer[hammer_key] = now
                    stats.dutch += 1
                    fired = True

    # ── STUCK POSITION: free slot after 10min, keep watching ──────
    # NO-ONLY mode: if our NO position hasn't moved meaningfully after 10 min,
    # free the cap slot so new entries aren't blocked.
    SEED_FREE_MINS = 10.0
    if has_no and not has_yes and not pos.stagnant:
        age_mins = (now - pos.created_at) / 60.0
        if age_mins >= SEED_FREE_MINS:
            pos.stagnant = True
            log.info(f"SLOT FREED: {title[:60]}\n"
                     f"   NO@{pos.no_avg_price:.3f} ${pos.no_total:.2f} "
                     f"— {age_mins:.0f}min old, slot freed, holding to resolution")

    return fired


def _sig_spike(market_id, title, yes_tok, no_tok, yes_ask, no_ask, ys, ns, pos):
    """
    NO-ONLY SPIKE: price moved >SPIKE_PCT_THRESH in SPIKE_WINDOW_SECS.
    Only buys NO token. Two scenarios:
    1. NO price dropped (mean-revert buy) → buy NO at discount
    2. NO price rose (momentum) → buy NO riding momentum
    """
    now = time.time()
    if not ys.history or not ns.history: return False
    if min(now - ys.first_ts, now - ns.first_ts) < 30.0:
        return False

    nv = ns.price_velocity(SPIKE_WINDOW_SECS)
    if abs(nv) < SPIKE_PCT_THRESH:
        return False

    price = no_ask
    tok = no_tok
    
    if nv < 0:
        # NO price dropped → mean-revert buy (discount entry)
        reason = f"NO mean-revert -{abs(nv)*100:.1f}%/{SPIKE_WINDOW_SECS}s"
    else:
        # NO price rose → momentum buy
        if price > 0.80:
            return False  # too expensive for momentum chase
        reason = f"NO momentum +{nv*100:.1f}%/{SPIKE_WINDOW_SECS}s"

    if price < 0.05 or price > 0.95: return False

    sz = min(_rn1_size_for_price(price), MAX_TRADE_SIZE)
    
    log.info(f"SPIKE: {title[:60]}\n   {reason} -> Buy NO@{price:.3f} ${sz:.2f}")
    stats.spike += 1
    _fill(market_id, "NO", price, sz, tok, pos, f"SPK-NO|{title[:35]}")
    return True


def _sig_obi(market_id, title, yes_tok, no_tok, yes_ask, no_ask, ys, ns, pos):
    """
    NO-ONLY ORDER_BOOK_IMBALANCE: bid_size >> ask_size on NO side.
    Only buys NO when there's buy pressure on the NO book.
    """
    ni = ns.ob_imbalance()
    if ni < OB_IMBALANCE_MIN: return False  # only buy NO when NO has buy pressure
    if ns.spread() > OB_MAX_SPREAD: return False
    
    price = no_ask
    tok = no_tok
    if price < 0.05 or price > 0.95: return False

    sz = min(_rn1_size_for_price(price), MAX_TRADE_SIZE)
    
    log.info(f"OB IMBALANCE: {title[:60]}\n   NO imb={ni:.2f} spread={ns.spread():.3f} -> Buy NO@{price:.3f} ${sz:.2f}")
    stats.obi += 1
    _fill(market_id, "NO", price, sz, tok, pos, f"OBI-NO|{title[:35]}")
    return True


# ── RN1-STYLE: NEAR-CERTAIN HARVESTING ──────────────────────────────
def _sig_near_certain(market_id, title, yes_tok, no_tok, yes_ask, no_ask, pos, now):
    """
    NO-ONLY: Buy NO at 0.80-0.97.
    RN1's data: 14% of all trades in this range, all NO tokens.
    At $1K startup this was 11.7% of volume allocation.
    """
    nc_cap = MY_BANKROLL * NC_MAX_EXPOSURE
    current_nc = ledger.directional_exposure()
    if current_nc >= nc_cap:
        return False

    # NO-ONLY: only check NO price
    if not (NC_MIN_PRICE <= no_ask <= NC_MAX_PRICE):
        return False

    price = no_ask
    tok = no_tok

    # Hourly-adjusted cooldown
    hw = _get_hourly_weight()
    adjusted_cooldown = NC_COOLDOWN / max(hw, 0.2)
    nc_key = market_id + "_nc"
    if now - _last_nc.get(nc_key, 0) < adjusted_cooldown:
        return False

    info = market_registry.get(market_id)
    if not info:
        return False
    vol = info.get("volume", 0) + info.get("liquidity", 0)
    if vol < NC_MIN_VOLUME:
        return False

    if pos.yes_fills or pos.no_fills:
        return False

    sz = min(_rn1_size_for_price(price), nc_cap - current_nc, MY_BANKROLL * 0.02)
    if sz < 1.0:
        return False

    expected_return_pct = (1.0 / price - 1.0) * 100
    _last_nc[nc_key] = now
    log.info(f"NEAR-CERTAIN: {title[:60]}\n"
             f"   NO@{price:.3f} ${sz:.2f} | +{expected_return_pct:.1f}% expected | "
             f"NC: ${current_nc+sz:.0f}/${nc_cap:.0f}")
    stats.near_certain += 1
    _fill(market_id, "NO", price, sz, tok, pos, f"NC-NO|{title[:35]}")
    return True


# ── RN1-STYLE: PENNY SCOOP ──────────────────────────────────────────
def _sig_penny_scoop(market_id, title, yes_tok, no_tok, yes_ask, no_ask, pos, now):
    """
    NO-ONLY: Buy NO at 0.01-0.10.
    RN1's data: 7.5% of trades, avg size $3-18 depending on bankroll.
    Cheap lottery tickets — NO at $0.05 pays $1 if event happens as expected.
    """
    penny_cap = MY_BANKROLL * PENNY_MAX_TOTAL
    current_penny = ledger.penny_exposure()
    if current_penny >= penny_cap:
        return False

    # NO-ONLY: only check NO price
    if not (0.005 < no_ask <= PENNY_MAX_PRICE):
        return False

    price = no_ask
    tok = no_tok

    # Hourly-adjusted cooldown
    hw = _get_hourly_weight()
    adjusted_cooldown = PENNY_COOLDOWN / max(hw, 0.2)
    penny_key = market_id + "_penny"
    if now - _last_penny.get(penny_key, 0) < adjusted_cooldown:
        return False

    info = market_registry.get(market_id)
    if not info:
        return False
    vol = info.get("volume", 0) + info.get("liquidity", 0)
    if vol < NC_MIN_VOLUME:
        return False

    if pos.yes_fills or pos.no_fills:
        return False

    sz = min(_rn1_size_for_price(price), penny_cap - current_penny)
    if sz < 0.50:
        return False

    potential_shares = sz / price
    _last_penny[penny_key] = now
    log.info(f"PENNY SCOOP: {title[:60]}\n"
             f"   NO@{price:.3f} ${sz:.2f} -> {potential_shares:.0f} shares (${potential_shares:.0f} if wins) | "
             f"Penny: ${current_penny+sz:.0f}/${penny_cap:.0f}")
    stats.penny += 1
    _fill(market_id, "NO", price, sz, tok, pos, f"PENNY-NO|{title[:35]}")
    return True


# ── RN1-STYLE: FAVORED SIDE (0.60-0.80) ─────────────────────────────
def _sig_favored_side(market_id, title, yes_tok, no_tok, yes_ask, no_ask, pos, now):
    """
    NO-ONLY: Buy NO at 0.60-0.80.
    RN1's 2nd largest bucket: 21% of trades, $25.8M volume, ALL NO tokens.
    Expected return: 25-67% per dollar if event doesn't happen.
    """
    fav_cap = MY_BANKROLL * FAV_MAX_EXPOSURE
    current_fav = ledger.favored_exposure()
    if current_fav >= fav_cap:
        return False

    # NO-ONLY: only check NO price
    if not (FAV_MIN_PRICE <= no_ask <= FAV_MAX_PRICE):
        return False

    price = no_ask
    tok = no_tok

    # Hourly-adjusted cooldown
    hw = _get_hourly_weight()
    adjusted_cooldown = FAV_COOLDOWN / max(hw, 0.2)
    fav_key = market_id + "_fav"
    if now - _last_fav.get(fav_key, 0) < adjusted_cooldown:
        return False

    info = market_registry.get(market_id)
    if not info:
        return False
    vol = info.get("volume", 0) + info.get("liquidity", 0)
    if vol < FAV_MIN_VOLUME:
        return False

    if pos.yes_fills or pos.no_fills:
        return False

    sz = min(_rn1_size_for_price(price), fav_cap - current_fav, MY_BANKROLL * 0.02)
    if sz < 1.0:
        return False

    expected_return_pct = (1.0 / price - 1.0) * 100
    _last_fav[fav_key] = now
    log.info(f"FAVORED SIDE: {title[:60]}\n"
             f"   NO@{price:.3f} ${sz:.2f} | +{expected_return_pct:.1f}% if wins | "
             f"Fav: ${current_fav+sz:.0f}/${fav_cap:.0f}")
    stats.favored += 1
    _fill(market_id, "NO", price, sz, tok, pos, f"FAV-NO|{title[:35]}")
    return True


# ── RN1-STYLE: CORE MID (0.15-0.60) ────────────────────────────────
def _sig_core_mid(market_id, title, yes_tok, no_tok, yes_ask, no_ask, pos, now):
    """
    NO-ONLY: Buy NO at 0.15-0.60.
    RN1's BIGGEST bucket: 57% of all trades, ~$44M volume, ALL NO tokens.
    This is his core activity — directional NO buying in the mid-range.
    
    At startup ($853): 42.3% of volume was mid-range + 19.2% low-range = 61.5%.
    This signal covers what was previously split between Phase 1 seeds and 
    the low range. Now unified as one continuous NO-buying strategy.
    
    Sizing uses the 22-bucket percentile data from his actual trade distribution:
      0.15-0.20: avg $32, 0.20-0.25: avg $40, 0.25-0.30: avg $49
      0.30-0.35: avg $58, 0.35-0.40: avg $72, 0.40-0.45: avg $82
      0.45-0.50: avg $105, 0.50-0.55: avg $102, 0.55-0.60: avg $111
    Pattern: size scales UP with price (higher conviction at higher prices).
    """
    mid_cap = MY_BANKROLL * MID_MAX_EXPOSURE
    current_mid = ledger.mid_exposure()
    if current_mid >= mid_cap:
        return False

    # NO-ONLY: only check NO price
    if not (MID_MIN_PRICE <= no_ask <= MID_MAX_PRICE):
        return False

    price = no_ask
    tok = no_tok

    # Hourly-adjusted cooldown
    hw = _get_hourly_weight()
    adjusted_cooldown = MID_COOLDOWN / max(hw, 0.2)
    mid_key = market_id + "_mid"
    if now - _last_mid.get(mid_key, 0) < adjusted_cooldown:
        return False

    info = market_registry.get(market_id)
    if not info:
        return False
    vol = info.get("volume", 0) + info.get("liquidity", 0)
    if vol < MID_MIN_VOLUME:
        return False

    if pos.yes_fills or pos.no_fills:
        return False

    # RN1 sizing: uses his actual 22-bucket price-size curve
    # Check for conviction opportunity (high OBI = market agrees with our direction)
    is_conviction = False
    state = market_registry.get(market_id, {}).get("_state")
    if state:
        no_state = state.get("no") 
        if no_state and hasattr(no_state, 'ob_imbalance') and no_state.ob_imbalance() > CONVICTION_OBI_THRESH:
            conv_key = market_id + "_conv"
            if now - _last_conviction.get(conv_key, 0) > CONVICTION_COOLDOWN:
                is_conviction = True
                _last_conviction[conv_key] = now
    
    sz = _rn1_size_for_price(price, conviction=is_conviction)
    # RN1 actual: p50=$12, but p99=$1220 at ~$10K bankroll. Don't cap too tight.
    sz = min(sz, mid_cap - current_mid, MY_BANKROLL * 0.02)
    sz = max(1.0, round(sz, 2))
    if sz < 1.0:
        return False

    expected_return_pct = (1.0 / price - 1.0) * 100
    _last_mid[mid_key] = now
    log.info(f"CORE MID: {title[:60]}\n"
             f"   NO@{price:.3f} ${sz:.2f} | +{expected_return_pct:.1f}% if wins | "
             f"Mid: ${current_mid+sz:.0f}/${mid_cap:.0f}")
    stats.mid += 1
    _fill(market_id, "NO", price, sz, tok, pos, f"MID-NO|{title[:35]}")
    return True


def _sig_sell_loser(market_id, title, no_tok, no_ask, pos, now):
    """Batch cleanup of NO positions. RN1 deep analysis:
    - 0.68% sell rate (21,862 sells across 1M+ trades)
    - ALL NO sells (never sold YES because never bought YES)
    - Median sell price: $0.21, p10: $0.095, p90: $0.68
    - Sells losers (NO collapsed) and occasionally winners (NO near $1)
    - Total sell revenue: $17.6M (meaningful recovery)."""
    if not pos.no_fills: return False
    age_hr = (now - pos.created_at) / 3600.0
    if age_hr < SELL_MIN_AGE_HR: return False
    
    # Check if it's cleanup time (every BATCH_CLEANUP_INTERVAL_HR hours)
    cleanup_key = "_batch_cleanup_last"
    last_cleanup = _last_p2.get(cleanup_key, 0)
    if now - last_cleanup < BATCH_CLEANUP_INTERVAL_HR * 3600:
        return False  # not cleanup time yet

    sell_reason = None
    
    # Category 1: LOSER — NO value has collapsed (YES winning)
    # RN1 sells at 0.07-0.18 (his NO is nearly worthless)
    if no_ask <= SELL_LOSER_THRESH and no_ask > 0.005:
        drop_pct = (pos.no_avg_price - no_ask) / pos.no_avg_price if pos.no_avg_price > 0 else 0
        if drop_pct > 0.50:  # dropped 50%+ from entry
            sell_reason = f"LOSER (entry@{pos.no_avg_price:.3f} now@{no_ask:.3f} -{drop_pct*100:.0f}%)"
    
    # Category 2: WINNER — NO value near $1 (can take profit before resolution)
    # RN1 sells at 0.95-1.00 (his NO is nearly at max value)  
    if no_ask >= SELL_WINNER_THRESH:
        gain_pct = (no_ask - pos.no_avg_price) / pos.no_avg_price if pos.no_avg_price > 0 else 0
        if gain_pct > 0.10:  # at least 10% gain
            sell_reason = f"WINNER (entry@{pos.no_avg_price:.3f} now@{no_ask:.3f} +{gain_pct*100:.0f}%)"
    
    # Category 3: STALE — position old enough to be from a resolved/stale event
    if age_hr > SELL_STALE_DAYS * 24 and pos.stagnant:
        sell_reason = f"STALE ({age_hr/24:.0f}d old, stagnant)"
    
    if not sell_reason:
        return False

    sell_key = market_id + "_sell"
    if now - _last_p2.get(sell_key, 0) < 300: return False  # once per 5 min per market

    _last_p2[sell_key] = now
    _last_p2[cleanup_key] = now  # reset batch timer
    
    log.info(f"BATCH SELL: {title[:60]}\n"
             f"   {sell_reason} | deployed=${pos.no_total:.2f}")
    stats.sells += 1

    if not DRY_RUN:
        try:
            shares_held = pos.no_total / pos.no_avg_price if pos.no_avg_price > 0 else 0
            sell_size = round(shares_held * no_ask, 2)
            place_limit_order(no_tok, no_ask, sell_size, f"SELL-NO|{title[:35]}", use_fak=True)
        except Exception as e:
            log.warning(f"Sell order failed: {e}")

    pos.resolved = True  # mark as done, free the slot
    return True


# ─────────────────────────────────────────────────────────────────────
# WEBSOCKET FEED
# ─────────────────────────────────────────────────────────────────────
def _handle_ws_event(data: dict):
    et = data.get("event_type")
    if et not in ("price_change","last_trade_price"): return
    stats.ws_events += 1
    affected = set()

    if et == "price_change":
        for c in data.get("price_changes",[]):
            aid = c.get("asset_id")
            if not aid: continue
            mid = token_to_market.get(aid)
            if not mid: continue
            try:
                price    = float(c.get("price") or 0)
                bid      = float(c.get("best_bid") or 0)
                ask      = float(c.get("best_ask") or 0)
                bid_size = float(c.get("best_bid_size") or 0)
                ask_size = float(c.get("best_ask_size") or 0)
            except: continue
            if ask == 0 and price > 0: ask = price
            if bid == 0 and price > 0: bid = price * 0.99
            token_states[aid].update(price=price,bid=bid,ask=ask,
                                     bid_size=bid_size,ask_size=ask_size)
            affected.add(mid)
    elif et == "last_trade_price":
        aid = data.get("asset_id")
        mid = token_to_market.get(aid) if aid else None
        if mid:
            try: token_states[aid].update(price=float(data.get("price") or 0))
            except: pass
            affected.add(mid)

    for mid in affected:
        try: evaluate_market(mid)
        except Exception as e: log.warning(f"eval err [{mid[:12]}]: {e}")


def _handle_ws_message(raw: str):
    try:
        payload = json.loads(raw)
    except: return
    if isinstance(payload, list):
        for item in payload: _handle_ws_event(item)
    elif isinstance(payload, dict):
        _handle_ws_event(payload)


def _run_websocket_loop() -> bool:
    """
    Start WebSocket feed. Tries websocket-client (sync, simpler) first,
    then falls back to websockets (async). Both are equivalent functionally.
    Install ONE of:
      pip3 install websocket-client   # preferred — already a py-clob-client dep
      pip3 install websockets         # async alternative
    """
    # Try websocket-client first (sync WebSocketApp — from sports_server.py)
    try:
        from websocket import WebSocketApp
        _start_ws_sync()
        return True
    except ImportError:
        pass

    # Fall back to websockets async
    try:
        import websockets as _ws_lib
        _start_ws_async()
        return True
    except ImportError:
        pass

    log.warning("No WebSocket library found — REST polling mode")
    log.warning("Install: pip3 install websocket-client")
    return False


def _start_ws_sync():
    """websocket-client (sync) implementation — simpler, no asyncio needed."""
    from websocket import WebSocketApp

    def _run():
        backoff = 5
        while True:
            try:
                with _registry_lock:
                    all_tokens = list(token_to_market.keys())
                if not all_tokens:
                    time.sleep(5); continue

                sub_msg = None  # set on open

                def on_open(ws):
                    global _ws_global
                    nonlocal sub_msg
                    _ws_global = ws
                    _ws_subscribed_tokens.clear()
                    # Subscribe in chunks of 500 (WS max per connection)
                    for i in range(0, len(all_tokens), 500):
                        chunk = all_tokens[i:i+500]
                        ws.send(json.dumps({"assets_ids": chunk, "type": "market"}))
                        _ws_subscribed_tokens.update(chunk)
                    log.info(f"WS connected (sync) — {len(all_tokens)} tokens subscribed")
                    backoff_ref[0] = 5

                def on_message(ws, message):
                    _handle_ws_message(message)

                def on_error(ws, error):
                    log.debug(f"WS error: {error}")

                def on_close(ws, code, msg):
                    log.warning(f"WS closed ({code}) — reconnecting in {backoff}s")

                # Ping loop runs in separate thread
                ws_ref = [None]
                def _ping_loop():
                    while True:
                        time.sleep(10)
                        ws = ws_ref[0]
                        if ws:
                            try: ws.send("PING")
                            except: break

                backoff_ref = [backoff]
                wsa = WebSocketApp(
                    WS_URI,
                    on_open=on_open,
                    on_message=on_message,
                    on_error=on_error,
                    on_close=on_close,
                )
                ws_ref[0] = wsa
                threading.Thread(target=_ping_loop, daemon=True).start()
                wsa.run_forever()

            except Exception as e:
                log.warning(f"WS exception: {e}")

            time.sleep(min(backoff, 60))
            backoff = min(backoff * 1.5, 60)

    threading.Thread(target=_run, name="ws-feed-sync", daemon=True).start()


def _start_ws_async():
    """websockets (async) implementation — fallback."""
    import asyncio

    async def _connect():
        import websockets as _ws_lib
        backoff = 1
        while True:
            try:
                with _registry_lock:
                    all_tokens = list(token_to_market.keys())
                if not all_tokens:
                    await asyncio.sleep(5); continue
                log.info(f"WS connecting (async) — {len(all_tokens)} tokens...")
                async with _ws_lib.connect(WS_URI, ping_interval=None, open_timeout=15) as ws:
                    for i in range(0, len(all_tokens), 500):
                        await ws.send(json.dumps({"assets_ids": all_tokens[i:i+500], "type":"market"}))
                    log.info("WS connected (async) — live feed active")
                    backoff = 1
                    async def _ping():
                        while True:
                            await asyncio.sleep(10)
                            try: await ws.send("PING")
                            except: return
                    pt = asyncio.create_task(_ping())
                    try:
                        async for msg in ws:
                            _handle_ws_message(msg)
                    finally:
                        pt.cancel()
            except Exception as e:
                log.warning(f"WS async error: {e} — retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_connect())

    threading.Thread(target=_thread, name="ws-feed-async", daemon=True).start()

# ─────────────────────────────────────────────────────────────────────
# REST FALLBACK LOOP
# ─────────────────────────────────────────────────────────────────────
def poll_loop():
    from concurrent.futures import ThreadPoolExecutor, as_completed
    log.info(f"REST polling mode — {SCAN_INTERVAL}s interval")
    while True:
        with _registry_lock:
            mids = list(market_registry.keys())
        with ThreadPoolExecutor(max_workers=5) as ex:
            futs = {ex.submit(refresh_prices_rest, mid): mid for mid in mids}
            for f in as_completed(futs):
                mid = futs[f]
                try: f.result(); evaluate_market(mid)
                except Exception as e: log.debug(f"poll err {mid}: {e}")
        time.sleep(SCAN_INTERVAL)

# ─────────────────────────────────────────────────────────────────────
# BACKGROUND THREADS
# ─────────────────────────────────────────────────────────────────────
def _position_watcher_loop():
    """
    Polls prices via REST every 5s for markets where we hold positions.
    Prioritizes one-sided seeds closest to completing an arb.
    """
    _last_seed_log = 0.0
    while True:
        time.sleep(5)
        try:
            # Every 60s: check if any positions have been resolved on Polymarket
            _check_resolutions_via_api()
            with ledger.lock:
                open_positions = [
                    (mid, pos) for mid, pos in ledger.positions.items()
                    if (pos.yes_fills or pos.no_fills) and not pos.resolved
                ]
            if not open_positions:
                continue

            # PRIORITIZE: two-sided positions first (need management),
            # then one-sided by how close the opposite side is to completing.
            # This ensures the watcher spends time on completable arbs,
            # not checking 100 stagnant positions with no chance.
            two_sided = []
            one_sided = []
            for mid, pos in open_positions:
                has_y = bool(pos.yes_fills)
                has_n = bool(pos.no_fills)
                if has_y and has_n:
                    two_sided.append((mid, pos))
                else:
                    # Score by how cheap the opposite side was last seen
                    tok = pos.no_token if has_y else pos.yes_token
                    ts = token_states.get(tok)
                    opp_price = ts.ask if ts and ts.ask > 0 else 1.0
                    seed_price = pos.yes_avg_price if has_y else pos.no_avg_price
                    combined = seed_price + opp_price
                    one_sided.append((combined, mid, pos))

            # Sort one-sided by combined price (lowest = closest to arb)
            one_sided.sort(key=lambda x: x[0])

            # Check all two-sided + top 20 most promising one-sided per cycle
            MAX_ONE_SIDED_PER_CYCLE = 20
            check_list = [(m, p) for m, p in two_sided]
            check_list += [(m, p) for _, m, p in one_sided[:MAX_ONE_SIDED_PER_CYCLE]]

            # Every 2 min: log top seeds being watched
            now_t = time.time()
            if one_sided and now_t - _last_seed_log > 120:
                _last_seed_log = now_t
                log.info(f"SEED WATCHER: {len(two_sided)} two-sided + top {min(len(one_sided), MAX_ONE_SIDED_PER_CYCLE)}/{len(one_sided)} one-sided")
                for combined, m, p in one_sided[:5]:
                    side = "YES" if p.yes_fills else "NO"
                    seed_p = p.yes_avg_price if p.yes_fills else p.no_avg_price
                    opp_p = combined - seed_p
                    log.info(f"  → {p.title[:50]} | {side}@{seed_p:.3f} + opp@{opp_p:.3f} = {combined:.3f}")

            for mid, pos in check_list:
                info = market_registry.get(mid)
                if not info:
                    # Market not in registry (filtered out) but we hold a position
                    # Still evaluate using stored token IDs from ledger
                    with ledger.lock:
                        _pos = ledger.positions.get(mid)
                    if _pos and _pos.yes_token and _pos.no_token:
                        log.debug(f"Position watcher: {_pos.title[:50]} not in registry — using stored tokens")
                        yes_tok = _pos.yes_token
                        no_tok  = _pos.no_token
                        # Manually fetch prices and evaluate
                        book_yes = http_get(f"{CLOB}/book", params={"token_id": yes_tok})
                        book_no  = http_get(f"{CLOB}/book", params={"token_id": no_tok})
                        if book_yes:
                            ask = _best(book_yes, "ask")
                            token_states[yes_tok].update(price=ask, bid=_best(book_yes,"bid"), ask=ask)
                        if book_no:
                            ask = _best(book_no, "ask")
                            token_states[no_tok].update(price=ask, bid=_best(book_no,"bid"), ask=ask)
                        # Register temporarily so evaluate_market can find it
                        market_registry[mid] = {
                            "title": _pos.title, "event_title": _pos.title.split(":")[0],
                            "yes_token": yes_tok, "no_token": no_tok,
                            "live": True, "volume": 999, "liquidity": 999,
                            "info": {}, "end_dt": None
                        }
                        try:
                            evaluate_market(mid)
                        except Exception as e:
                            log.debug(f"Orphan position eval err {mid}: {e}")
                    continue
                yes_tok = info["yes_token"]
                no_tok  = info["no_token"]
                # Fetch fresh prices from REST
                book_yes = http_get(f"{CLOB}/book", params={"token_id": yes_tok})
                book_no  = http_get(f"{CLOB}/book", params={"token_id": no_tok})
                if book_yes:
                    ask = _best(book_yes, "ask")
                    bid = _best(book_yes, "bid")
                    token_states[yes_tok].update(
                        price=ask, bid=bid, ask=ask,
                        bid_size=_sz(book_yes, "bid"),
                        ask_size=_sz(book_yes, "ask")
                    )
                if book_no:
                    ask = _best(book_no, "ask")
                    bid = _best(book_no, "bid")
                    token_states[no_tok].update(
                        price=ask, bid=bid, ask=ask,
                        bid_size=_sz(book_no, "bid"),
                        ask_size=_sz(book_no, "ask")
                    )
                # Run evaluation — this will fire Phase 2 if prices have moved
                try:
                    evaluate_market(mid)
                except Exception as e:
                    log.debug(f"Position watcher eval err {mid}: {e}")
        except Exception as e:
            log.debug(f"Position watcher err: {e}")

_last_resolution_check = 0.0


def _detect_winner_from_api(mid: str) -> Optional[str]:
    """Try to detect winner from CLOB + data-api. Returns YES/NO/None."""
    # data-api position value: value=0 means lost
    if PROXY_WALLET:
        pos_info = http_get(f"https://data-api.polymarket.com/positions", params={
            "user": PROXY_WALLET.lower(),
            "market": mid,
        })
        if pos_info and isinstance(pos_info, list) and pos_info:
            p = pos_info[0]
            value = float(p.get("currentValue") or p.get("value") or 0)
            size = float(p.get("size") or 0)
            outcome = str(p.get("outcome", "")).lower()
            if value < 0.01 and size > 0:
                return None  # lost, but we don't know which side "won"
            elif value > 0.5 and size > 0:
                # Has value = this outcome won
                if outcome in ("yes", "true", "1"): return "YES"
                if outcome in ("no", "false", "0"): return "NO"
    return None


def _check_market_closed(mid: str) -> bool:
    """Check if market is closed via CLOB API (the only API that supports condition_id)."""
    data = http_get(f"{CLOB}/markets/{mid}")
    if data and isinstance(data, dict):
        return data.get("closed", False) or not data.get("accepting_orders", True)
    return False


def _check_resolutions_via_api():
    """
    Resolve positions by checking CLOB API for closed status.
    CLOB /markets/{condition_id} returns closed: true for settled markets.
    (Gamma API does NOT support condition_id lookups — returns random data.)
    """
    global _last_resolution_check
    now = time.time()
    if now - _last_resolution_check < 60.0:
        return
    _last_resolution_check = now

    with ledger.lock:
        open_positions = [
            (mid, pos) for mid, pos in ledger.positions.items()
            if not pos.resolved and (pos.yes_fills or pos.no_fills)
        ]
    if not open_positions:
        return

    resolved_count = 0
    api_checks = 0
    MAX_CHECKS = 30  # limit API calls per cycle

    for mid, pos in open_positions:
        if api_checks >= MAX_CHECKS:
            break
        api_checks += 1

        # Check CLOB API — does it support condition_id directly
        if not _check_market_closed(mid):
            continue

        # Market is CLOSED. Determine P&L.
        cost = pos.yes_total + pos.no_total
        if cost < 0.01:
            continue

        # Try to detect winner via data-api position value
        winner = _detect_winner_from_api(mid)

        if winner:
            if winner == "YES" and pos.yes_avg_price > 0:
                payout = pos.yes_total / pos.yes_avg_price
            elif winner == "NO" and pos.no_avg_price > 0:
                payout = pos.no_total / pos.no_avg_price
            else:
                payout = 0
            pl = round(payout - cost, 2)
        else:
            # Closed + can't determine winner = assume loss
            # (data-api shows value=0 for lost positions)
            pl = round(-cost, 2)

        tag = "WIN" if pl >= 0 else "LOSS"
        pos.resolved = True
        pos.stagnant = True
        pos.resolved_pl = pl
        resolved_count += 1
        log.info(f"RESOLVED [{tag}]: {pos.title[:55]}\n"
                 f"   {winner or 'closed+lost'} | cost ${cost:.2f} | P&L ${pl:+.2f}")

    if resolved_count:
        ledger.save()
        log.info(f"Resolution: {resolved_count} positions resolved this cycle")


def _depth_refresh_loop():
    """
    Populates bid_size/ask_size for OBI signal via background REST fetches.
    Runs every 30s across all markets. Never touches the WS thread.
    """
    while True:
        time.sleep(30)
        try:
            with _registry_lock:
                mids = list(market_registry.keys())
            # Only refresh markets with no size data yet
            needs_refresh = []
            for mid in mids:
                info = market_registry.get(mid)
                if not info:
                    continue
                ys = token_states[info["yes_token"]]
                ns = token_states[info["no_token"]]
                if ys.bid_size == 0 and ys.ask_size == 0:
                    needs_refresh.append(mid)
            if not needs_refresh:
                continue
            # Fetch in small batches to avoid hammering the API
            from concurrent.futures import ThreadPoolExecutor, as_completed
            def _fetch_depth(mid):
                info = market_registry.get(mid)
                if not info:
                    return
                for tok, ts in [(info["yes_token"], token_states[info["yes_token"]]),
                                (info["no_token"],  token_states[info["no_token"]])]:
                    book = http_get(f"{CLOB}/book", params={"token_id": tok})
                    if book:
                        ts.bid_size = _sz(book, "bid")
                        ts.ask_size = _sz(book, "ask")
            with ThreadPoolExecutor(max_workers=4) as ex:
                list(ex.map(_fetch_depth, needs_refresh[:20]))  # cap at 20 per cycle
        except Exception as e:
            log.debug(f"Depth refresh err: {e}")


def _stats_loop():
    while True:
        time.sleep(120)
        session_pnl = MY_BANKROLL - _session_start_bankroll if _session_start_bankroll > 0 else 0
        log.info(f"STATS: {stats.summary()} | Active:{ledger.active_count()}/{MAX_ACTIVE_MARKETS} | "
                 f"NC:${ledger.directional_exposure():.0f} Fav:${ledger.favored_exposure():.0f} Mid:${ledger.mid_exposure():.0f} Penny:${ledger.penny_exposure():.0f} | "
                 f"BRL:${MY_BANKROLL:,.0f} P&L:${session_pnl:+,.0f}")
        ledger.print_all()


# ─────────────────────────────────────────────────────────────────────
# AUTO-BANKROLL: query actual wallet balance and scale all sizing
# ─────────────────────────────────────────────────────────────────────
_last_bankroll_check = 0.0
_initial_bankroll = 0.0
_session_start_bankroll = 0.0

def _fetch_wallet_balance() -> float:
    """Get USDC balance + open position value from Polymarket APIs.
    Returns total bankroll (available USDC + deployed in positions)."""
    usdc_balance = 0.0
    position_value = 0.0

    # Method 1: Query CLOB client for collateral balance
    try:
        client = get_clob_client()
        if hasattr(client, 'get_balance'):
            bal = client.get_balance()
            if bal:
                usdc_balance = float(bal) / 1e6  # USDC has 6 decimals
        elif hasattr(client, 'get_collateral'):
            bal = client.get_collateral()
            if bal:
                usdc_balance = float(bal) / 1e6
    except Exception as e:
        log.debug(f"Balance via CLOB client failed: {e}")

    # Method 2: Data API for position values
    try:
        wallet = PROXY_WALLET or ""
        if wallet:
            data = http_get(f"https://data-api.polymarket.com/positions",
                          params={"user": wallet, "sizeThreshold": "0.01"},
                          timeout=10)
            if data and isinstance(data, list):
                for pos in data:
                    try:
                        size = float(pos.get("size", 0))
                        price = float(pos.get("currentPrice") or pos.get("price", 0))
                        position_value += size * price
                    except:
                        pass
    except Exception as e:
        log.debug(f"Position value fetch failed: {e}")

    # Method 3: Fallback — use ledger's own tracking
    if usdc_balance == 0 and position_value == 0:
        deployed = ledger.total_deployed()
        # Estimate: starting bankroll minus deployed + estimated unrealized
        return MY_BANKROLL  # can't determine, keep current

    total = usdc_balance + position_value
    return total


def _bankroll_refresh_loop():
    """Periodically check actual wallet balance and scale all sizing.
    Runs every 5 minutes. This is how we compound — as profits accumulate,
    trade sizes automatically increase, matching RN1's scaling pattern."""
    global MY_BANKROLL, _last_bankroll_check, _initial_bankroll, _session_start_bankroll
    global BASE_TRADE_SIZE, MAX_TRADE_SIZE, MAX_POSITION_PER_MARKET, MAX_ONE_SIDE
    global NC_TRADE_SIZE, PENNY_TRADE_SIZE, FAV_TRADE_SIZE, MID_TRADE_SIZE, ENDGAME_TRADE_SIZE
    global UPSET_HEDGE_SIZE

    REFRESH_INTERVAL = 300  # 5 minutes

    # Wait for bot to initialize
    time.sleep(30)

    # Record starting bankroll
    initial = _fetch_wallet_balance()
    if initial > 0:
        _initial_bankroll = initial
        _session_start_bankroll = initial
        log.info(f"BANKROLL INIT: ${initial:,.2f} detected")
    else:
        _initial_bankroll = MY_BANKROLL
        _session_start_bankroll = MY_BANKROLL

    while True:
        time.sleep(REFRESH_INTERVAL)
        try:
            new_balance = _fetch_wallet_balance()
            if new_balance <= 0:
                continue

            old_bankroll = MY_BANKROLL
            # Sanity: don't let it jump more than 50% in one check
            # (could indicate API error)
            if new_balance > old_bankroll * 1.5:
                log.warning(f"BANKROLL: suspicious jump ${old_bankroll:.0f} → ${new_balance:.0f}, capping at +50%")
                new_balance = old_bankroll * 1.5
            if new_balance < old_bankroll * 0.5:
                log.warning(f"BANKROLL: suspicious drop ${old_bankroll:.0f} → ${new_balance:.0f}, flooring at -50%")
                new_balance = old_bankroll * 0.5

            # Only update if meaningfully different (>2%)
            change_pct = (new_balance - old_bankroll) / old_bankroll * 100
            if abs(change_pct) < 2.0:
                continue

            MY_BANKROLL = round(new_balance, 2)

            # Scale all sizing proportionally using RN1's ACTUAL bankroll-level ratios:
            # startup<2K: 1.64%, growing 2K-10K: 0.59%, scaling 10K-50K: 0.35%
            # mature 50K-200K: 0.08%, whale 200K+: 0.03%
            if MY_BANKROLL < 2000:
                base_pct = 0.012    # startup: 1.2% (close to his 1.64%)
            elif MY_BANKROLL < 10000:
                base_pct = 0.006    # growing: 0.6% (close to his 0.59%)
            elif MY_BANKROLL < 50000:
                base_pct = 0.0035   # scaling: 0.35% (matches his 0.35%)
            elif MY_BANKROLL < 200000:
                base_pct = 0.0008   # mature: 0.08% (matches his 0.08%)
            else:
                base_pct = 0.0003   # whale: 0.03% (matches his 0.028%)
            BASE_TRADE_SIZE = round(MY_BANKROLL * base_pct, 2)
            MAX_TRADE_SIZE = round(MY_BANKROLL * 0.075, 2)     # 7.5%
            MAX_POSITION_PER_MARKET = round(MY_BANKROLL * 0.15, 2)  # 15%
            MAX_ONE_SIDE = round(MY_BANKROLL * 0.12, 2)        # 12%
            NC_TRADE_SIZE = round(MY_BANKROLL * base_pct, 2)   # same as base
            FAV_TRADE_SIZE = round(MY_BANKROLL * base_pct, 2)
            MID_TRADE_SIZE = round(MY_BANKROLL * base_pct, 2)
            PENNY_TRADE_SIZE = round(MY_BANKROLL * base_pct * 0.3, 2)  # 30% of base
            ENDGAME_TRADE_SIZE = round(MY_BANKROLL * 0.02, 2)  # 2%

            session_pnl = MY_BANKROLL - _session_start_bankroll
            session_pct = (session_pnl / _session_start_bankroll) * 100 if _session_start_bankroll > 0 else 0

            log.info(f"BANKROLL UPDATE: ${old_bankroll:,.0f} → ${MY_BANKROLL:,.0f} ({change_pct:+.1f}%)\n"
                     f"   Session P&L: ${session_pnl:+,.2f} ({session_pct:+.1f}%)\n"
                     f"   Sizing: Base=${BASE_TRADE_SIZE:.2f} NC=${NC_TRADE_SIZE:.2f} "
                     f"Fav=${FAV_TRADE_SIZE:.2f} Mid=${MID_TRADE_SIZE:.2f} "
                     f"Penny=${PENNY_TRADE_SIZE:.2f} MaxTrade=${MAX_TRADE_SIZE:.2f}")

        except Exception as e:
            log.debug(f"Bankroll refresh error: {e}")

SWEEP_INTERVAL_MINS = float(os.getenv("SWEEP_INTERVAL", "3"))  # full re-eval every 3 min

def _sweep_all_markets():
    """Re-evaluate every tracked market at current prices.
    Clears _attempted_fills first so previously-skipped orders get retried.
    Serial evaluation — safe with shared _attempted_fills state.
    """
    global _attempted_fills
    with _registry_lock:
        mids = list(market_registry.keys())
    _attempted_fills = set()
    swept = 0
    for mid in mids:
        try:
            evaluate_market(mid)
            swept += 1
        except Exception as e:
            log.debug(f"sweep eval err [{mid[:12]}]: {e}")
    log.info(f"SWEEP: {swept} markets re-evaluated, attempt cache cleared")

def _market_refresh_loop():
    last_sweep = time.time()
    while True:
        time.sleep(60)  # check every minute
        now = time.time()
        try:
            # Full market list refresh every MARKET_REFRESH_MINS
            if now - last_sweep >= MARKET_REFRESH_MINS * 60:
                ms = fetch_live_markets()
                register_markets(ms)
                log.info(f"Market list refreshed: {len(market_registry)} markets")

            # Full sweep re-evaluation every SWEEP_INTERVAL_MINS
            if now - last_sweep >= SWEEP_INTERVAL_MINS * 60:
                _sweep_all_markets()
                last_sweep = now
        except Exception as e:
            log.warning(f"Refresh err: {e}")

def _order_poll_loop():
    while True:
        time.sleep(30)
        if DRY_RUN: continue
        now = time.time()
        with ledger.lock:
            positions = list(ledger.positions.values())
        for pos in positions:
            for oid in [o for o,i in list(pos.open_orders.items())
                        if now - i["placed_at"] > ORDER_EXPIRY_SECS]:
                try:
                    get_clob_client().cancel_order(oid)
                    log.info(f"Cancelled stale order {oid[:12]}")
                except: pass
                pos.open_orders.pop(oid, None)

# ─────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────
def main():
    global DRY_RUN, MY_BANKROLL, MIN_SPREAD_PCT, BASE_TRADE_SIZE

    p = argparse.ArgumentParser()
    p.add_argument("--live",       action="store_true")
    p.add_argument("--fresh",      action="store_true", help="Clear ledger and start fresh")
    p.add_argument("--bankroll",   type=float, default=MY_BANKROLL)
    p.add_argument("--min-spread", type=float, default=MIN_SPREAD_PCT)
    p.add_argument("--base-size",  type=float, default=BASE_TRADE_SIZE)
    args = p.parse_args()

    if args.live:       DRY_RUN = False
    MY_BANKROLL    = args.bankroll
    MIN_SPREAD_PCT = args.min_spread
    BASE_TRADE_SIZE= args.base_size

    # Clear old ledger if --fresh
    if args.fresh:
        import pathlib
        lf = pathlib.Path(LEDGER_FILE)
        if lf.exists():
            lf.unlink()
            log.info("FRESH START: ledger.json deleted")

    mode = "DRY RUN" if DRY_RUN else "LIVE TRADING"
    log.info(f"""
╔══════════════════════════════════════════════════════╗
║          PolyArb v3 — WebSocket Multi-Strategy       ║
╠══════════════════════════════════════════════════════╣
║  Mode:     {mode:<43}║
║  Bankroll: ${MY_BANKROLL:<43.0f}║
║  Signals:  DUTCH_BOOK | PHASE1/2 | SPIKE | OBI      ║
╚══════════════════════════════════════════════════════╝""")

    if not DRY_RUN: get_clob_client()

    log.info("Fetching live markets...")
    register_markets(fetch_live_markets())
    log.info(f"Tracking {len(market_registry)} markets / {len(token_to_market)} tokens")

    # Restore positions from previous session
    ledger.load()
    ledger.resolve_missing_markets()  # free slots for finished games

    threading.Thread(target=_stats_loop,           daemon=True).start()
    threading.Thread(target=_market_refresh_loop,  daemon=True).start()
    threading.Thread(target=_bankroll_refresh_loop, daemon=True, name="bankroll").start()
    threading.Thread(target=_order_poll_loop,       daemon=True).start()
    threading.Thread(target=_position_watcher_loop, daemon=True, name="pos-watcher").start()
    threading.Thread(target=_depth_refresh_loop,    daemon=True, name="depth-refresh").start()
    log.info("Position watcher started — polling open positions every 5s via REST")

    ws_active = _run_websocket_loop()

    if ws_active:
        log.info("WebSocket feed starting — event-driven mode")
        try:
            while True: time.sleep(60)
        except KeyboardInterrupt: pass
    else:
        try:
            poll_loop()
        except KeyboardInterrupt: pass

    log.info("\nStopped.")
    log.info(f"Final: {stats.summary()}")
    ledger.print_all()

if __name__ == "__main__":
    main()
