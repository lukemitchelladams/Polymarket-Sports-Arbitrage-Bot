"""
PolyArb v3 — WebSocket-Driven Multi-Strategy Arb Bot
======================================================
Replicates strategy of 0x2005d16a84ceefa912d4e380cd32e7ff827875ea

ARCHITECTURE: WebSocket-first (falls back to REST polling)
  - Subscribes to wss://ws-subscriptions-clob.polymarket.com/ws/market
  - Receives price_change events pushed in real-time (ms latency vs 5s polling)
  - Maintains a rolling 60s price history per token for signal detection

STRATEGIES (from MarketPulse-X analysis):
  1. DUTCH_BOOK    -- YES+NO sum < threshold: guaranteed spread exists NOW
  2. PHASE1_BUILD  -- One side in 0.30-0.69 range: enter directionally, wait for other
  3. SPIKE_DETECT  -- Price moved >3% in 10s: momentum/mean-revert signal
  4. OB_IMBALANCE  -- Bid size >> Ask size: buy pressure incoming

HOW TO RUN:
  pip3 install websockets        # one-time, enables real-time feed
  python3 arb_v3.py              # dry run
  python3 arb_v3.py --live       # live trading (requires keys.env)
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
# His Jul '25 at ~$1K: 203 trades/day, $1.9K/day vol, 3x turnover
# Our target: 200+ trades/day, $3K/day vol, 50-100 concurrent markets
#
# His distribution (1M+ buys):
#   0.01-0.10:  7.5% trades,  $855K vol  → PENNY
#   0.10-0.20:  8.7% trades,  $2.5M vol  → DEEP VALUE
#   0.20-0.40: 22.0% trades, $12.5M vol  → CORE SEEDS  (Phase 1)
#   0.40-0.60: 26.0% trades, $26.7M vol  → PRIMARY ZONE (Phase 1)
#   0.60-0.80: 21.0% trades, $25.8M vol  → FAVORED SIDE
#   0.80-0.90:  8.2% trades, $12.5M vol  → NEAR-CERTAIN
#   0.90-1.00:  5.8% trades,  $9.5M vol  → HEAVY FAVORITES
# ─────────────────────────────────────────────────────────────────────

# ── ARB ENGINE (guaranteed profit — always priority #1) ──
ENTRY_THRESHOLD  = float(os.getenv("ENTRY_THRESHOLD", "0.98"))
MIN_SPREAD_PCT   = float(os.getenv("MIN_SPREAD_PCT", "2.0"))

# ── PHASE 1 SEEDS (his 0.10-0.60 zone = 57% of trades) ──
PHASE1_MID_MIN   = float(os.getenv("PHASE1_MID_MIN", "0.15"))
PHASE1_MID_MAX   = float(os.getenv("PHASE1_MID_MAX", "0.60"))
PHASE1_OTHER_MAX = float(os.getenv("PHASE1_OTHER_MAX", "0.90"))
HARVEST_THRESHOLD= float(os.getenv("HARVEST_THRESHOLD", "0.85"))

# ── TRADE SIZING ($1K bankroll = his exact Jul '25 starting point) ──
# His Jul avg trade: $1.9K/day ÷ 203 = $9.36 avg
# Median much lower (~$3-5), he layers small fills then larger ones
BASE_TRADE_SIZE  = float(os.getenv("BASE_TRADE_SIZE", "5.0"))     # 0.5% bankroll
MAX_TRADE_SIZE   = float(os.getenv("MAX_TRADE_SIZE", "50.0"))     # 5% max single fill
MAX_POSITION_PER_MARKET = float(os.getenv("MAX_POSITION", "100.0"))  # 10% per market
MAX_ONE_SIDE = float(os.getenv("MAX_ONE_SIDE", "75.0"))           # 7.5% one-side cap

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

# ── NEAR-CERTAIN HARVESTING (his 0.80-1.00 = 14% of trades) ──
# At $1K: can run 100+ NC positions simultaneously
NC_MIN_PRICE      = float(os.getenv("NC_MIN_PRICE", "0.80"))
NC_MAX_PRICE      = float(os.getenv("NC_MAX_PRICE", "0.97"))
NC_TRADE_SIZE     = float(os.getenv("NC_TRADE_SIZE", "5.0"))      # 0.5% bankroll per trade
NC_MAX_EXPOSURE   = float(os.getenv("NC_MAX_EXPOSURE", "0.35"))   # 35% = $350 cap
NC_COOLDOWN       = float(os.getenv("NC_COOLDOWN", "30.0"))
NC_MIN_VOLUME     = float(os.getenv("NC_MIN_VOLUME", "200"))

# ── PENNY SCOOP (his 0.01-0.10 = 7.5% of trades) ──
PENNY_MAX_PRICE   = float(os.getenv("PENNY_MAX_PRICE", "0.10"))
PENNY_TRADE_SIZE  = float(os.getenv("PENNY_TRADE_SIZE", "2.00"))  # 0.2% bankroll
PENNY_MAX_TOTAL   = float(os.getenv("PENNY_MAX_TOTAL", "0.08"))   # 8% = $80 cap
PENNY_COOLDOWN    = float(os.getenv("PENNY_COOLDOWN", "45.0"))

# ── FAVORED SIDE (his 0.60-0.80 = 21% of trades) ──
FAV_MIN_PRICE     = float(os.getenv("FAV_MIN_PRICE", "0.60"))
FAV_MAX_PRICE     = float(os.getenv("FAV_MAX_PRICE", "0.80"))
FAV_TRADE_SIZE    = float(os.getenv("FAV_TRADE_SIZE", "4.0"))     # 0.4% bankroll
FAV_MAX_EXPOSURE  = float(os.getenv("FAV_MAX_EXPOSURE", "0.25"))  # 25% = $250 cap
FAV_COOLDOWN      = float(os.getenv("FAV_COOLDOWN", "45.0"))
FAV_MIN_VOLUME    = float(os.getenv("FAV_MIN_VOLUME", "300"))

# STALE PRICE GATE: allow Phase 2 completion up to 5% negative combined
# (live prices lag reality — RN1 enters at slight negative spread intentionally)
STALE_PRICE_GATE   = float(os.getenv("STALE_PRICE_GATE", "1.02"))  # allow 2% negative — RN1 does this

# FOCUS MODE: max markets with open positions at once. Once full, no new
# Phase 1 entries — concentrate on managing what we already hold.
MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "75"))   # RN1 Jul '25: ~50 markets
MAX_PER_EVENT      = int(os.getenv("MAX_PER_EVENT", "5"))     # allow more sub-markets per event
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
        """Count positions that need active management against the cap.
        Only counts:
          - Two-sided positions (active arbs being managed)
          - Recent one-sided seeds (< 10 min, still trying to complete)
        Old one-sided seeds and resolved/stagnant positions don't count."""
        now = time.time()
        with self.lock:
            count = 0
            for p in self.positions.values():
                if p.resolved:
                    continue
                has_yes = p.yes_total > 0
                has_no = p.no_total > 0
                if has_yes and has_no:
                    count += 1  # two-sided = active arb
                elif has_yes or has_no:
                    # one-sided seed — only count if recent
                    age = now - p.created_at if hasattr(p, 'created_at') and p.created_at else 9999
                    if age < 600:  # < 10 min
                        count += 1
                # else: empty position, skip
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
        self.near_certain=0; self.penny=0; self.favored=0  # RN1-style strategies
    def summary(self):
        m = (time.time()-self.start)/60
        return (f"Runtime:{m:.1f}m | Markets:{self.markets_seen} | WS:{self.ws_events} | "
                f"Dutch:{self.dutch} Spike:{self.spike} OBI:{self.obi} NC:{self.near_certain} Fav:{self.favored} Penny:{self.penny} | "
                f"Orders:{self.orders} Fills:{self.fills} | ${ledger.total_deployed():.2f}")

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


def _fill(market_id, side, price, size, token, pos, label):
    if size < 1.00: return
    # ═══ NEW POSITION CAP ═══
    # First entry into any market: $7.50 max. Speculative until both sides exist.
    MAX_NEW_FILL = 7.50
    is_new = (pos is None) or (pos.total_deployed < 1.0)
    if is_new and size > MAX_NEW_FILL:
        log.info(f"  NEW-POS CAP: ${size:.2f} → ${MAX_NEW_FILL:.2f} (first entry) | {label[:40]}")
        size = MAX_NEW_FILL
    # ═══ BUG PROTECTION SAFETY CAP ═══
    MAX_SINGLE_TRADE = MY_BANKROLL * 0.33
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
    else:
        # Live mode: use FAK (market order) for guaranteed immediate fill.
        # Records fill optimistically at asked price; actual fill may be
        # 1-2¢ worse due to slippage but position tracking stays accurate.
        oid = place_limit_order(token, price, size, label, use_fak=True)
        if oid:
            ledger.record_fill(market_id, side, price, size)
            stats.fills += 1
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

    # ── ENDGAME: HAMMER winning side + UPSET HEDGE cheap side ────────────
    # Only fires if we already hold a position. Checked first because
    # near-zero prices are time-sensitive.
    # If endgame fires (rescue/hedge/hammer), skip Dutch/rebalance this cycle.
    if we_have_position:
        endgame_fired = _sig_endgame(market_id, title, yes_tok, no_tok,
                        yes_ask, no_ask, pos, has_yes, has_no, now)
        if endgame_fired:
            if DRY_RUN:
                log.info(f"   POS: YES${pos.yes_total:.0f}@{pos.yes_avg_price:.3f} | "
                         f"NO${pos.no_total:.0f}@{pos.no_avg_price:.3f} | "
                         f"Spread:{pos.implied_spread*100:.1f}%")
            return  # never combine endgame + rebalance in same cycle

    # ── CROSS-MARKET DUTCH: NO on both team-win markets (draw pays both) ─
    if not we_have_position:
        _sig_cross_dutch(market_id, now)

    # ── DUTCH / PHASE 2 / ADDING ──────────────────────────────────────────
    dutch_fired = _sig_dutch(market_id, title, yes_tok, no_tok,
                             yes_ask, no_ask, ys, ns, pos,
                             has_yes, has_no, now)
    if dutch_fired:
        if DRY_RUN and pos.total_deployed > 0:
            log.info(f"   POS: YES${pos.yes_total:.0f}@{pos.yes_avg_price:.3f} | "
                     f"NO${pos.no_total:.0f}@{pos.no_avg_price:.3f} | "
                     f"Spread:{pos.implied_spread*100:.1f}%")
        return

    # ── SPIKE / OBI: only enter new markets here, focus on existing ones ──
    if we_have_position:
        return  # already in this market — Dutch/endgame handles it

    if now - _last_spike[market_id] >= SPIKE_COOLDOWN:
        if _sig_spike(market_id, title, yes_tok, no_tok,
                      yes_ask, no_ask, ys, ns, pos):
            _last_spike[market_id] = now
            return

    if now - _last_obi[market_id] >= OBI_COOLDOWN:
        if _sig_obi(market_id, title, yes_tok, no_tok,
                    yes_ask, no_ask, ys, ns, pos):
            _last_obi[market_id] = now
            return

    # ── RN1-STYLE SIGNALS (probabilistic, not guaranteed) ────────────
    # These run AFTER arb signals — arbs get priority.
    # Order matches his volume distribution: NC > Favored > Penny
    
    # Near-certain: buy heavy favorites at 0.80-0.97
    if _sig_near_certain(market_id, title, yes_tok, no_tok,
                         yes_ask, no_ask, pos, now):
        return

    # Favored side: buy likely winners at 0.60-0.80
    if _sig_favored_side(market_id, title, yes_tok, no_tok,
                         yes_ask, no_ask, pos, now):
        return

    # Penny scoop: buy collapsing sides at 0.01-0.10
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


def _sig_dutch(market_id, title, yes_tok, no_tok, yes_ask, no_ask, ys, ns, pos, has_yes, has_no, now):
    """Dutch book detection + two-phase position building."""
    if not has_yes and not has_no:
        total = yes_ask + no_ask
        sp    = (1.0 - total) * 100
        # Simultaneous entry
        if total < 1.05:  # log when we're getting close (throttled per market)
            _near_key = market_id + "_near"
            if now - _last_hammer.get(_near_key, 0) > 10.0:  # max once per 10s per market
                log.info(f"NEAR ARB: {title[:50]} combined={total:.3f} (need <{ENTRY_THRESHOLD:.2f})")
                _last_hammer[_near_key] = now
        if total < ENTRY_THRESHOLD and sp >= MIN_SPREAD_PCT:
            ys_, ns_ = calculate_size(yes_ask, no_ask, pos)
            if ys_ <= 0 and ns_ <= 0: return False
            log.info(f"DUTCH BOOK: {title[:60]}\n"
                     f"   YES@{yes_ask:.3f}+NO@{no_ask:.3f}={total:.3f} ({sp:.1f}%) "
                     f"sizes ${ys_:.2f}/${ns_:.2f}")
            stats.dutch += 1
            if ys_ > 0: _fill(market_id,"YES",yes_ask,ys_,yes_tok,pos,f"YES|{title[:35]}")
            if ns_ > 0: _fill(market_id,"NO", no_ask, ns_, no_tok, pos,f"NO |{title[:35]}")
            return True
        # Phase 1 directional
        y_ok = PHASE1_MID_MIN <= yes_ask <= PHASE1_MID_MAX
        n_ok = PHASE1_MID_MIN <= no_ask  <= PHASE1_MID_MAX
        if not y_ok and not n_ok: return False
        if y_ok and n_ok: side = "YES" if yes_ask >= no_ask else "NO"
        elif y_ok:        side = "YES"
        else:             side = "NO"
        price = yes_ask if side == "YES" else no_ask
        tok   = yes_tok if side == "YES" else no_tok
        other = no_ask  if side == "YES" else yes_ask

        # House-edge filter — combined > 1.04 means we need too big a move to profit
        if yes_ask + no_ask > 1.04:
            log.debug(f"P1 skip: combined {yes_ask+no_ask:.3f}>1.04 (house edge) | {title[:50]}")
            return False
        if other > PHASE1_OTHER_MAX:
            log.info(f"P1 skip other@{other:.3f}>{PHASE1_OTHER_MAX} | {title[:50]}")
            return False
        gap_needed = ENTRY_THRESHOLD - price
        if other - gap_needed > 0.30:
            log.info(f"P1 skip gap too large ({other:.3f}->{gap_needed:.3f} drop={other-gap_needed:.2f}) | {title[:50]}")
            return False

        # Seed size: SMALL and fixed. Seeds are speculative one-sided bets hoping
        # Phase 2 completes. The real money is made when we lock the spread with
        # the opposite side (sized to match). Seeds just stake our claim.
        # $2-5 max — never risk real money on an incomplete arb.
        sz = 2.0

        # INSTANT LOCK: spread already exists — fire both sides immediately.
        if price + other < ENTRY_THRESHOLD:
            sp_now = (1.0 - price - other) * 100
            ys_, ns_ = calculate_size(yes_ask, no_ask, pos)
            if ys_ > 0 or ns_ > 0:
                log.info(f"P1-LOCK: {title[:60]}\n"
                         f"   {side}@{price:.3f}+other@{other:.3f}={price+other:.3f} "
                         f"({sp_now:.1f}% spread — locking both sides now)")
                stats.dutch += 1
                if ys_ > 0: _fill(market_id,"YES",yes_ask,ys_,yes_tok,pos,f"LOCK-YES|{title[:35]}")
                if ns_ > 0: _fill(market_id,"NO", no_ask, ns_, no_tok, pos,f"LOCK-NO |{title[:35]}")
                return True

        log.info(f"PHASE 1: {title[:60]}\n"
                 f"   {side}@{price:.3f} ${sz:.2f} | other@{other:.3f} "
                 f"(need <{gap_needed:.3f} to lock, gap={other-gap_needed:.2f})")
        stats.dutch += 1
        _fill(market_id, side, price, sz, tok, pos, f"P1-{side}|{title[:35]}")
        return True

    # Phase 2 / Ongoing
    ys_ = ns_ = 0.0; ym = nm = None
    p2_gate = STALE_PRICE_GATE

    # Detect if position needs rebalancing
    # Triggers on: (a) share imbalance > 1.5x, OR (b) position is underwater (negative guaranteed P&L)
    _rebalance_mode = False
    if has_yes and has_no and pos.yes_avg_price > 0 and pos.no_avg_price > 0:
        _yes_sh = pos.yes_total / pos.yes_avg_price
        _no_sh  = pos.no_total  / pos.no_avg_price
        _cost   = pos.yes_total + pos.no_total
        _current_pl = min(_yes_sh - _cost, _no_sh - _cost)
        _rebalance_mode = (_yes_sh > _no_sh * 1.5 or _no_sh > _yes_sh * 1.5 or _current_pl < 0)


    # Phase 2 completion: one-sided only — must be profitable
    # CRITICAL FIX: use our ENTRY PRICE for the side we own, not current ask.
    # We already bought our side — profitability depends on what WE paid + what
    # we'll pay for the opposite side, not on what the market currently quotes
    # for the side we already hold.
    if has_no and not has_yes:
        our_cost = pos.no_avg_price  # what we actually paid
        c = yes_ask + our_cost; ym = 1.0 - c
        if c < p2_gate and ym > 0:
            no_shares = pos.no_total / pos.no_avg_price if pos.no_avg_price > 0 else 0
            ys_ = round(no_shares * yes_ask, 2)
            ys_ = min(ys_, MAX_POSITION_PER_MARKET - pos.total_deployed)
    elif has_yes and has_no:
        c = yes_ask + no_ask; ym = 1.0 - c
        if c < p2_gate and ym > 0:
            ys_ = _size_marginal(max(ym, 0.001), yes_ask, pos.no_total, pos=pos, side="YES", yes_ask=yes_ask, no_ask=no_ask)
    if has_yes and not has_no:
        our_cost = pos.yes_avg_price  # what we actually paid
        c = no_ask + our_cost; nm = 1.0 - c
        if c < p2_gate and nm > 0:
            yes_shares = pos.yes_total / pos.yes_avg_price if pos.yes_avg_price > 0 else 0
            ns_ = round(yes_shares * no_ask, 2)
            ns_ = min(ns_, MAX_POSITION_PER_MARKET - pos.total_deployed)
    elif has_no and has_yes:
        c = yes_ask + no_ask; nm = 1.0 - c
        if c < p2_gate and nm > 0:
            ns_ = _size_marginal(max(nm, 0.001), no_ask, pos.yes_total, pos=pos, side="NO", yes_ask=yes_ask, no_ask=no_ask)

    # REBALANCE OVERRIDE: must come BEFORE heavy-side gate
    # CRITICAL SAFETY: check cooldown FIRST to prevent spam, and NEVER rebalance
    # if buying the light side at current prices would lose money (combined >= 1.0)
    if _rebalance_mode:
        _rebal_key = market_id + "_rebal"
        if now - _last_p2.get(_rebal_key, 0) < 30.0:  # 30 second cooldown
            _rebalance_mode = False  # disable so it doesn't bypass gates below
        else:
            _yes_sh = pos.yes_total / pos.yes_avg_price
            _no_sh  = pos.no_total  / pos.no_avg_price
            _pos_cost = pos.yes_total + pos.no_total
            # CRITICAL: never rebalance if combined ask >= 1.0 — every buy loses money
            if yes_ask + no_ask >= 1.0:
                log.debug(f"REBALANCE skip: combined {yes_ask+no_ask:.3f} >= 1.0 — buy would lose money | {title[:40]}")
                _rebalance_mode = False
            else:
                _rebal_cap = min(MY_BANKROLL * 0.15, _pos_cost * 0.50)
                _rebal_cap = max(_rebal_cap, 2.0)  # at least $2
                if _yes_sh > _no_sh:
                    target_no_sh = _yes_sh - _no_sh
                    ns_ = min(max(round(target_no_sh * no_ask, 2), 2.0), _rebal_cap)
                    ys_ = 0.0
                    log.info(f"REBALANCE: {title[:50]} YES({_yes_sh:.1f}sh) >> NO({_no_sh:.1f}sh) — buying ${ns_:.2f} NO@{no_ask:.3f} (cap ${_rebal_cap:.0f})")
                elif _no_sh > _yes_sh:
                    target_yes_sh = _no_sh - _yes_sh
                    ys_ = min(max(round(target_yes_sh * yes_ask, 2), 2.0), _rebal_cap)
                    ns_ = 0.0
                    log.info(f"REBALANCE: {title[:50]} NO({_no_sh:.1f}sh) >> YES({_yes_sh:.1f}sh) — buying ${ys_:.2f} YES@{yes_ask:.3f} (cap ${_rebal_cap:.0f})")
                _last_p2[_rebal_key] = now

    if ys_ <= 0 and ns_ <= 0: return False


    rem = MAX_POSITION_PER_MARKET - pos.total_deployed
    tot = ys_ + ns_
    if tot > rem:
        ys_ = round(ys_*rem/tot,2); ns_ = round(ns_*rem/tot,2)
    if _rebalance_mode:
        phase = "REBALANCE"
    elif has_yes != has_no:
        phase = "PHASE 2"
    else:
        phase = "ADDING"

    # ROI IMPROVEMENT GATE is now in _fill() — applies universally to ALL
    # phases (ADDING, REBALANCE, PHASE 2, etc). Every trade on a two-sided
    # position must improve guaranteed P&L or it's blocked at the _fill level.
    # No duplicate check needed here.

    # ═══ UNIVERSAL ROI GATE: check COMBINED effect of both planned fills ═══
    # For two-sided positions, both fills together must improve guaranteed P&L.
    if not _check_roi_improvement(pos, ys_, ns_, yes_ask, no_ask, f"{phase}|{title[:35]}"):
        return False

    # Skip if neither side meets minimum order size — avoids spam loop
    if ys_ < 1.00 and ns_ < 1.00:
        return False
    # Per-market cooldown: suppress repeat logs for same signal within 30s
    # EXCEPTION: rebalance mode gets a longer cooldown but is never fully blocked
    p2_key = market_id + "_p2"
    cooldown = 15.0 if _rebalance_mode else 30.0   # rebalance: aggressive 15s cooldown
    if now - _last_p2[p2_key] < cooldown:
        return False
    _last_p2[p2_key] = now
    if _rebalance_mode:
        log.info(f"REBALANCE: {title[:55]} — executing light-side buy")

    log.info(f"{phase}: {title[:60]}\n"
             f"   YES@{yes_ask:.3f} m:{(ym or 0)*100:.1f}% ${ys_:.2f} | "
             f"NO@{no_ask:.3f} m:{(nm or 0)*100:.1f}% ${ns_:.2f}")
    stats.dutch += 1
    if ys_ > 0: _fill(market_id,"YES",yes_ask,ys_,yes_tok,pos,f"YES|{title[:35]}")
    if ns_ > 0: _fill(market_id,"NO", no_ask, ns_, no_tok, pos,f"NO |{title[:35]}")
    return True


def _sig_endgame(market_id, title, yes_tok, no_tok, yes_ask, no_ask,
                 pos, has_yes, has_no, now):
    """
    Endgame management for markets we're already in.

    Two modes once a game is essentially decided (one side >= HAMMER_PRICE):

    HAMMER: We hold the WINNING side (high price). Add more aggressively —
    near-certain payout. Rate-limited by _last_hammer cooldown.

    UPSET HEDGE: The LOSING side has collapsed to pennies. Buy a small amount
    as insurance against a late-game reversal. RN1 pattern: hold YES@0.92,
    also grab NO@0.08 in case of upset. Only fires once per endgame.

    ENDGAME DUTCH: If we hold one side and the other has collapsed, complete
    the Dutch at massive spread (original endgame behavior).
    """
    yes_winning = yes_ask >= HAMMER_PRICE
    no_winning  = no_ask  >= HAMMER_PRICE
    yes_cheap   = 0.005 < yes_ask <= UPSET_HEDGE_MAX
    no_cheap    = 0.005 < no_ask  <= UPSET_HEDGE_MAX

    fired = False

    # ── HAMMER: add to our winning side ───────────────────────────────
    # GUARD: only hammer when BOTH sides are filled AND spread is positive.
    # Prevents hammering a one-sided position into a directional bet, and
    # prevents adding to a position that's already underwater.
    HAMMER_COOLDOWN = 120.0  # max once per 2 minutes
    # True guaranteed spread using correct payout math
    if pos.yes_total > 0 and pos.no_total > 0 and pos.yes_avg_price > 0 and pos.no_avg_price > 0:
        _cost = pos.yes_total + pos.no_total
        _ysh  = pos.yes_total / pos.yes_avg_price
        _nsh  = pos.no_total  / pos.no_avg_price
        current_spread = min(_ysh - _cost, _nsh - _cost) / _cost if _cost > 0 else -1.0
    else:
        current_spread = -1.0
    # CRITICAL: block hammer if live market sum > 1.0 (already negative spread at current prices)
    live_sum = yes_ask + no_ask
    # Block hammer if live prices show negative spread — market moved against us
    # Use strict 0.96 threshold: only hammer when still clearly profitable at live prices
    # HAMMER requires position is already balanced enough that BOTH outcomes profit.
    # If YES wins: YES_shares (= yes_total/yes_avg) must cover total_cost
    # If NO wins:  NO_shares  (= no_total/no_avg)   must cover total_cost
    total_cost   = pos.yes_total + pos.no_total
    yes_shares   = pos.yes_total / pos.yes_avg_price if pos.yes_avg_price > 0 else 0
    no_shares    = pos.no_total  / pos.no_avg_price  if pos.no_avg_price  > 0 else 0
    both_covered = (yes_shares >= total_cost * 0.85 and no_shares >= total_cost * 0.85)
    hammer_ok = (pos.yes_total > 0 and pos.no_total > 0
                 and current_spread > 0.005
                 and live_sum <= 0.96
                 and both_covered)  # only hammer if already balanced

    if yes_winning and has_yes and hammer_ok and now - _last_hammer[market_id + "_yes"] > HAMMER_COOLDOWN:
        sz = min(pos.yes_total * 0.5, pos.no_total * 0.5)
        sz = max(sz, 2.0)
        if sz >= 1.0 and _check_roi_improvement(pos, sz, 0, yes_ask, 0, f"HAMMER-YES|{title[:35]}"):
            log.info(f"HAMMER YES: {title[:60]}\n"
                     f"   YES@{yes_ask:.3f} near-certain (spread={current_spread*100:.1f}%) → adding ${sz:.2f}")
            _fill(market_id, "YES", yes_ask, sz, yes_tok, pos, f"HAMMER-YES|{title[:35]}")
            _last_hammer[market_id + "_yes"] = now
            stats.dutch += 1
            fired = True

    if no_winning and has_no and hammer_ok and now - _last_hammer[market_id + "_no"] > HAMMER_COOLDOWN:
        sz = min(pos.no_total * 0.5, pos.yes_total * 0.5)
        sz = max(sz, 2.0)
        if sz >= 1.0 and _check_roi_improvement(pos, 0, sz, 0, no_ask, f"HAMMER-NO|{title[:35]}"):
            log.info(f"HAMMER NO: {title[:60]}\n"
                     f"   NO@{no_ask:.3f} near-certain (spread={current_spread*100:.1f}%) → adding ${sz:.2f}")
            _fill(market_id, "NO", no_ask, sz, no_tok, pos, f"HAMMER-NO|{title[:35]}")
            _last_hammer[market_id + "_no"] = now
            stats.dutch += 1
            fired = True

    # ── UPSET HEDGE: buy tiny amount of collapsing side ───────────────
    # Only hedge if we've already hammered (we have a big winning position).
    # Cheap losing side is our insurance against a comeback.
    if yes_cheap and has_yes and no_winning:
        # We hammered NO (it's high) but YES collapsed — hedge against YES comeback
        # Wait: this means we hold YES, and NO is high (NO is winning). But we hold YES.
        # Actually: has_yes means we entered YES early. NO winning means NO=0.85+.
        # YES collapsing to 0.08 means our position is losing. Buy more YES for hedge.
        pass  # handled by ENDGAME DUTCH below

    if no_cheap and has_no and yes_winning:
        # Symmetric: we hold NO, YES is winning. NO collapsing. Same — ENDGAME DUTCH.
        pass

    # Specifically: we hammered our winning side, now hedge the cheap losing side
    if yes_cheap and has_yes and yes_winning:
        # We hold YES (high) AND YES is still high — no, yes_cheap means YES is low
        # This case: has_yes and yes is cheap means we entered YES early and it lost.
        pass

    # Clean hedge cases:
    # Case 1: We hammered NO (hold NO, no_ask >= HAMMER), YES is now pennies → hedge YES
    if no_winning and has_no and yes_cheap:
        hedge_key = market_id + "_hedge_yes"
        if now - _last_hammer.get(hedge_key, 0) > 300:  # hedge once per 5 min
            sz = min(UPSET_HEDGE_SIZE * (MY_BANKROLL / 1000.0), 10.0)
            sz = min(sz, MAX_POSITION_PER_MARKET - pos.total_deployed)
            if sz >= 0.50 and _check_roi_improvement(pos, sz, 0, yes_ask, 0, f"HEDGE-YES|{title[:35]}"):
                log.info(f"UPSET HEDGE: {title[:60]}\n"
                         f"   YES@{yes_ask:.3f} (upset insurance, hold NO@{pos.no_avg_price:.3f})")
                _fill(market_id, "YES", yes_ask, sz, yes_tok, pos, f"HEDGE-YES|{title[:35]}")
                _last_hammer[hedge_key] = now
                stats.dutch += 1
                fired = True

    # Case 2: We hammered YES (hold YES, yes_ask >= HAMMER), NO is now pennies → hedge NO
    if yes_winning and has_yes and no_cheap:
        hedge_key = market_id + "_hedge_no"
        if now - _last_hammer.get(hedge_key, 0) > 300:
            sz = min(UPSET_HEDGE_SIZE * (MY_BANKROLL / 1000.0), 10.0)
            sz = min(sz, MAX_POSITION_PER_MARKET - pos.total_deployed)
            if sz >= 0.50 and _check_roi_improvement(pos, 0, sz, 0, no_ask, f"HEDGE-NO|{title[:35]}"):
                log.info(f"UPSET HEDGE: {title[:60]}\n"
                         f"   NO@{no_ask:.3f} (upset insurance, hold YES@{pos.yes_avg_price:.3f})")
                _fill(market_id, "NO", no_ask, sz, no_tok, pos, f"HEDGE-NO|{title[:35]}")
                _last_hammer[hedge_key] = now
                stats.dutch += 1
                fired = True

    # ── ENDGAME DUTCH: complete existing one-sided position ───────────
    # The other side just collapsed — lock in the spread.
    # CRITICAL: size to MATCH existing shares, not proportional to available capital.
    # If we have 3 NO shares, buy ~3 YES shares — not $112 worth.
    if yes_cheap and has_no and not has_yes:
        combined = pos.no_avg_price + yes_ask
        if combined < STALE_PRICE_GATE:
            spread = (1.0 - combined) * 100
            no_shares = pos.no_total / pos.no_avg_price if pos.no_avg_price > 0 else 0
            sz = round(no_shares * yes_ask, 2)  # match shares
            sz = max(1.0, sz)  # match existing shares — safety cap in _fill() handles max
            if sz >= 0.50:
                log.info(f"ENDGAME DUTCH: {title[:60]}\n"
                         f"   YES@{yes_ask:.3f} + NO avg@{pos.no_avg_price:.3f} = {combined:.3f} ({spread:.1f}% spread) sz=${sz:.2f}")
                _fill(market_id, "YES", yes_ask, sz, yes_tok, pos, f"EG-YES|{title[:35]}")
                stats.dutch += 1
                fired = True

    if no_cheap and has_yes and not has_no:
        combined = pos.yes_avg_price + no_ask
        if combined < STALE_PRICE_GATE:
            spread = (1.0 - combined) * 100
            yes_shares = pos.yes_total / pos.yes_avg_price if pos.yes_avg_price > 0 else 0
            sz = round(yes_shares * no_ask, 2)  # match shares
            sz = max(1.0, sz)  # match existing shares — safety cap in _fill() handles max
            if sz >= 0.50:
                log.info(f"ENDGAME DUTCH: {title[:60]}\n"
                         f"   NO@{no_ask:.3f} + YES avg@{pos.yes_avg_price:.3f} = {combined:.3f} ({spread:.1f}% spread) sz=${sz:.2f}")
                _fill(market_id, "NO", no_ask, sz, no_tok, pos, f"EG-NO|{title[:35]}")
                stats.dutch += 1
                fired = True

    # ── SEED RESCUE: one-sided seed dropping toward zero ─────────────────
    # If we hold only one side and it has dropped significantly from entry
    # (now <= SEED_RESCUE_MAX), buy the opposite side to cap the loss.
    # e.g. bought YES@0.69, now YES@0.12 — rather than lose $1, buy NO@0.86
    # combined = 0.12 + 0.86 = 0.98 → worst case small loss, not total wipeout.
    # Only fires once per market (rescue_key cooldown).
    SEED_RESCUE_MAX   = 0.18   # seed must have dropped to this or below
    SEED_RESCUE_DROP  = 0.25   # seed must have dropped at least this much from entry

    if has_yes and not has_no:
        dropped = pos.yes_avg_price - yes_ask
        if yes_ask <= SEED_RESCUE_MAX and dropped >= SEED_RESCUE_DROP:
            rescue_key = market_id + "_rescue"
            if now - _last_hammer.get(rescue_key, 0) > 600:  # once per 10 min
                # Buy NO to cap the loss
                combined = yes_ask + no_ask
                loss_without_rescue = pos.yes_total  # lose everything
                loss_with_rescue    = max(0, combined - 1.0) * pos.yes_total + 1.0
                if no_ask < 0.97 and combined < 1.02:  # only if NO is not already maxed
                    sz = max(1.0, round(pos.yes_total * 0.8, 2))
                    sz = min(sz, MAX_POSITION_PER_MARKET - pos.total_deployed, MAX_ONE_SIDE - pos.no_total)
                    if sz >= 1.0:
                        log.info(f"SEED RESCUE: {title[:60]}\n"
                                 f"   YES seed@{pos.yes_avg_price:.3f} dropped to {yes_ask:.3f} "
                                 f"(drop={dropped:.2f}) — buying NO@{no_ask:.3f} to cap loss\n"
                                 f"   Without rescue: -${loss_without_rescue:.2f} | "
                                 f"With rescue: combined={combined:.3f}")
                        _fill(market_id, "NO", no_ask, sz, no_tok, pos, f"RESCUE-NO|{title[:35]}")
                        _last_hammer[rescue_key] = now
                        stats.dutch += 1
                        fired = True

    if has_no and not has_yes:
        dropped = pos.no_avg_price - no_ask
        if no_ask <= SEED_RESCUE_MAX and dropped >= SEED_RESCUE_DROP:
            rescue_key = market_id + "_rescue"
            if now - _last_hammer.get(rescue_key, 0) > 600:
                combined = yes_ask + no_ask
                if yes_ask < 0.97 and combined < 1.02:
                    sz = max(1.0, round(pos.no_total * 0.8, 2))
                    sz = min(sz, MAX_POSITION_PER_MARKET - pos.total_deployed, MAX_ONE_SIDE - pos.yes_total)
                    if sz >= 1.0:
                        log.info(f"SEED RESCUE: {title[:60]}\n"
                                 f"   NO seed@{pos.no_avg_price:.3f} dropped to {no_ask:.3f} "
                                 f"(drop={dropped:.2f}) — buying YES@{yes_ask:.3f} to cap loss\n"
                                 f"   Without rescue: -${pos.no_total:.2f} | combined={combined:.3f}")
                        _fill(market_id, "YES", yes_ask, sz, yes_tok, pos, f"RESCUE-YES|{title[:35]}")
                        _last_hammer[rescue_key] = now
                        _last_hammer[market_id + "_rebal"] = now + 300  # block rebalance for 5min after rescue
                        stats.dutch += 1
                        fired = True

    # ── UNDERWATER RECOVERY: bet small on the near-certain winner ────────
    # If we hold one side that's losing, and the OTHER side is >= 0.93,
    # place a small bet ($3-5) on the winning side to claw back some losses.
    # NOT an arb — a high-probability directional play. 93%+ win rate.
    # Max $5 per recovery, once per market.
    RECOVERY_PRICE = 0.89   # winning side must be at least this
    RECOVERY_MAX_PRICE = 0.95  # but not above this (return too thin)
    RECOVERY_MAX   = 5.00   # max bet size
    RECOVERY_MIN   = 2.00   # min bet size

    is_one_sided = (has_yes != has_no)
    if is_one_sided:
        # Only recover on LIVE games — pre-game 0.89 just means favorite, not decided
        info = market_registry.get(market_id, {})
        game_is_live = info.get("live", False)
        recovery_key = market_id + "_recovery"
        if game_is_live and now - _last_hammer.get(recovery_key, 0) > 600:

            if has_no and yes_ask >= RECOVERY_PRICE and yes_ask <= RECOVERY_MAX_PRICE and pos.no_avg_price > 0.30:
                # We hold NO, YES is winning at 93%+ → buy YES to claw back
                potential_return = RECOVERY_MAX / yes_ask  # shares × $1
                potential_profit = potential_return - RECOVERY_MAX
                sz = min(RECOVERY_MAX, max(RECOVERY_MIN, pos.no_total * 0.3))
                log.info(f"UNDERWATER RECOVERY: {title[:55]}\n"
                         f"   Hold NO@{pos.no_avg_price:.3f} (losing ${pos.no_total:.2f})\n"
                         f"   Buying YES@{yes_ask:.3f} ${sz:.2f} → "
                         f"if YES wins: +${sz/yes_ask - sz:.2f} clawback")
                _fill(market_id, "YES", yes_ask, sz, yes_tok, pos,
                      f"RECOVER-YES|{title[:35]}")
                _last_hammer[recovery_key] = now
                stats.dutch += 1
                fired = True

            elif has_yes and no_ask >= RECOVERY_PRICE and no_ask <= RECOVERY_MAX_PRICE and pos.yes_avg_price > 0.30:
                # We hold YES, NO is winning at 93%+ → buy NO to claw back
                sz = min(RECOVERY_MAX, max(RECOVERY_MIN, pos.yes_total * 0.3))
                log.info(f"UNDERWATER RECOVERY: {title[:55]}\n"
                         f"   Hold YES@{pos.yes_avg_price:.3f} (losing ${pos.yes_total:.2f})\n"
                         f"   Buying NO@{no_ask:.3f} ${sz:.2f} → "
                         f"if NO wins: +${sz/no_ask - sz:.2f} clawback")
                _fill(market_id, "NO", no_ask, sz, no_tok, pos,
                      f"RECOVER-NO|{title[:35]}")
                _last_hammer[recovery_key] = now
                stats.dutch += 1
                fired = True

    # ── STUCK SEED: free slot after 15min, no hedge, keep watching ──────
    # If a one-sided seed can't find a profitable Phase 2 after 15 minutes,
    # stop counting it against the cap so new arbs aren't blocked.
    # NO hedge placed — position stays open:
    #   a) completes profitably if opposite side eventually gets cheap enough
    #   b) resolves naturally when market settles (win or lose the $1)
    SEED_FREE_MINS = 10.0
    is_one_sided = (has_yes != has_no)
    if is_one_sided and not pos.stagnant:
        age_mins = (now - pos.created_at) / 60.0
        if age_mins >= SEED_FREE_MINS:
            pos.stagnant = True  # free the cap slot, no hedge
            seed_price = yes_ask if has_yes else no_ask
            opp_price  = no_ask  if has_yes else yes_ask
            log.info(f"SEED SLOT FREED: {title[:60]}\n"
                     f"   {'YES' if has_yes else 'NO'}@{seed_price:.3f} stuck {age_mins:.0f}min "
                     f"— slot freed, still watching (combined={seed_price+opp_price:.3f})\n"
                     f"   Will complete if opposite drops to <{(1.0-seed_price):.3f}")

    return fired


def _sig_spike(market_id, title, yes_tok, no_tok, yes_ask, no_ask, ys, ns, pos):
    """
    SPIKE_DETECT: price moved >SPIKE_PCT_THRESH in SPIKE_WINDOW_SECS.

    Guards against the three failure modes observed in dry run:
    1. Warmup noise  — ignore spikes until token has 30s+ of history
    2. Negative spread — never enter a side if combined price > ENTRY_THRESHOLD
    3. Momentum-follow on extreme prices (>0.80) — skip, too risky in live sports
    """
    # Guard 1: warmup — need at least 30s of price history before trusting velocity
    now = time.time()
    if not ys.history or not ns.history: return False
    if min(now - ys.first_ts, now - ns.first_ts) < 30.0:
        return False  # still in warmup, don't trust velocity

    yv = ys.price_velocity(SPIKE_WINDOW_SECS)
    nv = ns.price_velocity(SPIKE_WINDOW_SECS)
    spike_side = None; vel = 0.0
    if abs(yv) >= SPIKE_PCT_THRESH and abs(yv) > abs(nv):
        spike_side = "YES"; vel = yv
    elif abs(nv) >= SPIKE_PCT_THRESH:
        spike_side = "NO";  vel = nv
    if not spike_side: return False

    has_yes = bool(pos.yes_fills)
    has_no  = bool(pos.no_fills)

    if vel > 0:
        # Momentum: price rose on spike_side → buy that same side
        # Guard: skip if already very expensive (>0.80) — late momentum is a trap
        side  = spike_side
        price = yes_ask if side == "YES" else no_ask
        tok   = yes_tok if side == "YES" else no_tok
        if price > 0.80:
            return False  # momentum-chasing at high price = bad risk/reward
        reason = f"momentum +{vel*100:.1f}%/{SPIKE_WINDOW_SECS}s"
    else:
        # Mean-revert: price dropped on spike_side → buy the OTHER (now cheaper) side
        side  = "NO"  if spike_side == "YES" else "YES"
        price = no_ask if spike_side == "YES" else yes_ask
        tok   = no_tok if spike_side == "YES" else yes_tok
        reason = f"mean-revert -{abs(vel)*100:.1f}%/{SPIKE_WINDOW_SECS}s"

    if price < 0.05 or price > 0.95: return False

    # Guard 2: spread sanity — never enter if it would create a negative spread
    # Check what the OTHER side's reference price is
    if side == "YES":
        other_ref = pos.no_avg_price if has_no else no_ask
    else:
        other_ref = pos.yes_avg_price if has_yes else yes_ask

    combined = price + other_ref
    if combined >= ENTRY_THRESHOLD:
        log.debug(f"SPIKE skip — combined {combined:.3f} >= threshold {ENTRY_THRESHOLD} | {title[:40]}")
        return False

    # Guard 3: don't create a large opposing position if we have a phase1 entry
    # on the other side at a price that already lost. Let Phase2 handle completion.
    if (has_yes and side == "NO") or (has_no and side == "YES"):
        existing_avg = pos.yes_avg_price if has_yes else pos.no_avg_price
        if existing_avg + price >= ENTRY_THRESHOLD:
            return False  # Phase 2 dutch signal will handle this if it becomes profitable

    _spike_spread = (1.0 - combined) * 100
    deploy_frac = min(0.15, 2.0 * (_spike_spread / 100.0) ** 0.75)
    sz = max(1.0, round(MY_BANKROLL * deploy_frac, 2))
    # ROI gate for existing two-sided positions
    ya, na = (sz, 0) if side == "YES" else (0, sz)
    yp_, np_ = (price, 0) if side == "YES" else (0, price)
    if not _check_roi_improvement(pos, ya, na, yp_, np_, f"SPK-{side}|{title[:35]}"):
        return False
    log.info(f"SPIKE: {title[:60]}\n   {reason} -> Buy {side}@{price:.3f} ${sz:.2f} | combined={combined:.3f}")
    stats.spike += 1
    _fill(market_id, side, price, sz, tok, pos, f"SPK-{side}|{title[:35]}")
    return True


def _sig_obi(market_id, title, yes_tok, no_tok, yes_ask, no_ask, ys, ns, pos):
    """
    ORDER_BOOK_IMBALANCE: bid_size >> ask_size -> buy pressure incoming.
    Size data is populated by the background _depth_refresh_loop — never
    blocks the WS thread with REST calls here.
    """
    yi, ni = ys.ob_imbalance(), ns.ob_imbalance()
    if abs(yi) < OB_IMBALANCE_MIN and abs(ni) < OB_IMBALANCE_MIN: return False

    if abs(yi) >= abs(ni) and ys.spread() <= OB_MAX_SPREAD and yes_ask > 0.05:
        imb=yi; side="YES" if yi>0 else "NO"
        price=yes_ask if side=="YES" else no_ask
        tok=yes_tok if side=="YES" else no_tok
    elif abs(ni) > 0 and ns.spread() <= OB_MAX_SPREAD and no_ask > 0.05:
        imb=ni; side="NO" if ni>0 else "YES"
        price=no_ask if side=="NO" else yes_ask
        tok=no_tok if side=="NO" else yes_tok
    else:
        return False

    if price < 0.05 or price > 0.95: return False

    # Spread sanity check — same as SPIKE guard
    other_ref = no_ask if side == "YES" else yes_ask
    if price + other_ref >= ENTRY_THRESHOLD: return False

    _obi_spread = (1.0 - price - other_ref) * 100
    deploy_frac = min(0.15, 1.5 * (_obi_spread / 100.0) ** 0.75 * min(abs(imb), 3.0))
    sz = max(1.0, round(MY_BANKROLL * deploy_frac, 2))
    # ROI gate for existing two-sided positions
    ya, na = (sz, 0) if side == "YES" else (0, sz)
    yp_, np_ = (price, 0) if side == "YES" else (0, price)
    if not _check_roi_improvement(pos, ya, na, yp_, np_, f"OBI-{side}|{title[:35]}"):
        return False
    log.info(f"OB IMBALANCE: {title[:60]}\n   imb={imb:.2f} spread={ys.spread():.3f} -> Buy {side}@{price:.3f} ${sz:.2f}")
    stats.obi += 1
    _fill(market_id, side, price, sz, tok, pos, f"OBI-{side}|{title[:35]}")
    return True


# ── RN1-STYLE: NEAR-CERTAIN HARVESTING ──────────────────────────────
def _sig_near_certain(market_id, title, yes_tok, no_tok, yes_ask, no_ask, pos, now):
    """
    RN1's bread-and-butter: buy favorites at 0.86-0.96 in live games.
    NOT guaranteed — probabilistic edge sized to survive bad streaks.
    Over hundreds of trades: ~$8K/day from $84K deployed at 95% win rate.
    """
    nc_cap = MY_BANKROLL * NC_MAX_EXPOSURE
    current_nc = ledger.directional_exposure()
    if current_nc >= nc_cap:
        return False

    candidates = []
    if NC_MIN_PRICE <= yes_ask <= NC_MAX_PRICE:
        candidates.append(("YES", yes_ask, yes_tok))
    if NC_MIN_PRICE <= no_ask <= NC_MAX_PRICE:
        candidates.append(("NO", no_ask, no_tok))
    if not candidates:
        return False

    candidates.sort(key=lambda x: -x[1])
    side, price, tok = candidates[0]

    nc_key = market_id + "_nc"
    if now - _last_nc.get(nc_key, 0) < NC_COOLDOWN:
        return False

    info = market_registry.get(market_id)
    if not info:
        return False
    # Don't require live flag — price at 0.86+ IS the signal.
    # A market doesn't reach 0.86 unless it's either a heavy pre-game
    # favorite or a live game with clear momentum. Both are valid.
    vol = info.get("volume", 0) + info.get("liquidity", 0)
    if vol < NC_MIN_VOLUME:
        return False

    if pos.yes_fills or pos.no_fills:
        return False

    sz = min(NC_TRADE_SIZE, nc_cap - current_nc, MY_BANKROLL * 0.05)
    if sz < 1.0:
        return False

    expected_return_pct = (1.0 / price - 1.0) * 100
    _last_nc[nc_key] = now
    log.info(f"NEAR-CERTAIN: {title[:60]}\n"
             f"   {side}@{price:.3f} ${sz:.2f} | +{expected_return_pct:.1f}% expected | "
             f"NC: ${current_nc+sz:.0f}/${nc_cap:.0f}")
    stats.near_certain += 1
    _fill(market_id, side, price, sz, tok, pos, f"NC-{side}|{title[:35]}")
    return True


# ── RN1-STYLE: PENNY SCOOP ──────────────────────────────────────────
def _sig_penny_scoop(market_id, title, yes_tok, no_tok, yes_ask, no_ask, pos, now):
    """
    Tiny bets on collapsing sides at 0.01-0.08. Occasional 10-50x payoff.
    RN1: 308 penny trades, $1,102 total. Cheap insurance / lottery tickets.
    """
    penny_cap = MY_BANKROLL * PENNY_MAX_TOTAL
    current_penny = ledger.penny_exposure()
    if current_penny >= penny_cap:
        return False

    candidates = []
    if 0.005 < yes_ask <= PENNY_MAX_PRICE:
        candidates.append(("YES", yes_ask, yes_tok))
    if 0.005 < no_ask <= PENNY_MAX_PRICE:
        candidates.append(("NO", no_ask, no_tok))
    if not candidates:
        return False

    candidates.sort(key=lambda x: x[1])
    side, price, tok = candidates[0]

    penny_key = market_id + "_penny"
    if now - _last_penny.get(penny_key, 0) < PENNY_COOLDOWN:
        return False

    info = market_registry.get(market_id)
    if not info:
        return False
    # Require decent volume — penny prices on dead markets are meaningless.
    # On active/live markets, pennies mean the game has swung hard.
    vol = info.get("volume", 0) + info.get("liquidity", 0)
    if vol < NC_MIN_VOLUME:
        return False

    if pos.yes_fills or pos.no_fills:
        return False

    sz = min(PENNY_TRADE_SIZE, penny_cap - current_penny)
    if sz < 0.50:
        return False

    potential_shares = sz / price
    _last_penny[penny_key] = now
    log.info(f"PENNY SCOOP: {title[:60]}\n"
             f"   {side}@{price:.3f} ${sz:.2f} -> {potential_shares:.0f} shares (${potential_shares:.0f} if wins) | "
             f"Penny: ${current_penny+sz:.0f}/${penny_cap:.0f}")
    stats.penny += 1
    _fill(market_id, side, price, sz, tok, pos, f"PENNY-{side}|{title[:35]}")
    return True


# ── RN1-STYLE: FAVORED SIDE (0.60-0.80) ─────────────────────────────
def _sig_favored_side(market_id, title, yes_tok, no_tok, yes_ask, no_ask, pos, now):
    """
    RN1's 2nd largest bucket: 21% of trades, $25.8M volume.
    Buys the favored side at 0.60-0.80 in active markets.
    
    This sits between core arb seeds (0.20-0.60) and near-certain (0.80+).
    The favored team/outcome is likely to win but there's meaningful uncertainty.
    Expected return: 25-67% per dollar if it resolves favorably.
    Expected win rate: ~70-85%.
    
    At $300 bankroll this is our biggest probabilistic profit driver.
    Size conservatively — each individual trade can lose.
    """
    fav_cap = MY_BANKROLL * FAV_MAX_EXPOSURE
    current_fav = ledger.favored_exposure()
    if current_fav >= fav_cap:
        return False

    candidates = []
    if FAV_MIN_PRICE <= yes_ask <= FAV_MAX_PRICE:
        candidates.append(("YES", yes_ask, yes_tok))
    if FAV_MIN_PRICE <= no_ask <= FAV_MAX_PRICE:
        candidates.append(("NO", no_ask, no_tok))
    if not candidates:
        return False

    # Pick the highest probability side (highest price)
    candidates.sort(key=lambda x: -x[1])
    side, price, tok = candidates[0]

    # Cooldown
    fav_key = market_id + "_fav"
    if now - _last_fav.get(fav_key, 0) < FAV_COOLDOWN:
        return False

    # Must have real volume
    info = market_registry.get(market_id)
    if not info:
        return False
    vol = info.get("volume", 0) + info.get("liquidity", 0)
    if vol < FAV_MIN_VOLUME:
        return False

    # Don't enter if we already have a position
    if pos.yes_fills or pos.no_fills:
        return False

    # The other side should NOT also be high — that would mean mispriced/arb
    # (which Dutch signal handles). We want clear directional conviction.
    other_price = no_ask if side == "YES" else yes_ask
    if other_price > 0.50:
        return False  # both sides >0.50+0.60 = >1.10, let Dutch handle it
    
    sz = min(FAV_TRADE_SIZE, fav_cap - current_fav, MY_BANKROLL * 0.03)
    if sz < 1.0:
        return False

    expected_return_pct = (1.0 / price - 1.0) * 100
    _last_fav[fav_key] = now
    log.info(f"FAVORED SIDE: {title[:60]}\n"
             f"   {side}@{price:.3f} ${sz:.2f} | +{expected_return_pct:.1f}% if wins | "
             f"other@{other_price:.3f} | Fav: ${current_fav+sz:.0f}/${fav_cap:.0f}")
    stats.favored += 1
    _fill(market_id, side, price, sz, tok, pos, f"FAV-{side}|{title[:35]}")
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


def _detect_winner_from_api(mid: str) -> str | None:
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
                 f"NC:${ledger.directional_exposure():.0f} Fav:${ledger.favored_exposure():.0f} Penny:${ledger.penny_exposure():.0f} | "
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
    global NC_TRADE_SIZE, PENNY_TRADE_SIZE, FAV_TRADE_SIZE, ENDGAME_TRADE_SIZE
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

            # Scale all sizing proportionally
            # Base ratios (at $1000 baseline):
            #   BASE_TRADE = 0.5%, MAX_TRADE = 5%, MAX_POS = 10%, MAX_ONE = 7.5%
            #   NC = 0.5%, FAV = 0.4%, PENNY = 0.2%, ENDGAME = 2%
            BASE_TRADE_SIZE = round(MY_BANKROLL * 0.005, 2)   # 0.5%
            MAX_TRADE_SIZE = round(MY_BANKROLL * 0.05, 2)      # 5%
            MAX_POSITION_PER_MARKET = round(MY_BANKROLL * 0.10, 2)  # 10%
            MAX_ONE_SIDE = round(MY_BANKROLL * 0.075, 2)       # 7.5%
            NC_TRADE_SIZE = round(MY_BANKROLL * 0.005, 2)      # 0.5%
            FAV_TRADE_SIZE = round(MY_BANKROLL * 0.004, 2)     # 0.4%
            PENNY_TRADE_SIZE = round(MY_BANKROLL * 0.002, 2)   # 0.2%
            ENDGAME_TRADE_SIZE = round(MY_BANKROLL * 0.02, 2)  # 2%

            session_pnl = MY_BANKROLL - _session_start_bankroll
            session_pct = (session_pnl / _session_start_bankroll) * 100 if _session_start_bankroll > 0 else 0

            log.info(f"BANKROLL UPDATE: ${old_bankroll:,.0f} → ${MY_BANKROLL:,.0f} ({change_pct:+.1f}%)\n"
                     f"   Session P&L: ${session_pnl:+,.2f} ({session_pct:+.1f}%)\n"
                     f"   Sizing: Base=${BASE_TRADE_SIZE:.2f} NC=${NC_TRADE_SIZE:.2f} "
                     f"Fav=${FAV_TRADE_SIZE:.2f} Penny=${PENNY_TRADE_SIZE:.2f} "
                     f"MaxTrade=${MAX_TRADE_SIZE:.2f}")

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
    p.add_argument("--bankroll",   type=float, default=MY_BANKROLL)
    p.add_argument("--min-spread", type=float, default=MIN_SPREAD_PCT)
    p.add_argument("--base-size",  type=float, default=BASE_TRADE_SIZE)
    args = p.parse_args()

    if args.live:       DRY_RUN = False
    MY_BANKROLL    = args.bankroll
    MIN_SPREAD_PCT = args.min_spread
    BASE_TRADE_SIZE= args.base_size

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
