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

ENTRY_THRESHOLD  = float(os.getenv("ENTRY_THRESHOLD", "0.97"))
MIN_SPREAD_PCT   = float(os.getenv("MIN_SPREAD_PCT", "2.5"))
PHASE1_MID_MIN   = float(os.getenv("PHASE1_MID_MIN", "0.30"))
PHASE1_MID_MAX   = float(os.getenv("PHASE1_MID_MAX", "0.69"))
# Max price the OTHER side can be at for a Phase 1 entry to make sense.
# If other_side > this, completion requires a near-impossible reversal.
# e.g. entering YES@0.68 when NO is already 0.97 → need NO to drop to 0.29. Skip.
# Good entries: other_side 0.40-0.75 means a 10-30 cent move closes the spread.
PHASE1_OTHER_MAX = float(os.getenv("PHASE1_OTHER_MAX", "0.80"))
HARVEST_THRESHOLD= float(os.getenv("HARVEST_THRESHOLD", "0.85"))
BASE_TRADE_SIZE  = float(os.getenv("BASE_TRADE_SIZE", "5.0"))
MAX_TRADE_SIZE   = float(os.getenv("MAX_TRADE_SIZE", "50.0"))
MAX_POSITION_PER_MARKET = float(os.getenv("MAX_POSITION", "500"))

# MarketPulse-derived signal thresholds
SPIKE_PCT_THRESH = float(os.getenv("SPIKE_PCT", "0.03"))    # 3% move in window
SPIKE_WINDOW_SECS= int(os.getenv("SPIKE_WINDOW", "10"))     # seconds
OB_IMBALANCE_MIN = float(os.getenv("OB_IMBALANCE", "0.6"))  # (bid-ask)/(bid+ask)
OB_MAX_SPREAD    = float(os.getenv("OB_MAX_SPREAD", "0.04"))# only trade tight books

SCAN_INTERVAL    = float(os.getenv("SCAN_INTERVAL", "3.0")) # REST fallback
MAX_MARKETS      = int(os.getenv("MAX_MARKETS", "300"))
ORDER_EXPIRY_SECS= int(os.getenv("ORDER_EXPIRY", "120"))
MARKET_REFRESH_MINS = int(os.getenv("MARKET_REFRESH", "10"))
# Per-signal cooldowns
SPIKE_COOLDOWN   = 30.0
OBI_COOLDOWN     = 20.0

# ENDGAME SWEEP: buy near-certain outcomes outright (no Dutch partner needed)
# RN1 does BOSS@0.020, Bounty Hunters@0.031, LAG Gaming@0.100 — harvesting
# the losing team's last liquidity for pennies.
ENDGAME_MAX_PRICE  = float(os.getenv("ENDGAME_MAX_PRICE", "0.15"))
ENDGAME_TRADE_SIZE = float(os.getenv("ENDGAME_SIZE", "25.0"))

# STALE PRICE GATE: allow Phase 2 completion up to 5% negative combined
# (live prices lag reality — RN1 enters at slight negative spread intentionally)
STALE_PRICE_GATE   = float(os.getenv("STALE_PRICE_GATE", "1.05"))

# FOCUS MODE: max markets with open positions at once. Once full, no new
# Phase 1 entries — concentrate on managing what we already hold.
MAX_ACTIVE_MARKETS = int(os.getenv("MAX_ACTIVE_MARKETS", "8"))

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
        if not self.yes_fills or not self.no_fills: return 0
        return 1.0 - (self.yes_avg_price + self.no_avg_price)

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
        if not self.yes_fills or not self.no_fills: return 0
        tc = self.yes_total + self.no_total
        avg = (self.yes_avg_price + self.no_avg_price) / 2 or 0.5
        return self.implied_spread * (tc / avg) - tc * 0.01

    def summary(self):
        return (
            f"YES:{len(self.yes_fills)} @{self.yes_avg_price:.3f} ${self.yes_total:.0f} | "
            f"NO:{len(self.no_fills)} @{self.no_avg_price:.3f} ${self.no_total:.0f} | "
            f"Spread:{self.implied_spread*100:.1f}% | Est:${self.estimated_profit:.2f}"
        )


class PositionLedger:
    def __init__(self):
        self.positions: Dict[str, MarketPosition] = {}
        self.lock = threading.Lock()

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

    def total_deployed(self):
        with self.lock:
            return sum(p.total_deployed for p in self.positions.values())

    def _total_nolock(self):
        """Total without acquiring lock — only call when lock already held."""
        return sum(p.total_deployed for p in self.positions.values())

    def active_count(self) -> int:
        """Number of markets where we currently hold at least one side."""
        with self.lock:
            return sum(
                1 for p in self.positions.values()
                if p.yes_fills or p.no_fills
            )

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
    def summary(self):
        m = (time.time()-self.start)/60
        return (f"Runtime:{m:.1f}m | Markets:{self.markets_seen} | WS:{self.ws_events} | "
                f"Dutch:{self.dutch} Spike:{self.spike} OBI:{self.obi} | "
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
            for tid in (s.get("tags") or "").replace(" ","").split(","):
                if tid.isdigit():
                    tag_ids.add(int(tid))
        result = list(tag_ids) if tag_ids else _FALLBACK_TAG_IDS
        _sports_tag_ids_cache = result
        log.info(f"Sports tags loaded: {len(result)} tag IDs (all sports incl. esports)")
        return result
    except Exception as e:
        log.warning(f"Could not fetch /sports: {e} — using fallback tag IDs")
        return _FALLBACK_TAG_IDS


def _fetch_events_for_tag(tag_id: int) -> List[dict]:
    """Fetch live events for one sport tag."""
    data = http_get(f"{GAMMA}/events", params={
        "tag_id": tag_id, "closed": "false", "limit": "50",
        "order": "id", "ascending": "false",
    })
    return data if isinstance(data, list) else []


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

    # Fetch all sport tags in parallel
    with ThreadPoolExecutor(max_workers=min(12, len(tag_ids))) as ex:
        futures = {ex.submit(_fetch_events_for_tag, tid): tid for tid in tag_ids}
        for f in cf_completed(futures):
            try:
                for ev in f.result():
                    slug = ev.get("slug") or str(ev.get("id",""))
                    if not slug or slug in seen_event_slugs:
                        continue
                    # Only live (in-progress) games — key insight from sports_server.py
                    if not ev.get("live"):
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
        log.info(f"fetch_live_markets: {len(flat_markets)} markets from {len(raw_events)} live events")
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
                "title":      title,
                "yes_token":  yes_tok,
                "no_token":   no_tok,
                "end_dt":     end_dt,
                "sport_type": m.get("_sport_type",""),   # moneyline/totals/spreads/btts
                "live":       m.get("_event_live", True),
                "score":      m.get("_event_score"),
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
    bs = MY_BANKROLL / 1000.0
    sp = (1.0 - yp - np_) * 100
    if sp < MIN_SPREAD_PCT: return 0, 0
    if   sp < 3:  m = 0.5
    elif sp < 5:  m = 1.0
    elif sp < 8:  m = 2.5
    elif sp < 12: m = 6.0
    elif sp < 20: m = 12.0
    else:         m = 25.0
    shares = BASE_TRADE_SIZE * m * bs
    rem = MAX_POSITION_PER_MARKET - pos.total_deployed
    if rem <= 0: return 0, 0
    shares = min(shares, rem/((yp+np_)/2*2), MAX_TRADE_SIZE/max(yp,0.01))
    shares = max(shares, 0)
    return round(shares*yp,2), round(shares*np_,2)

def _size_marginal(marg, price, other_vol):
    if marg < 0.005: return 0.0
    bs = MY_BANKROLL / 1000.0
    sp = marg * 100
    if   sp < 2:  bm = 0.3
    elif sp < 3:  bm = 0.8
    elif sp < 5:  bm = 1.5
    elif sp < 8:  bm = 3.0
    elif sp < 15: bm = 7.0
    elif sp < 30: bm = 15.0
    elif sp < 50: bm = 30.0
    else:         bm = 50.0
    pm = 3.0 if price>=0.92 else (1.8 if price>=0.85 else (2.5 if price<=0.08 else 1.0))
    om = 2.0 if other_vol>200 else (1.4 if other_vol>50 else (1.1 if other_vol>10 else 1.0))
    return min(BASE_TRADE_SIZE * bm * pm * om * bs, MAX_TRADE_SIZE)

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
    if size_usdc < 0.50: return None
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
            log.info(f"  {tag} ${size_usdc:.2f}@{price:.4f} {label} [{oid[:12]}]")
            stats.orders += 1
            return oid
        log.warning(f"  Order failed: {resp} {label}")
    except Exception as e:
        log.error(f"  Order err: {e} {label}")
    return None

def _fill(market_id, side, price, size, token, pos, label):
    if size < 0.50: return
    oid = place_limit_order(token, price, size, label)
    if DRY_RUN:
        ledger.record_fill(market_id, side, price, size)
        stats.fills += 1
    elif oid:
        pos.open_orders[oid] = {"token":token,"side":side,"price":price,"size":size,"placed_at":time.time()}

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

    if yes_ask <= 0.01 or no_ask <= 0.01: return
    if yes_ask >= 0.999 or no_ask >= 0.999: return
    if now - max(ys.last_ts, ns.last_ts) > 60: return

    pos     = ledger.get_or_create(market_id, title, yes_tok, no_tok)
    if pos.total_deployed >= MAX_POSITION_PER_MARKET: return

    has_yes = bool(pos.yes_fills)
    has_no  = bool(pos.no_fills)
    we_have_position = has_yes or has_no

    # ── FOCUS GATE: don't enter new markets if we're already managing enough ──
    if not we_have_position and ledger.active_count() >= MAX_ACTIVE_MARKETS:
        return

    # ── ENDGAME: HAMMER winning side + UPSET HEDGE cheap side ────────────
    # Only fires if we already hold a position. Checked first because
    # near-zero prices are time-sensitive.
    if we_have_position:
        if _sig_endgame(market_id, title, yes_tok, no_tok,
                        yes_ask, no_ask, pos, has_yes, has_no, now):
            if DRY_RUN:
                log.info(f"   POS: YES${pos.yes_total:.0f}@{pos.yes_avg_price:.3f} | "
                         f"NO${pos.no_total:.0f}@{pos.no_avg_price:.3f} | "
                         f"Spread:{pos.implied_spread*100:.1f}%")
            return

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


def _sig_dutch(market_id, title, yes_tok, no_tok, yes_ask, no_ask, ys, ns, pos, has_yes, has_no, now):
    """Dutch book detection + two-phase position building."""
    if not has_yes and not has_no:
        total = yes_ask + no_ask
        sp    = (1.0 - total) * 100
        # Simultaneous entry
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

        # Feasibility filter: if the other side is already too high, completion
        # requires an unrealistic reversal. e.g. YES@0.68 with NO@0.97 means
        # NO must drop to 0.29 — a 68-cent reversal in a live game. Skip it.
        # Good entries have other_side 0.40-0.79: a 10-25 cent move closes the gap.
        if other > PHASE1_OTHER_MAX:
            log.debug(f"P1 skip other@{other:.3f}>{PHASE1_OTHER_MAX} | {title[:50]}")
            return False
        # Secondary: how many cents does the other side need to fall?
        gap_needed = ENTRY_THRESHOLD - price
        if other - gap_needed > 0.30:  # other must drop >30 cents — too far
            log.debug(f"P1 skip gap too large ({other:.3f}->{gap_needed:.3f} drop={other-gap_needed:.2f}) | {title[:50]}")
            return False

        sz = max(1.0, min(BASE_TRADE_SIZE * 1.5 * (MY_BANKROLL / 1000.0), MAX_TRADE_SIZE / 3))
        log.info(f"PHASE 1: {title[:60]}\n"
                 f"   {side}@{price:.3f} ${sz:.2f} | other@{other:.3f} "
                 f"(need <{gap_needed:.3f} to lock, gap={other-gap_needed:.2f})")
        stats.dutch += 1
        _fill(market_id, side, price, sz, tok, pos, f"P1-{side}|{title[:35]}")
        return True

    # Phase 2 / Ongoing
    ys_ = ns_ = 0.0; ym = nm = None
    # Phase 2 uses STALE_PRICE_GATE (1.05) — allows up to 5% negative spread
    # on completion. Live prices lag reality; RN1 enters intentionally at
    # slight negative spread and waits for market to reprice.
    p2_gate = STALE_PRICE_GATE
    if has_no and not has_yes:
        c = pos.no_avg_price + yes_ask; ym = 1.0-c
        if c < p2_gate and ym*100 >= -5.0:
            ys_ = _size_marginal(max(ym, 0.001), yes_ask, pos.no_total)
    elif has_yes:
        nr = pos.no_avg_price if has_no else no_ask; c = yes_ask+nr; ym = 1.0-c
        if c < p2_gate and ym*100 >= -5.0:
            ys_ = _size_marginal(max(ym, 0.001), yes_ask, pos.no_total)
    if has_yes and not has_no:
        c = pos.yes_avg_price + no_ask; nm = 1.0-c
        if c < p2_gate and nm*100 >= -5.0:
            ns_ = _size_marginal(max(nm, 0.001), no_ask, pos.yes_total)
    elif has_no:
        yr = pos.yes_avg_price if has_yes else yes_ask; c = no_ask+yr; nm = 1.0-c
        if c < p2_gate and nm*100 >= -5.0:
            ns_ = _size_marginal(max(nm, 0.001), no_ask, pos.yes_total)
    if ys_ <= 0 and ns_ <= 0: return False
    rem = MAX_POSITION_PER_MARKET - pos.total_deployed
    tot = ys_ + ns_
    if tot > rem:
        ys_ = round(ys_*rem/tot,2); ns_ = round(ns_*rem/tot,2)
    phase = "PHASE 2" if (has_yes != has_no) else "ADDING"

    # ADDING gate: PRICE-BASED, not time-based.
    # We are always scanning. We only add when the price is genuinely better
    # than our last fill on that side — so every new buy lowers our avg cost.
    # This is how the GitHub copytrade bot works: track tempPrice (best seen),
    # only buy when price drops to a new low. Same principle here.
    # Phase 2 (one-sided completion) bypasses this gate — always fire.
    if phase == "ADDING":
        yes_improved = (ys_ > 0 and yes_ask < pos.last_yes_price)
        no_improved  = (ns_ > 0 and no_ask  < pos.last_no_price)
        if not yes_improved and not no_improved:
            return False  # No improvement on either side — keep scanning, skip this tick
        # Only add the sides that actually improved
        if not yes_improved: ys_ = 0.0
        if not no_improved:  ns_ = 0.0
        if ys_ <= 0 and ns_ <= 0: return False

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
    HAMMER_COOLDOWN = 60.0  # max once per minute
    if yes_winning and has_yes and now - _last_hammer[market_id + "_yes"] > HAMMER_COOLDOWN:
        sz = min(MAX_TRADE_SIZE * 0.5, MAX_POSITION_PER_MARKET - pos.total_deployed)
        sz = max(sz, 1.0)
        if sz >= 0.50:
            log.info(f"HAMMER YES: {title[:60]}\n"
                     f"   YES@{yes_ask:.3f} near-certain → adding ${sz:.2f}")
            _fill(market_id, "YES", yes_ask, sz, yes_tok, pos, f"HAMMER-YES|{title[:35]}")
            _last_hammer[market_id + "_yes"] = now
            stats.dutch += 1
            fired = True

    if no_winning and has_no and now - _last_hammer[market_id + "_no"] > HAMMER_COOLDOWN:
        sz = min(MAX_TRADE_SIZE * 0.5, MAX_POSITION_PER_MARKET - pos.total_deployed)
        sz = max(sz, 1.0)
        if sz >= 0.50:
            log.info(f"HAMMER NO: {title[:60]}\n"
                     f"   NO@{no_ask:.3f} near-certain → adding ${sz:.2f}")
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
            if sz >= 0.50:
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
            if sz >= 0.50:
                log.info(f"UPSET HEDGE: {title[:60]}\n"
                         f"   NO@{no_ask:.3f} (upset insurance, hold YES@{pos.yes_avg_price:.3f})")
                _fill(market_id, "NO", no_ask, sz, no_tok, pos, f"HEDGE-NO|{title[:35]}")
                _last_hammer[hedge_key] = now
                stats.dutch += 1
                fired = True

    # ── ENDGAME DUTCH: complete existing one-sided position ───────────
    # The other side just collapsed — lock in the spread at whatever we can.
    if yes_cheap and has_no and not has_yes:
        combined = pos.no_avg_price + yes_ask
        if combined < STALE_PRICE_GATE:
            spread = (1.0 - combined) * 100
            sz = max(1.0, BASE_TRADE_SIZE * 3 * (MY_BANKROLL / 1000.0))
            sz = min(sz, MAX_POSITION_PER_MARKET - pos.total_deployed)
            if sz >= 0.50:
                log.info(f"ENDGAME DUTCH: {title[:60]}\n"
                         f"   YES@{yes_ask:.3f} + NO avg@{pos.no_avg_price:.3f} = {combined:.3f} ({spread:.1f}% spread)")
                _fill(market_id, "YES", yes_ask, sz, yes_tok, pos, f"EG-YES|{title[:35]}")
                stats.dutch += 1
                fired = True

    if no_cheap and has_yes and not has_no:
        combined = pos.yes_avg_price + no_ask
        if combined < STALE_PRICE_GATE:
            spread = (1.0 - combined) * 100
            sz = max(1.0, BASE_TRADE_SIZE * 3 * (MY_BANKROLL / 1000.0))
            sz = min(sz, MAX_POSITION_PER_MARKET - pos.total_deployed)
            if sz >= 0.50:
                log.info(f"ENDGAME DUTCH: {title[:60]}\n"
                         f"   NO@{no_ask:.3f} + YES avg@{pos.yes_avg_price:.3f} = {combined:.3f} ({spread:.1f}% spread)")
                _fill(market_id, "NO", no_ask, sz, no_tok, pos, f"EG-NO|{title[:35]}")
                stats.dutch += 1
                fired = True

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

    sz = max(1.0, min(BASE_TRADE_SIZE * 2.0 * (MY_BANKROLL / 1000.0), MAX_TRADE_SIZE / 2))
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

    sz = max(1.0, min(BASE_TRADE_SIZE * abs(imb) * 3 * (MY_BANKROLL / 1000.0), MAX_TRADE_SIZE / 2))
    log.info(f"OB IMBALANCE: {title[:60]}\n   imb={imb:.2f} spread={ys.spread():.3f} -> Buy {side}@{price:.3f} ${sz:.2f}")
    stats.obi += 1
    _fill(market_id, side, price, sz, tok, pos, f"OBI-{side}|{title[:35]}")
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
        except Exception as e: log.debug(f"eval err: {e}")


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
                    nonlocal sub_msg
                    # Subscribe in chunks of 500 (WS max per connection)
                    for i in range(0, len(all_tokens), 500):
                        chunk = all_tokens[i:i+500]
                        ws.send(json.dumps({"assets_ids": chunk, "type": "market"}))
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
    Polls prices via REST every 5s for any market where we hold an open
    position. This ensures Phase 2 fires even when WS goes quiet (halftime,
    low trading volume, etc). Runs independently of the WS feed.
    """
    while True:
        time.sleep(5)
        try:
            with ledger.lock:
                open_positions = [
                    (mid, pos) for mid, pos in ledger.positions.items()
                    if (pos.yes_fills or pos.no_fills) and pos.total_deployed < MAX_POSITION_PER_MARKET
                ]
            if not open_positions:
                continue
            for mid, pos in open_positions:
                info = market_registry.get(mid)
                if not info:
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
        log.info(f"STATS: {stats.summary()}")
        ledger.print_all()

def _market_refresh_loop():
    while True:
        time.sleep(MARKET_REFRESH_MINS * 60)
        try:
            ms = fetch_live_markets()
            register_markets(ms)
            log.info(f"Market list refreshed: {len(market_registry)} markets")
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

    threading.Thread(target=_stats_loop,           daemon=True).start()
    threading.Thread(target=_market_refresh_loop,  daemon=True).start()
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
