#!/usr/bin/env python3
"""
PolyArb v7 — ALL-IN-ONE TRIAD ARBITRAGE BOT
══════════════════════════════════════════════
RN1's decoded strategy in a single file.

CORE STRATEGY:
  For every soccer match, buy NO on all three outcomes:
    1. "Will Team A win on DATE?" → buy NO
    2. "Will Team B win on DATE?" → buy NO
    3. "Will Team A vs. Team B end in a draw?" → buy NO

  Payout: EXACTLY $2.00 regardless of match result
  Profit = $2.00 - total_cost_of_three_legs

SUPPLEMENTARY:
  Also buys NO on non-triad markets (O/U, spreads, esports, NBA props)
  for additional coverage. These are standard directional NO bets.

ARCHITECTURE:
  1. Discovery: REST poll Gamma API → fetch all active markets
  2. Matcher:   Group into triads (draw anchor → find matching win markets)
  3. Pricer:    Fetch NO ask from CLOB order books for all 3 legs
  4. Executor:  Buy legs when triad cost < threshold
  5. Accumulator: Re-buy legs on cooldown to build positions
  6. Ledger:    Track positions, P&L, triad completion status

Usage:
  python3.11 arb_v7.py --bankroll 1000 --fresh          # dry run, fresh start
  python3.11 arb_v7.py --bankroll 1000 --live            # live trading
  python3.11 arb_v7.py --bankroll 1000 --scan-only       # just scan, don't trade
"""

import os, sys, json, time, math, re, signal, logging, threading, random, hashlib
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Set
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ═════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═════════════════════════════════════════════════════════════════════
KEYS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "keys.env")
if os.path.exists(KEYS_FILE):
    with open(KEYS_FILE) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

PRIVATE_KEY = os.getenv("PK", os.getenv("PRIVATE_KEY", ""))
CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"

# ─── BANKROLL & LIMITS ──────────────────────────────────────────────
MY_BANKROLL        = float(os.getenv("BANKROLL", "1000"))
DRY_RUN            = os.getenv("DRY_RUN", "true").lower() in ("true", "1", "yes")
MAX_DEPLOY_PCT     = float(os.getenv("MAX_DEPLOY", "0.90"))        # 90% max deployed
MAX_PER_MATCH      = float(os.getenv("MAX_MATCH", "0"))            # 0 = auto (15% bankroll)
MAX_PER_MARKET     = float(os.getenv("MAX_POS", "100"))            # $100 max per single market

# ─── TRIAD STRATEGY ─────────────────────────────────────────────────
TRIAD_MAX_COST     = float(os.getenv("TRIAD_MAX", "2.04"))         # buy if 3-leg cost < this
TRIAD_IDEAL_COST   = float(os.getenv("TRIAD_IDEAL", "1.98"))       # ideal: guaranteed profit
TRIAD_ACCUM_CD     = float(os.getenv("TRIAD_ACCUM_CD", "45"))      # seconds between adds per leg
TRIAD_ENTRY_CD     = float(os.getenv("TRIAD_ENTRY_CD", "5"))       # seconds min age before adding

# ─── SUPPLEMENTARY ──────────────────────────────────────────────────
SUPP_ENABLED       = os.getenv("SUPP", "true").lower() in ("true", "1")
SUPP_MAX_PCT       = float(os.getenv("SUPP_MAX", "0.25"))          # max 25% in supplementary
SUPP_COOLDOWN      = float(os.getenv("SUPP_CD", "60"))             # per-market cooldown
SUPP_MIN_PRICE     = float(os.getenv("SUPP_MIN_P", "0.10"))        # don't buy NO below 10¢
SUPP_MAX_PRICE     = float(os.getenv("SUPP_MAX_P", "0.95"))        # don't buy NO above 95¢

# ─── RATE LIMITS ─────────────────────────────────────────────────────
MAX_FILLS_MIN      = int(os.getenv("MAX_FILLS_MIN", "30"))
MAX_NEW_HR         = int(os.getenv("MAX_NEW_HR", "90"))

# ─── TIMING ──────────────────────────────────────────────────────────
REFRESH_SEC        = int(os.getenv("REFRESH_SEC", "120"))           # re-discover markets
SCAN_SEC           = int(os.getenv("SCAN_SEC", "10"))               # scan triads for prices
STATS_SEC          = int(os.getenv("STATS_SEC", "30"))              # log stats
SAVE_SEC           = int(os.getenv("SAVE_SEC", "30"))               # save ledger

# ─── FUTURES FILTER (skip season-long markets) ──────────────────────
FUTURES_KEYWORDS = [
    'finals', 'stanley cup', 'world cup', 'super bowl', 'championship',
    'premier league winner', 'serie a winner', 'bundesliga winner',
    'ligue 1 winner', 'la liga winner', 'champion', 'playoffs',
    'division', 'conference', 'nomination', 'presidential', 'election',
    'award', 'mvp', 'rookie', 'draft', 'qualify', 'relegat', 'top goal',
    'golden boot', 'ballon', 'best player', 'most valuable',
]

# ═════════════════════════════════════════════════════════════════════
# LOGGING
# ═════════════════════════════════════════════════════════════════════
os.makedirs("logs", exist_ok=True)
_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join("logs", f"arb_v7_{_ts}.log")

log = logging.getLogger("v7")
log.setLevel(logging.DEBUG)
_fh = logging.FileHandler(LOG_FILE)
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(_fh)
_sh = logging.StreamHandler()
_sh.setLevel(logging.INFO)
_sh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(_sh)

# ═════════════════════════════════════════════════════════════════════
# API LAYER
# ═════════════════════════════════════════════════════════════════════
_api_calls = 0

def api_get(url, timeout=12, retries=2):
    global _api_calls
    for attempt in range(retries + 1):
        try:
            _api_calls += 1
            req = Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Accept": "application/json",
            })
            resp = urlopen(req, timeout=timeout)
            return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            if e.code == 429:
                time.sleep(2 ** attempt * 2)
                continue
            if e.code == 403:
                time.sleep(1 + attempt)
                continue
            if e.code >= 500:
                time.sleep(1)
                continue
            return None
        except (URLError, TimeoutError, OSError):
            time.sleep(0.5 * (attempt + 1))
            continue
    return None


def fetch_book(token_id: str) -> Tuple[Optional[float], float]:
    """Fetch order book → (best_ask_price, total_ask_depth_usd)."""
    url = f"{CLOB}/book?token_id={token_id}"
    book = api_get(url, timeout=8)
    if not book:
        return None, 0

    asks = book.get("asks", [])
    if not asks:
        return None, 0

    best = None
    depth = 0
    for a in asks:
        try:
            p = float(a["price"])
            s = float(a["size"])
            if p > 0 and s > 0:
                if best is None or p < best:
                    best = p
                depth += s * p
        except (KeyError, ValueError, TypeError):
            continue
    return best, round(depth, 2)


# ═════════════════════════════════════════════════════════════════════
# RATE LIMITER
# ═════════════════════════════════════════════════════════════════════
_fill_ts: List[float] = []
_new_ts: List[float] = []

def rate_ok(is_new=False) -> bool:
    now = time.time()
    while _fill_ts and now - _fill_ts[0] > 60:
        _fill_ts.pop(0)
    while _new_ts and now - _new_ts[0] > 3600:
        _new_ts.pop(0)
    if len(_fill_ts) >= MAX_FILLS_MIN:
        return False
    if is_new and len(_new_ts) >= MAX_NEW_HR:
        return False
    return True


# ═════════════════════════════════════════════════════════════════════
# RN1 SIZING CURVE
# ═════════════════════════════════════════════════════════════════════
def rn1_size(price: float) -> float:
    """RN1-calibrated size. At $1K: median ~$8-9, max ~$20 initial."""
    scale = MY_BANKROLL / 853  # his startup bankroll
    if   price < 0.10: base = 3.0
    elif price < 0.20: base = 5.0
    elif price < 0.30: base = 7.0
    elif price < 0.50: base = 9.0
    elif price < 0.70: base = 11.0
    elif price < 0.80: base = 13.0
    elif price < 0.90: base = 15.0
    else:               base = 18.0
    return round(min(base * scale, MY_BANKROLL * 0.02), 2)


# ═════════════════════════════════════════════════════════════════════
# MARKET DISCOVERY
# ═════════════════════════════════════════════════════════════════════
def fetch_all_markets(max_markets=35000) -> List[dict]:
    """Fetch all active markets from Gamma API."""
    log.info("Fetching active markets from Gamma...")
    all_m = []
    offset = 0
    while len(all_m) < max_markets:
        url = f"{GAMMA}/markets?limit=100&offset={offset}&active=true&closed=false"
        data = api_get(url, timeout=15)
        if not data or not isinstance(data, list) or len(data) == 0:
            break
        for m in data:
            if not m.get("active") or m.get("closed"):
                continue
            clob_ids = m.get("clobTokenIds", [])
            if isinstance(clob_ids, str):
                try: clob_ids = json.loads(clob_ids)
                except: clob_ids = []
            if not isinstance(clob_ids, list) or len(clob_ids) < 2:
                continue
            m["_clob_yes"] = str(clob_ids[0])
            m["_clob_no"] = str(clob_ids[1])
            m["_title"] = m.get("question", "") or m.get("title", "")
            all_m.append(m)
        if len(data) < 100:
            break
        offset += 100
        time.sleep(0.15)
    log.info(f"Fetched {len(all_m):,} active markets with tokens")
    return all_m


# ═════════════════════════════════════════════════════════════════════
# TRIAD BUILDER
# ═════════════════════════════════════════════════════════════════════
def _normalize(s):
    return re.sub(r'[^a-z0-9\s]', '', s.lower()).strip()

def _is_futures(title):
    tl = title.lower()
    return any(kw in tl for kw in FUTURES_KEYWORDS)


class Triad:
    __slots__ = [
        "team_a", "team_b", "match_key",
        "win_a", "win_b", "draw",           # market dicts
        "tok_a", "tok_b", "tok_d",           # NO token IDs
        "price_a", "price_b", "price_d",     # live NO ask prices
        "depth_a", "depth_b", "depth_d",
        "last_price_time",
        "leg_deployed", "leg_shares", "leg_fills",
        "last_fill_time",
    ]

    def __init__(self, team_a, team_b, win_a, win_b, draw):
        self.team_a = team_a
        self.team_b = team_b
        na, nb = _normalize(team_a), _normalize(team_b)
        self.match_key = f"{min(na,nb)}|{max(na,nb)}"
        self.win_a = win_a
        self.win_b = win_b
        self.draw = draw
        self.tok_a = win_a["_clob_no"]
        self.tok_b = win_b["_clob_no"]
        self.tok_d = draw["_clob_no"]
        self.price_a = self.price_b = self.price_d = None
        self.depth_a = self.depth_b = self.depth_d = 0.0
        self.last_price_time = 0
        self.leg_deployed = {"a": 0.0, "b": 0.0, "d": 0.0}
        self.leg_shares = {"a": 0.0, "b": 0.0, "d": 0.0}
        self.leg_fills = {"a": 0, "b": 0, "d": 0}
        self.last_fill_time = {"a": 0.0, "b": 0.0, "d": 0.0}

    @property
    def cost(self):
        if self.price_a and self.price_b and self.price_d:
            return round(self.price_a + self.price_b + self.price_d, 4)
        return None

    @property
    def edge(self):
        c = self.cost
        return round(2.00 - c, 4) if c else None

    @property
    def edge_pct(self):
        c = self.cost
        return round((2.00 - c) / c * 100, 2) if c and c > 0 else None

    @property
    def total_deployed(self):
        return sum(self.leg_deployed.values())

    @property
    def is_entered(self):
        return any(v > 0 for v in self.leg_deployed.values())

    @property
    def is_complete(self):
        return all(v > 0 for v in self.leg_deployed.values())

    def leg_price(self, leg):
        return {"a": self.price_a, "b": self.price_b, "d": self.price_d}.get(leg)

    def leg_token(self, leg):
        return {"a": self.tok_a, "b": self.tok_b, "d": self.tok_d}.get(leg)

    def leg_market(self, leg):
        return {"a": self.win_a, "b": self.win_b, "d": self.draw}.get(leg)

    def short_name(self):
        a = self.team_a[:20] if len(self.team_a) > 20 else self.team_a
        b = self.team_b[:20] if len(self.team_b) > 20 else self.team_b
        return f"{a} v {b}"


def build_triads(markets: List[dict]) -> Tuple[List[Triad], List[dict]]:
    """Group markets into triads. Returns (triads, leftover_markets)."""
    win_by_team = defaultdict(list)
    draws = []
    triad_ids: Set[str] = set()

    for m in markets:
        title = m["_title"]

        # Draw: "Will X vs. Y end in a draw?"
        dm = re.match(r'Will (.+?)\s+vs\.?\s+(.+?)\s+end in a draw\s*\??$', title, re.I)
        if dm:
            draws.append({
                "m": m,
                "a": dm.group(1).strip(),
                "b": dm.group(2).strip(),
                "na": _normalize(dm.group(1)),
                "nb": _normalize(dm.group(2)),
            })
            continue

        # Win: "Will X win on DATE?" (not futures)
        wm = re.match(r'Will (.+?) win(?:\s+on\s+\d{4}-\d{2}-\d{2})?\s*\??$', title, re.I)
        if wm and not _is_futures(title):
            team = wm.group(1).strip()
            win_by_team[_normalize(team)].append(m)

    # Match draws to wins
    triads = []
    for d in draws:
        wa = win_by_team.get(d["na"])
        wb = win_by_team.get(d["nb"])

        # Fuzzy fallback
        if not wa:
            for k, v in win_by_team.items():
                if d["na"] in k or k in d["na"]:
                    wa = v; break
        if not wb:
            for k, v in win_by_team.items():
                if d["nb"] in k or k in d["nb"]:
                    wb = v; break

        if wa and wb:
            best_a = max(wa, key=lambda x: float(x.get("volume", 0) or 0))
            best_b = max(wb, key=lambda x: float(x.get("volume", 0) or 0))
            t = Triad(d["a"], d["b"], best_a, best_b, d["m"])
            triads.append(t)
            triad_ids.update([
                best_a.get("id", ""),
                best_b.get("id", ""),
                d["m"].get("id", ""),
            ])

    leftover = [m for m in markets if m.get("id", "") not in triad_ids]
    log.info(f"TRIADS: {len(triads)} complete | {len(draws)} draws | "
             f"{len(win_by_team)} win-teams | {len(leftover)} leftover")
    return triads, leftover


# ═════════════════════════════════════════════════════════════════════
# LEDGER
# ═════════════════════════════════════════════════════════════════════
class Position:
    def __init__(self, market_id, title=""):
        self.market_id = market_id
        self.title = title
        self.fills: List[Tuple[float, float, float]] = []
        self.cost = 0.0
        self.shares = 0.0
        self.avg_price = 0.0
        self.resolved = False
        self.triad_key = ""
        self.leg = ""          # "a", "b", "d", or ""
        self.created_at = time.time()

    def add_fill(self, price, size):
        sh = size / price if price > 0 else 0
        self.fills.append((price, size, time.time()))
        self.cost += size
        self.shares += sh
        self.avg_price = self.cost / self.shares if self.shares > 0 else 0

    def to_dict(self):
        return {k: getattr(self, k) for k in
                ["market_id","title","fills","cost","shares","avg_price",
                 "resolved","triad_key","leg","created_at"]}

    @classmethod
    def from_dict(cls, d):
        p = cls(d["market_id"], d.get("title",""))
        for k in ["fills","cost","shares","avg_price","resolved",
                   "triad_key","leg","created_at"]:
            if k in d: setattr(p, k, d[k])
        return p


class Ledger:
    def __init__(self):
        self.positions: Dict[str, Position] = {}
        self._lock = threading.Lock()

    def get(self, mid) -> Optional[Position]:
        return self.positions.get(mid)

    def record(self, mid, price, size, title="", triad_key="", leg=""):
        with self._lock:
            if mid not in self.positions:
                self.positions[mid] = Position(mid, title)
            p = self.positions[mid]
            p.add_fill(price, size)
            if triad_key: p.triad_key = triad_key
            if leg: p.leg = leg
            if title and not p.title: p.title = title

    def deployed(self):
        return sum(p.cost for p in self.positions.values() if not p.resolved)

    def active_count(self):
        return sum(1 for p in self.positions.values() if not p.resolved and p.cost > 0)

    def match_deployed(self, triad_key):
        return sum(p.cost for p in self.positions.values()
                   if not p.resolved and p.triad_key == triad_key)

    def supp_deployed(self):
        return sum(p.cost for p in self.positions.values()
                   if not p.resolved and not p.triad_key)

    def triad_legs(self, triad_key) -> Dict[str, Position]:
        return {p.leg: p for p in self.positions.values()
                if not p.resolved and p.triad_key == triad_key and p.leg}

    def save(self, path="ledger.json"):
        with self._lock:
            data = {k: v.to_dict() for k, v in self.positions.items()}
        with open(path, "w") as f:
            json.dump(data, f, indent=2)

    def load(self, path="ledger.json"):
        if not os.path.exists(path): return
        with open(path) as f:
            data = json.load(f)
        with self._lock:
            for k, v in data.items():
                self.positions[k] = Position.from_dict(v)
        log.info(f"Loaded {len(self.positions)} positions")


ledger = Ledger()

# ═════════════════════════════════════════════════════════════════════
# STATS
# ═════════════════════════════════════════════════════════════════════
class Stats:
    def __init__(self):
        self.start = time.time()
        self.triads_total = 0
        self.triads_profitable = 0
        self.triads_entered = 0
        self.triads_complete = 0
        self.fills_triad = 0
        self.fills_accum = 0
        self.fills_supp = 0
        self.rej_rate = 0
        self.rej_deploy = 0
        self.rej_match = 0
        self.rej_edge = 0
        self.rej_price = 0
        self.scans = 0

    @property
    def runtime_min(self):
        return (time.time() - self.start) / 60

    @property
    def total_fills(self):
        return self.fills_triad + self.fills_accum + self.fills_supp

    def line(self):
        return (
            f"STATS: Runtime:{self.runtime_min:.1f}m | "
            f"Triads:{self.triads_total} prof:{self.triads_profitable} "
            f"entered:{self.triads_entered} done:{self.triads_complete} | "
            f"Fills[T:{self.fills_triad} A:{self.fills_accum} S:{self.fills_supp} "
            f"={self.total_fills}] | "
            f"Rej[rate:{self.rej_rate} dep:{self.rej_deploy} match:{self.rej_match} "
            f"edge:{self.rej_edge} price:{self.rej_price}] | "
            f"${ledger.deployed():.2f} | Active:{ledger.active_count()} | "
            f"BRL:${MY_BANKROLL:,.0f}"
        )

stats = Stats()

# ═════════════════════════════════════════════════════════════════════
# ORDER EXECUTION
# ═════════════════════════════════════════════════════════════════════
_clob_client = None

def get_clob():
    global _clob_client
    if _clob_client: return _clob_client
    if not PRIVATE_KEY or PRIVATE_KEY == "YOUR_PRIVATE_KEY_HERE":
        raise RuntimeError("Set PK in keys.env for live trading")
    from py_clob_client.client import ClobClient
    _clob_client = ClobClient(CLOB, key=PRIVATE_KEY, chain_id=137,
                               signature_type=2, funder=os.getenv("FUNDER"))
    _clob_client.set_api_creds(_clob_client.create_or_derive_api_creds())
    return _clob_client


def place_buy(token_id: str, price: float, size_usd: float, label: str = "") -> Optional[str]:
    """Place a NO buy order. Returns order_id or None."""
    shares = round(size_usd / price, 1)
    if shares < 1:
        return None

    if DRY_RUN:
        oid = f"dry_{int(time.time()*1000)}"
        log.info(f"  {'DRY':>4s} BUY NO @{price:.3f} ${size_usd:.2f} ({shares:.0f}sh) | {label}")
        return oid

    try:
        from py_clob_client.order import OrderArgs
        from py_clob_client.constants import BUY
        client = get_clob()
        args = OrderArgs(price=round(price, 2), size=shares, side=BUY, token_id=token_id)
        signed = client.create_order(args)
        signed["orderType"] = "FOK"
        resp = client.post_order(signed)
        if resp and resp.get("orderID"):
            log.info(f"  LIVE BUY NO @{price:.3f} ${size_usd:.2f} | {label} | OID:{resp['orderID'][:12]}")
            return resp["orderID"]
        log.warning(f"  ORDER FAILED: {resp}")
        return None
    except Exception as e:
        log.error(f"  ORDER ERROR: {e}")
        return None


# ═════════════════════════════════════════════════════════════════════
# TRIAD EXECUTION ENGINE
# ═════════════════════════════════════════════════════════════════════
def _max_match():
    """Max USD per match (triad)."""
    return MAX_PER_MATCH if MAX_PER_MATCH > 0 else MY_BANKROLL * 0.15

def _cap_size(size, triad, leg):
    """Apply all caps to a proposed trade size."""
    max_deploy = MY_BANKROLL * MAX_DEPLOY_PCT
    max_match = _max_match()

    size = min(size, max_deploy - ledger.deployed())
    size = min(size, max_match - ledger.match_deployed(triad.match_key))
    size = min(size, MAX_PER_MARKET - triad.leg_deployed.get(leg, 0))
    return max(size, 0)


def execute_leg(triad: Triad, leg: str) -> bool:
    """Buy one leg of a triad. Returns True if filled."""
    price = triad.leg_price(leg)
    token = triad.leg_token(leg)
    market = triad.leg_market(leg)
    if not price or not token or price <= 0:
        stats.rej_price += 1
        return False

    mid = market.get("id", "")
    title = market.get("_title", "")
    existing = ledger.get(mid)
    is_new = existing is None or existing.cost < 0.50

    if not rate_ok(is_new):
        stats.rej_rate += 1
        return False

    if ledger.deployed() >= MY_BANKROLL * MAX_DEPLOY_PCT:
        stats.rej_deploy += 1
        return False

    if ledger.match_deployed(triad.match_key) >= _max_match():
        stats.rej_match += 1
        return False

    # Size
    size = rn1_size(price)
    size = _cap_size(size, triad, leg)
    if size < 1.0:
        return False

    lbl = f"{'NEW' if is_new else 'ADD'}-{leg.upper()}|{triad.short_name()}"
    oid = place_buy(token, price, size, lbl)
    if not oid:
        return False

    # Record
    ledger.record(mid, price, size, title, triad.match_key, leg)
    triad.leg_deployed[leg] += size
    triad.leg_shares[leg] += size / price
    triad.leg_fills[leg] += 1
    triad.last_fill_time[leg] = time.time()
    _fill_ts.append(time.time())
    if is_new:
        _new_ts.append(time.time())
        stats.fills_triad += 1
    else:
        stats.fills_accum += 1
    return True


def process_triad(triad: Triad):
    """Evaluate a triad and execute trades if profitable."""
    cost = triad.cost
    if cost is None:
        return

    # Skip if too expensive
    if cost > TRIAD_MAX_COST:
        stats.rej_edge += 1
        return

    now = time.time()

    # Sync ledger state into triad object
    legs = ledger.triad_legs(triad.match_key)
    for leg_key in ["a", "b", "d"]:
        lp = legs.get(leg_key)
        if lp:
            triad.leg_deployed[leg_key] = lp.cost
            triad.leg_shares[leg_key] = lp.shares

    for leg in ["a", "b", "d"]:
        deployed = triad.leg_deployed[leg]

        if deployed < 0.50:
            # New leg — enter
            execute_leg(triad, leg)
        else:
            # Existing leg — accumulate on cooldown
            if deployed >= MAX_PER_MARKET:
                continue
            if now - triad.last_fill_time[leg] < TRIAD_ACCUM_CD:
                continue
            # Only accumulate if triad still looks good
            if cost <= TRIAD_MAX_COST:
                execute_leg(triad, leg)


# ═════════════════════════════════════════════════════════════════════
# SUPPLEMENTARY ENGINE (non-triad NO buys)
# ═════════════════════════════════════════════════════════════════════
_last_supp: Dict[str, float] = defaultdict(float)

def process_supplementary(market: dict):
    """Buy NO on non-triad markets (O/U, spreads, esports, etc.)."""
    mid = market.get("id", "")
    title = market.get("_title", "")
    token = market.get("_clob_no", "")
    if not token:
        return

    now = time.time()
    if now - _last_supp.get(mid, 0) < SUPP_COOLDOWN:
        return

    supp_cap = MY_BANKROLL * SUPP_MAX_PCT
    if ledger.supp_deployed() >= supp_cap:
        return
    if ledger.deployed() >= MY_BANKROLL * MAX_DEPLOY_PCT:
        return

    # Skip futures and non-sports
    if _is_futures(title):
        return

    # Only process sports-adjacent titles
    tl = title.lower()
    is_sports = any(w in tl for w in [
        'vs', 'o/u', 'spread', 'over/under', 'rebounds', 'assists',
        'points', 'goals', 'counter-strike', 'dota', 'valorant', 'lol',
        'nba', 'nhl', 'nfl', 'mlb',
    ])
    if not is_sports:
        return

    # Fetch price
    price, depth = fetch_book(token)
    if not price or price < SUPP_MIN_PRICE or price > SUPP_MAX_PRICE:
        return

    _last_supp[mid] = now

    pos = ledger.get(mid)
    is_new = pos is None or pos.cost < 0.50
    if not is_new and pos.cost >= MAX_PER_MARKET:
        return

    if not rate_ok(is_new):
        stats.rej_rate += 1
        return

    size = rn1_size(price)
    size = min(size, supp_cap - ledger.supp_deployed())
    size = min(size, MY_BANKROLL * MAX_DEPLOY_PCT - ledger.deployed())
    if pos:
        size = min(size, MAX_PER_MARKET - pos.cost)
    if size < 1.0:
        return

    lbl = f"SUPP|{title[:50]}"
    oid = place_buy(token, price, size, lbl)
    if oid:
        ledger.record(mid, price, size, title)
        _fill_ts.append(time.time())
        if is_new: _new_ts.append(time.time())
        stats.fills_supp += 1


# ═════════════════════════════════════════════════════════════════════
# DASHBOARD
# ═════════════════════════════════════════════════════════════════════
def print_dashboard(triads: List[Triad]):
    """Print inline dashboard to terminal."""
    os.system("clear" if os.name != "nt" else "cls")
    now = datetime.now().strftime("%H:%M:%S")

    print(f"\033[1;96m╔══════════════════════════════════════════════════════════════════════╗\033[0m")
    print(f"\033[1;96m║  PolyArb v7 — TRIAD ARBITRAGE    {now}    {'DRY RUN' if DRY_RUN else 'LIVE':>8s}  ║\033[0m")
    print(f"\033[1;96m╚══════════════════════════════════════════════════════════════════════╝\033[0m")

    s = stats
    print(f"  Up:{s.runtime_min:.1f}m  Triads:{s.triads_total} prof:{s.triads_profitable} "
          f"entered:{s.triads_entered} done:{s.triads_complete}  "
          f"Fills:T{s.fills_triad}+A{s.fills_accum}+S{s.fills_supp}={s.total_fills}")
    print(f"  Deploy:${ledger.deployed():.2f}/${MY_BANKROLL*MAX_DEPLOY_PCT:.0f}  "
          f"Active:{ledger.active_count()}  "
          f"Rej[r:{s.rej_rate} d:{s.rej_deploy} m:{s.rej_match} e:{s.rej_edge} p:{s.rej_price}]")

    # Show best triads
    priced = [t for t in triads if t.cost is not None]
    priced.sort(key=lambda t: t.cost)

    entered = [t for t in priced if t.is_entered]
    profitable = [t for t in priced if t.edge and t.edge > 0]

    if entered:
        print(f"\n  \033[1mACTIVE TRIADS\033[0m ({len(entered)})")
        print(f"  {'Match':<35s} {'A':>6s} {'B':>6s} {'D':>6s} {'Cost':>7s} {'Edge':>7s} {'Legs':>4s} {'Deployed':>8s}")
        for t in entered[:15]:
            c = t.cost or 0
            e = t.edge or 0
            legs = sum(1 for v in t.leg_deployed.values() if v > 0)
            clr = "\033[92m" if e > 0 else ("\033[93m" if e > -0.03 else "\033[91m")
            print(f"  {t.short_name():<35s} "
                  f"@{t.price_a or 0:.3f} @{t.price_b or 0:.3f} @{t.price_d or 0:.3f} "
                  f"${c:.3f} {clr}{e:+.3f}\033[0m {legs}/3  ${t.total_deployed:.2f}")

    if profitable and not entered:
        print(f"\n  \033[1mBEST OPPORTUNITIES\033[0m ({len(profitable)} profitable)")
        for t in profitable[:10]:
            print(f"  {t.short_name():<35s} "
                  f"@{t.price_a:.3f}+@{t.price_b:.3f}+@{t.price_d:.3f} "
                  f"=${t.cost:.3f} \033[92m{t.edge:+.3f}\033[0m")

    # Show supplementary
    supp_d = ledger.supp_deployed()
    if supp_d > 0:
        supp_pos = [(k, p) for k, p in ledger.positions.items()
                    if not p.resolved and not p.triad_key and p.cost > 0]
        print(f"\n  \033[1mSUPPLEMENTARY\033[0m ({len(supp_pos)} positions, ${supp_d:.2f})")

    # Footer
    total_shares = sum(p.shares for p in ledger.positions.values() if not p.resolved)
    print(f"\n  {'═'*68}")
    print(f"  DEPLOYED: ${ledger.deployed():.2f}  SHARES: {total_shares:.0f}  "
          f"IF-ALL-WIN: ${total_shares:.2f}  SUPP: ${supp_d:.2f}")


# ═════════════════════════════════════════════════════════════════════
# MAIN LOOP
# ═════════════════════════════════════════════════════════════════════
def main():
    import argparse
    p = argparse.ArgumentParser(description="PolyArb v7 — Triad Arbitrage")
    p.add_argument("--bankroll", type=float, default=1000)
    p.add_argument("--fresh", action="store_true")
    p.add_argument("--live", action="store_true")
    p.add_argument("--scan-only", action="store_true", help="Scan and display, don't trade")
    p.add_argument("--no-supp", action="store_true", help="Disable supplementary")
    p.add_argument("--no-dash", action="store_true", help="Disable dashboard (log only)")
    args = p.parse_args()

    global MY_BANKROLL, DRY_RUN, SUPP_ENABLED
    MY_BANKROLL = args.bankroll
    if args.live: DRY_RUN = False
    if args.no_supp: SUPP_ENABLED = False

    if args.fresh:
        for f in ["ledger.json"]:
            if os.path.exists(f):
                os.remove(f)
                log.info(f"Removed {f}")
    else:
        ledger.load()

    running = True
    def _sig(s, f):
        nonlocal running
        running = False
        log.info("Shutdown requested...")
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    log.info("=" * 60)
    log.info("PolyArb v7 — ALL-IN-ONE TRIAD ARBITRAGE BOT")
    log.info("=" * 60)
    log.info(f"  Bankroll:  ${MY_BANKROLL:,.0f}")
    log.info(f"  Mode:      {'DRY RUN' if DRY_RUN else 'LIVE'}")
    log.info(f"  Max cost:  ${TRIAD_MAX_COST}")
    log.info(f"  Supp:      {'ON' if SUPP_ENABLED else 'OFF'}")
    log.info(f"  Log:       {LOG_FILE}")
    log.info(f"  Scan-only: {args.scan_only}")

    triads: List[Triad] = []
    leftover: List[dict] = []
    last_refresh = 0
    last_scan = 0
    last_stats = 0
    last_save = 0
    last_dash = 0
    scan_idx = 0

    while running:
        now = time.time()

        # ── REFRESH MARKETS ──
        if now - last_refresh >= REFRESH_SEC:
            markets = fetch_all_markets()
            triads, leftover = build_triads(markets)
            last_refresh = now
            scan_idx = 0  # reset scan pointer

        # ── SCAN TRIADS (batch, not all at once) ──
        if now - last_scan >= SCAN_SEC and triads:
            stats.scans += 1
            # Scan a batch of triads per cycle (avoid API overload)
            batch_size = min(30, len(triads))
            batch_start = scan_idx % len(triads)
            batch = triads[batch_start:batch_start + batch_size]
            if batch_start + batch_size > len(triads):
                batch += triads[:batch_start + batch_size - len(triads)]
            scan_idx = (scan_idx + batch_size) % len(triads)

            # Prioritize: entered triads first, then by cheapest
            entered_triads = [t for t in triads if t.is_entered]
            for t in entered_triads:
                if t not in batch:
                    batch.append(t)

            for t in batch:
                if not running: break
                # Fetch 3 prices
                t.price_a, t.depth_a = fetch_book(t.tok_a)
                t.price_b, t.depth_b = fetch_book(t.tok_b)
                t.price_d, t.depth_d = fetch_book(t.tok_d)
                t.last_price_time = time.time()

                if not args.scan_only:
                    process_triad(t)

                time.sleep(0.08)

            # Count stats
            stats.triads_total = len(triads)
            stats.triads_profitable = sum(1 for t in triads if t.edge and t.edge > 0)
            stats.triads_entered = sum(1 for t in triads if t.is_entered)
            stats.triads_complete = sum(1 for t in triads if t.is_complete)

            last_scan = now

        # ── SUPPLEMENTARY (between scans) ──
        if SUPP_ENABLED and leftover and not args.scan_only:
            batch = random.sample(leftover, min(5, len(leftover)))
            for m in batch:
                if not running: break
                process_supplementary(m)
                time.sleep(0.15)

        # ── STATS LOG ──
        if now - last_stats >= STATS_SEC:
            log.info(stats.line())
            last_stats = now

        # ── SAVE LEDGER ──
        if now - last_save >= SAVE_SEC:
            ledger.save()
            last_save = now

        # ── DASHBOARD ──
        if not args.no_dash and now - last_dash >= 5:
            print_dashboard(triads)
            last_dash = now

        time.sleep(0.5)

    # ── SHUTDOWN ──
    ledger.save()
    log.info("=" * 60)
    log.info(f"SHUTDOWN | {stats.line()}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
