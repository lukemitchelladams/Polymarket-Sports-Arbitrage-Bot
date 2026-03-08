#!/usr/bin/env python3
"""
PolyArb v6 — TRIAD ARBITRAGE BOT
══════════════════════════════════
RN1's actual strategy decoded:

  For every soccer match (Team A vs Team B), buy NO on all three outcomes:
    1. "Will Team A win?" → buy NO
    2. "Will Team B win?" → buy NO  
    3. "Will Team A vs Team B end in a draw?" → buy NO

  Guaranteed payout: EXACTLY $2.00 regardless of result
  Profit = $2.00 - total_cost (when cost < $2.00)

  This is structural arbitrage, not prediction.

Also handles non-triad NO markets (O/U, spreads, esports) with RN1-calibrated
sizing and accumulation — these are supplementary edge, not the core strategy.

Usage:
  python3.11 arb_v6.py --bankroll 1000 --fresh     # fresh start
  python3.11 arb_v6.py --bankroll 1000              # resume from ledger

Architecture:
  1. Market Discovery: REST poll every 60s, find all active markets
  2. Triad Builder: group win/win/draw markets into complete triads
  3. Triad Executor: buy all three legs when total cost < $2.00
  4. Accumulator: add to existing positions on favorable price moves
  5. Supplementary: non-triad NO buys (O/U, spreads) for coverage
  6. Position Manager: track all positions, resolve at settlement
"""

import os
import sys
import json
import time
import math
import re
import signal
import logging
import threading
import hashlib
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Dict, List, Optional, Tuple
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ─────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────
# Load keys
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

# ─── STRATEGY PARAMS ────────────────────────────────────────────────
MY_BANKROLL       = float(os.getenv("BANKROLL", "1000"))
DRY_RUN           = os.getenv("DRY_RUN", "true").lower() in ("true", "1", "yes")
MAX_DEPLOY_PCT    = float(os.getenv("MAX_DEPLOY", "0.90"))          # 90% max deployed
MAX_PER_EVENT_PCT = float(os.getenv("MAX_EVENT_PCT", "0.15"))       # 15% max per event (match)
MAX_POSITION_PER_MARKET = float(os.getenv("MAX_POS", "100"))        # $100 max per single market

# ─── TRIAD PARAMS ───────────────────────────────────────────────────
TRIAD_MIN_EDGE    = float(os.getenv("TRIAD_EDGE", "-0.04"))         # buy if edge > -4¢ (near-profitable)
TRIAD_TARGET_EDGE = float(os.getenv("TRIAD_TARGET", "0.00"))        # ideal: cost < $2.00
TRIAD_MAX_COST    = float(os.getenv("TRIAD_MAX_COST", "2.04"))      # absolute max: $2.04 total cost
TRIAD_INITIAL_SIZE = float(os.getenv("TRIAD_SIZE", "0"))            # 0 = auto (use RN1 curve)
TRIAD_MAX_PER_MATCH = float(os.getenv("TRIAD_MAX_MATCH", "0"))      # 0 = use MAX_PER_EVENT_PCT
TRIAD_ACCUMULATE_INTERVAL = float(os.getenv("TRIAD_ACCUM", "60"))   # seconds between re-buys

# ─── SUPPLEMENTARY (non-triad) PARAMS ────────────────────────────
SUPP_ENABLED      = os.getenv("SUPP_ENABLED", "true").lower() in ("true", "1")
SUPP_COOLDOWN     = float(os.getenv("SUPP_CD", "60"))               # 60s per market
SUPP_MAX_PCT      = float(os.getenv("SUPP_MAX_PCT", "0.30"))        # max 30% of bankroll in supplementary

# ─── RATE LIMITING ──────────────────────────────────────────────────
MAX_FILLS_PER_MIN = int(os.getenv("MAX_FILLS_MIN", "30"))
MAX_NEW_POS_PER_HR = int(os.getenv("MAX_NEW_HR", "60"))

# ─── TIMING ─────────────────────────────────────────────────────────
MARKET_REFRESH_SEC = int(os.getenv("REFRESH_SEC", "60"))            # refresh market list
TRIAD_SCAN_SEC     = int(os.getenv("SCAN_SEC", "15"))               # scan triads for price updates
STATS_LOG_SEC      = int(os.getenv("STATS_SEC", "30"))              # log stats

# ─── LOGGING ────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join("logs", f"arb_v6_{_ts}.log")

log = logging.getLogger("v6")
log.setLevel(logging.DEBUG)
fh = logging.FileHandler(LOG_FILE)
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(fh)
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
sh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(sh)

# ─────────────────────────────────────────────────────────────────────
# RATE LIMITER
# ─────────────────────────────────────────────────────────────────────
_fill_times: List[float] = []
_new_pos_times: List[float] = []

def rate_ok(is_new=False) -> bool:
    now = time.time()
    # Prune old entries
    while _fill_times and now - _fill_times[0] > 60:
        _fill_times.pop(0)
    while _new_pos_times and now - _new_pos_times[0] > 3600:
        _new_pos_times.pop(0)
    
    if len(_fill_times) >= MAX_FILLS_PER_MIN:
        return False
    if is_new and len(_new_pos_times) >= MAX_NEW_POS_PER_HR:
        return False
    return True

# ─────────────────────────────────────────────────────────────────────
# API HELPERS
# ─────────────────────────────────────────────────────────────────────
_req_count = 0

def api_get(url, timeout=10, retries=2):
    global _req_count
    for attempt in range(retries + 1):
        try:
            _req_count += 1
            req = Request(url, headers={"User-Agent": "PolyArb/6.0", "Accept": "application/json"})
            resp = urlopen(req, timeout=timeout)
            return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            if e.code == 429:
                time.sleep(2 ** attempt * 2)
                continue
            if e.code >= 500:
                time.sleep(1)
                continue
            return None
        except (URLError, TimeoutError, OSError):
            time.sleep(0.5)
            continue
    return None


def fetch_book(token_id):
    """Fetch order book, return (best_ask, ask_depth_usd)."""
    url = f"{CLOB}/book?token_id={token_id}"
    book = api_get(url, timeout=8)
    if not book:
        return None, 0
    
    asks = book.get("asks", [])
    if not asks:
        return None, 0
    
    best_ask = None
    total_depth = 0
    for a in asks:
        try:
            p = float(a.get("price", 0))
            s = float(a.get("size", 0))
            if p > 0 and s > 0:
                if best_ask is None or p < best_ask:
                    best_ask = p
                total_depth += s * p
        except:
            continue
    
    return best_ask, round(total_depth, 2)


# ─────────────────────────────────────────────────────────────────────
# RN1 SIZING CURVE
# ─────────────────────────────────────────────────────────────────────
def rn1_size(price, bankroll=None):
    """RN1-calibrated size based on NO price.
    At $1K bankroll: median ~$8-9, max ~$20 for new entries.
    """
    br = bankroll or MY_BANKROLL
    scale = br / 853  # his startup bankroll
    
    # His price-size curve (from deep analysis)
    if price < 0.10:
        base = 3.0
    elif price < 0.20:
        base = 5.0
    elif price < 0.30:
        base = 7.0
    elif price < 0.50:
        base = 9.0
    elif price < 0.70:
        base = 11.0
    elif price < 0.80:
        base = 13.0
    elif price < 0.90:
        base = 15.0
    else:
        base = 18.0
    
    sz = base * scale
    # Cap at 2% bankroll for initial entry
    sz = min(sz, br * 0.02)
    return round(max(sz, 1.0), 2)


# ─────────────────────────────────────────────────────────────────────
# TRIAD BUILDER
# ─────────────────────────────────────────────────────────────────────
def normalize_team(name):
    t = name.strip().lower()
    for suffix in [' fc', ' sc', ' cf', ' afc', ' sfc', ' bk']:
        if t.endswith(suffix):
            t = t[:-len(suffix)].strip()
    t = re.sub(r'\s*\(.*?\)\s*', '', t)
    t = re.sub(r'\s+(sk|fk|bk|if)$', '', t)
    return t.strip()


def extract_match_info(title):
    """Returns (type, team_a, team_b) or None.
    Types: 'win', 'draw', 'ou', 'spread', 'match', 'other'
    """
    t = title.strip()
    
    # "Will TEAM win on DATE?" or "Will TEAM win?"
    m = re.match(r'Will (.+?) win(?:\s+on\s+\d{4}-\d{2}-\d{2})?\s*\??$', t, re.I)
    if m:
        return ("win", m.group(1).strip(), None)
    
    # "Will TEAM A vs. TEAM B end in a draw?"
    m = re.match(r'Will (.+?)\s+vs\.?\s+(.+?)\s+end in a draw\s*\??$', t, re.I)
    if m:
        return ("draw", m.group(1).strip(), m.group(2).strip())
    
    # O/U markets
    if re.search(r'o/u\s+\d', t, re.I):
        m = re.match(r'(.+?)\s+vs\.?\s+(.+?):\s*o/u', t, re.I)
        if m:
            return ("ou", m.group(1).strip(), m.group(2).strip())
        return ("ou", t, None)
    
    # Spread markets
    if 'spread' in t.lower():
        m = re.match(r'(.+?)\s+vs\.?\s+(.+?):\s*spread', t, re.I)
        if m:
            return ("spread", m.group(1).strip(), m.group(2).strip())
        return ("spread", t, None)
    
    # Generic "TEAM A vs. TEAM B"
    m = re.match(r'^(.+?)\s+vs\.?\s+(.+?)$', t, re.I)
    if m:
        return ("match", m.group(1).strip(), m.group(2).strip())
    
    return None


class Triad:
    """A complete triad: Team A win-NO + Team B win-NO + Draw-NO."""
    
    def __init__(self, team_a, team_b, win_a_market, win_b_market, draw_market):
        self.team_a = team_a
        self.team_b = team_b
        self.win_a = win_a_market   # dict with id, tokens, title, etc.
        self.win_b = win_b_market
        self.draw = draw_market
        
        # Live prices (updated by scanner)
        self.price_a = None
        self.price_b = None
        self.price_draw = None
        self.depth_a = 0
        self.depth_b = 0
        self.depth_draw = 0
        
        # Execution state
        self.legs_filled = {"a": 0, "b": 0, "draw": 0}  # USD deployed per leg
        self.legs_shares = {"a": 0, "b": 0, "draw": 0}   # shares per leg
        self.last_fill_time = 0
        self.fill_count = 0
        
        # Unique key for this match
        na = normalize_team(team_a)
        nb = normalize_team(team_b)
        self.match_key = f"{min(na,nb)}_vs_{max(na,nb)}"
    
    @property
    def total_cost_per_set(self):
        """Cost of one complete set (1 share of each NO)."""
        if self.price_a and self.price_b and self.price_draw:
            return self.price_a + self.price_b + self.price_draw
        return None
    
    @property
    def edge(self):
        """Edge = $2.00 - total_cost. Positive = profit."""
        cost = self.total_cost_per_set
        return 2.00 - cost if cost else None
    
    @property
    def edge_pct(self):
        cost = self.total_cost_per_set
        if cost and cost > 0:
            return (self.edge / cost) * 100
        return None
    
    @property
    def total_deployed(self):
        return self.legs_filled["a"] + self.legs_filled["b"] + self.legs_filled["draw"]
    
    @property
    def is_complete(self):
        """True if all three legs have been bought."""
        return all(v > 0 for v in self.legs_filled.values())
    
    @property
    def min_depth(self):
        return min(self.depth_a, self.depth_b, self.depth_draw)
    
    def needs_leg(self, leg):
        """True if this leg needs buying/accumulating."""
        return self.legs_filled.get(leg, 0) < MAX_POSITION_PER_MARKET
    
    def summary(self):
        cost = self.total_cost_per_set
        edge = self.edge
        return (f"{self.team_a} vs {self.team_b}: "
                f"A@{self.price_a or 0:.3f} + B@{self.price_b or 0:.3f} + D@{self.price_draw or 0:.3f} = "
                f"${cost:.4f}" if cost else f"{self.team_a} vs {self.team_b}: prices pending")


def build_triads(markets) -> List[Triad]:
    """Group markets into complete triads."""
    win_by_team = defaultdict(list)  # normalized_team → [market, ...]
    draw_markets = []
    
    for m in markets:
        info = extract_match_info(m.get("title", ""))
        if not info:
            continue
        
        mtype = info[0]
        if mtype == "win":
            team = info[1]
            norm = normalize_team(team)
            m["_team"] = team
            m["_norm"] = norm
            win_by_team[norm].append(m)
        elif mtype == "draw":
            m["_team_a"] = info[1]
            m["_team_b"] = info[2]
            m["_norm_a"] = normalize_team(info[1])
            m["_norm_b"] = normalize_team(info[2])
            draw_markets.append(m)
    
    triads = []
    used_draws = set()
    
    for draw_m in draw_markets:
        draw_id = draw_m.get("id", "")
        if draw_id in used_draws:
            continue
        
        na = draw_m["_norm_a"]
        nb = draw_m["_norm_b"]
        
        # Find best win market for each team
        wins_a = win_by_team.get(na, [])
        wins_b = win_by_team.get(nb, [])
        
        # Fuzzy fallback
        if not wins_a:
            for k, v in win_by_team.items():
                if na in k or k in na:
                    wins_a = v
                    break
        if not wins_b:
            for k, v in win_by_team.items():
                if nb in k or k in nb:
                    wins_b = v
                    break
        
        if wins_a and wins_b:
            best_a = max(wins_a, key=lambda x: x.get("volume", 0))
            best_b = max(wins_b, key=lambda x: x.get("volume", 0))
            
            # Must have NO tokens
            if best_a.get("tokens", {}).get("no") and \
               best_b.get("tokens", {}).get("no") and \
               draw_m.get("tokens", {}).get("no"):
                
                triad = Triad(
                    draw_m["_team_a"], draw_m["_team_b"],
                    best_a, best_b, draw_m
                )
                triads.append(triad)
                used_draws.add(draw_id)
    
    return triads


# ─────────────────────────────────────────────────────────────────────
# LEDGER
# ─────────────────────────────────────────────────────────────────────
class Position:
    def __init__(self, market_id, title=""):
        self.market_id = market_id
        self.title = title
        self.fills = []       # [(price, size, timestamp), ...]
        self.total_cost = 0
        self.total_shares = 0
        self.avg_price = 0
        self.resolved = False
        self.resolved_payout = None
        self.created_at = time.time()
        self.triad_key = ""   # match_key if part of a triad
        self.leg_type = ""    # "a", "b", "draw", or "" for supplementary
    
    def add_fill(self, price, size):
        shares = size / price if price > 0 else 0
        self.fills.append((price, size, time.time()))
        self.total_cost += size
        self.total_shares += shares
        self.avg_price = self.total_cost / self.total_shares if self.total_shares > 0 else 0
    
    @property
    def total_deployed(self):
        return self.total_cost
    
    @property
    def expected_payout(self):
        return self.total_shares  # each share pays $1 if NO wins
    
    @property
    def expected_return_pct(self):
        if self.total_cost > 0:
            return ((self.expected_payout - self.total_cost) / self.total_cost) * 100
        return 0
    
    def to_dict(self):
        return {
            "market_id": self.market_id,
            "title": self.title,
            "fills": self.fills,
            "total_cost": self.total_cost,
            "total_shares": self.total_shares,
            "avg_price": self.avg_price,
            "resolved": self.resolved,
            "resolved_payout": self.resolved_payout,
            "created_at": self.created_at,
            "triad_key": self.triad_key,
            "leg_type": self.leg_type,
        }
    
    @classmethod
    def from_dict(cls, d):
        p = cls(d["market_id"], d.get("title", ""))
        p.fills = d.get("fills", [])
        p.total_cost = d.get("total_cost", 0)
        p.total_shares = d.get("total_shares", 0)
        p.avg_price = d.get("avg_price", 0)
        p.resolved = d.get("resolved", False)
        p.resolved_payout = d.get("resolved_payout")
        p.created_at = d.get("created_at", time.time())
        p.triad_key = d.get("triad_key", "")
        p.leg_type = d.get("leg_type", "")
        return p


class Ledger:
    def __init__(self):
        self.positions: Dict[str, Position] = {}
        self.lock = threading.Lock()
    
    def get_or_create(self, market_id, title="") -> Position:
        with self.lock:
            if market_id not in self.positions:
                self.positions[market_id] = Position(market_id, title)
            return self.positions[market_id]
    
    def record_fill(self, market_id, price, size, title="", triad_key="", leg_type=""):
        with self.lock:
            if market_id not in self.positions:
                self.positions[market_id] = Position(market_id, title)
            pos = self.positions[market_id]
            pos.add_fill(price, size)
            if triad_key:
                pos.triad_key = triad_key
            if leg_type:
                pos.leg_type = leg_type
            if title and not pos.title:
                pos.title = title
    
    def total_deployed(self):
        with self.lock:
            return sum(p.total_cost for p in self.positions.values() if not p.resolved)
    
    def active_count(self):
        with self.lock:
            return sum(1 for p in self.positions.values() if not p.resolved and p.total_cost > 0)
    
    def event_exposure(self, triad_key):
        with self.lock:
            return sum(p.total_cost for p in self.positions.values()
                      if not p.resolved and p.triad_key == triad_key)
    
    def triad_positions(self, triad_key):
        """Get all positions for a given triad."""
        with self.lock:
            return {p.leg_type: p for p in self.positions.values()
                   if p.triad_key == triad_key and not p.resolved}
    
    def supplementary_exposure(self):
        with self.lock:
            return sum(p.total_cost for p in self.positions.values()
                      if not p.resolved and not p.triad_key)
    
    def save(self, path="ledger.json"):
        with self.lock:
            data = {mid: pos.to_dict() for mid, pos in self.positions.items()}
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
    
    def load(self, path="ledger.json"):
        if not os.path.exists(path):
            return
        with open(path) as f:
            data = json.load(f)
        with self.lock:
            for mid, d in data.items():
                self.positions[mid] = Position.from_dict(d)
        log.info(f"Loaded {len(self.positions)} positions from {path}")


ledger = Ledger()


# ─────────────────────────────────────────────────────────────────────
# STATS
# ─────────────────────────────────────────────────────────────────────
class Stats:
    def __init__(self):
        self.start = time.time()
        self.triads_found = 0
        self.triads_profitable = 0
        self.triad_fills = 0
        self.supp_fills = 0
        self.accumulations = 0
        self.rej_rate = 0
        self.rej_deploy = 0
        self.rej_event = 0
        self.rej_edge = 0
        self.scans = 0
    
    def summary(self):
        m = (time.time() - self.start) / 60
        total_fills = self.triad_fills + self.supp_fills + self.accumulations
        return (f"Runtime:{m:.1f}m | Triads:{self.triads_found} Prof:{self.triads_profitable} | "
                f"Fills: T:{self.triad_fills} S:{self.supp_fills} A:{self.accumulations} = {total_fills} | "
                f"Rej[rate:{self.rej_rate} dep:{self.rej_deploy} evt:{self.rej_event} edge:{self.rej_edge}] | "
                f"${ledger.total_deployed():.2f}")

stats = Stats()


# ─────────────────────────────────────────────────────────────────────
# ORDER EXECUTION
# ─────────────────────────────────────────────────────────────────────
_clob_client = None

def get_clob():
    global _clob_client
    if _clob_client:
        return _clob_client
    if not PRIVATE_KEY or PRIVATE_KEY == "YOUR_PRIVATE_KEY_HERE":
        raise RuntimeError("Set PK in keys.env")
    from py_clob_client.client import ClobClient
    _clob_client = ClobClient(CLOB, key=PRIVATE_KEY, chain_id=137,
                               signature_type=2, funder=os.getenv("FUNDER"))
    _clob_client.set_api_creds(_clob_client.create_or_derive_api_creds())
    return _clob_client


def place_order(token_id, price, size, label="", use_fak=True):
    """Place a buy order. Returns order_id or None."""
    if DRY_RUN:
        log.info(f"  DRY BUY: {label} | NO@{price:.4f} ${size:.2f}")
        return "dry_" + str(int(time.time()*1000))
    
    try:
        from py_clob_client.order import OrderArgs
        from py_clob_client.constants import BUY
        client = get_clob()
        
        order_args = OrderArgs(
            price=round(price, 2),
            size=round(size / price, 1),  # shares
            side=BUY,
            token_id=token_id,
        )
        
        if use_fak:
            signed = client.create_order(order_args)
            signed["orderType"] = "FOK"
            resp = client.post_order(signed)
        else:
            signed = client.create_order(order_args)
            resp = client.post_order(signed)
        
        if resp and resp.get("orderID"):
            log.info(f"  LIVE FILL: {label} | NO@{price:.4f} ${size:.2f} | OID:{resp['orderID'][:12]}")
            return resp["orderID"]
        else:
            log.warning(f"  ORDER FAILED: {resp}")
            return None
    except Exception as e:
        log.error(f"  ORDER ERROR: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────
# TRIAD EXECUTOR
# ─────────────────────────────────────────────────────────────────────
def execute_triad_leg(triad: Triad, leg: str, size: float = 0):
    """Buy one leg of a triad.
    leg: 'a' (team A win-NO), 'b' (team B win-NO), 'draw' (draw-NO)
    """
    if leg == "a":
        market = triad.win_a
        price = triad.price_a
        token = market.get("tokens", {}).get("no")
        title = market.get("title", "")
    elif leg == "b":
        market = triad.win_b
        price = triad.price_b
        token = market.get("tokens", {}).get("no")
        title = market.get("title", "")
    elif leg == "draw":
        market = triad.draw
        price = triad.price_draw
        token = market.get("tokens", {}).get("no")
        title = market.get("title", "")
    else:
        return False
    
    if not price or not token or price <= 0:
        return False
    
    market_id = market.get("id", "")
    
    # Check rate limit
    is_new = market_id not in ledger.positions or ledger.positions[market_id].total_cost < 1.0
    if not rate_ok(is_new):
        stats.rej_rate += 1
        return False
    
    # Check deployment cap
    max_deploy = MY_BANKROLL * MAX_DEPLOY_PCT
    current = ledger.total_deployed()
    if current >= max_deploy:
        stats.rej_deploy += 1
        return False
    
    # Check per-event cap
    max_event = MY_BANKROLL * MAX_PER_EVENT_PCT
    if TRIAD_MAX_PER_MATCH > 0:
        max_event = TRIAD_MAX_PER_MATCH
    event_deployed = ledger.event_exposure(triad.match_key)
    if event_deployed >= max_event:
        stats.rej_event += 1
        return False
    
    # Size: auto or manual
    if size <= 0:
        size = rn1_size(price)
    
    # Trim to caps
    size = min(size, max_deploy - current)
    size = min(size, max_event - event_deployed)
    size = min(size, MAX_POSITION_PER_MARKET)
    
    # Check per-market cap
    pos = ledger.positions.get(market_id)
    if pos and pos.total_cost + size > MAX_POSITION_PER_MARKET:
        size = MAX_POSITION_PER_MARKET - pos.total_cost
    
    if size < 1.0:
        return False
    
    # Execute
    label = f"TRIAD-{leg.upper()}|{triad.team_a} v {triad.team_b}|{title[:30]}"
    oid = place_order(token, price, size, label)
    
    if oid:
        ledger.record_fill(market_id, price, size, title,
                          triad_key=triad.match_key, leg_type=leg)
        triad.legs_filled[leg] += size
        triad.legs_shares[leg] += size / price
        triad.fill_count += 1
        triad.last_fill_time = time.time()
        _fill_times.append(time.time())
        if is_new:
            _new_pos_times.append(time.time())
        
        if is_new:
            stats.triad_fills += 1
        else:
            stats.accumulations += 1
        
        return True
    
    return False


def process_triad(triad: Triad):
    """Evaluate and execute a triad opportunity."""
    cost = triad.total_cost_per_set
    edge = triad.edge
    
    if cost is None or edge is None:
        return
    
    # Is this triad worth buying?
    if cost > TRIAD_MAX_COST:
        stats.rej_edge += 1
        return
    
    now = time.time()
    
    # Get existing positions for this triad
    existing = ledger.triad_positions(triad.match_key)
    
    # Determine which legs need filling
    for leg in ["a", "b", "draw"]:
        leg_pos = existing.get(leg)
        leg_deployed = leg_pos.total_cost if leg_pos else 0
        
        # Skip if at per-market cap
        if leg_deployed >= MAX_POSITION_PER_MARKET:
            continue
        
        # For new entries: always buy all three legs together
        if leg_deployed < 1.0:
            # New leg — buy it
            execute_triad_leg(triad, leg)
        else:
            # Existing leg — accumulate on favorable conditions
            if now - triad.last_fill_time < TRIAD_ACCUMULATE_INTERVAL:
                continue  # cooldown
            
            # Get current price for this leg
            if leg == "a":
                current_price = triad.price_a
            elif leg == "b":
                current_price = triad.price_b
            else:
                current_price = triad.price_draw
            
            if not current_price or not leg_pos:
                continue
            
            # Accumulate if: price hasn't crashed (we're not losing badly)
            price_ratio = current_price / leg_pos.avg_price if leg_pos.avg_price > 0 else 1.0
            if price_ratio < 0.50:
                continue  # NO price crashed, stop adding
            
            # Only accumulate if triad is still near-profitable
            if cost <= TRIAD_MAX_COST:
                execute_triad_leg(triad, leg)


# ─────────────────────────────────────────────────────────────────────
# SUPPLEMENTARY NO SIGNALS (O/U, Spreads, Esports)
# ─────────────────────────────────────────────────────────────────────
_last_supp: Dict[str, float] = defaultdict(float)

def process_supplementary(market):
    """Buy NO on non-triad markets (O/U, spreads, esports, etc.)."""
    if not SUPP_ENABLED:
        return
    
    market_id = market.get("id", "")
    title = market.get("title", "")
    no_token = market.get("tokens", {}).get("no")
    if not no_token:
        return
    
    # Cooldown
    now = time.time()
    if now - _last_supp.get(market_id, 0) < SUPP_COOLDOWN:
        return
    
    # Check supplementary exposure cap
    supp_cap = MY_BANKROLL * SUPP_MAX_PCT
    if ledger.supplementary_exposure() >= supp_cap:
        return
    
    # Check deployment cap
    max_deploy = MY_BANKROLL * MAX_DEPLOY_PCT
    if ledger.total_deployed() >= max_deploy:
        return
    
    # Skip if already in this market
    pos = ledger.positions.get(market_id)
    if pos and pos.total_cost > 0:
        # Accumulation for existing supplementary positions
        if pos.total_cost >= MAX_POSITION_PER_MARKET:
            return
        if now - pos.fills[-1][2] < SUPP_COOLDOWN:
            return
    
    # Fetch current NO price
    price, depth = fetch_book(no_token)
    if not price or price <= 0.05 or price >= 0.98:
        return  # too cheap or too expensive
    
    # Size
    size = rn1_size(price)
    size = min(size, supp_cap - ledger.supplementary_exposure())
    size = min(size, max_deploy - ledger.total_deployed())
    
    if size < 1.0:
        return
    
    # Rate check
    is_new = pos is None or pos.total_cost < 1.0
    if not rate_ok(is_new):
        stats.rej_rate += 1
        return
    
    label = f"SUPP|{title[:50]}"
    oid = place_order(no_token, price, size, label)
    
    if oid:
        ledger.record_fill(market_id, price, size, title)
        _fill_times.append(time.time())
        if is_new:
            _new_pos_times.append(time.time())
        stats.supp_fills += 1
        _last_supp[market_id] = now


# ─────────────────────────────────────────────────────────────────────
# MARKET DISCOVERY
# ─────────────────────────────────────────────────────────────────────
def fetch_markets(max_markets=2000):
    """Fetch all active markets from Gamma API."""
    log.info("Fetching active markets...")
    all_markets = []
    offset = 0
    limit = 100
    
    while len(all_markets) < max_markets:
        url = f"{GAMMA}/markets?limit={limit}&offset={offset}&active=true&closed=false"
        data = api_get(url, timeout=15)
        if not data or not isinstance(data, list):
            break
        
        for m in data:
            if not m.get("active") or m.get("closed"):
                continue
            
            clob_ids = m.get("clobTokenIds", [])
            if isinstance(clob_ids, str):
                try: clob_ids = json.loads(clob_ids)
                except: clob_ids = []
            
            tokens = {}
            if len(clob_ids) >= 2:
                tokens["yes"] = str(clob_ids[0])
                tokens["no"]  = str(clob_ids[1])
            
            if not tokens.get("no"):
                continue
            
            market = {
                "id": m.get("id", ""),
                "condition_id": m.get("conditionId", ""),
                "title": m.get("question", m.get("title", "")),
                "category": m.get("category", ""),
                "end_date": m.get("endDate", ""),
                "volume": float(m.get("volume", 0) or 0),
                "liquidity": float(m.get("liquidity", 0) or 0),
                "tokens": tokens,
            }
            all_markets.append(market)
        
        if len(data) < limit:
            break
        offset += limit
        time.sleep(0.2)
    
    log.info(f"Fetched {len(all_markets)} active markets with NO tokens")
    return all_markets


# ─────────────────────────────────────────────────────────────────────
# MAIN LOOP
# ─────────────────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="PolyArb v6 — Triad Arbitrage")
    parser.add_argument("--bankroll", type=float, default=1000)
    parser.add_argument("--fresh", action="store_true", help="Delete ledger and start fresh")
    parser.add_argument("--dry-run", action="store_true", default=True)
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    
    global MY_BANKROLL, DRY_RUN
    MY_BANKROLL = args.bankroll
    if args.live:
        DRY_RUN = False
    
    if args.fresh:
        for f in ["ledger.json"]:
            if os.path.exists(f):
                os.remove(f)
                log.info(f"Removed {f}")
    else:
        ledger.load()
    
    # Graceful shutdown
    running = True
    def signal_handler(sig, frame):
        nonlocal running
        log.info("\nShutting down...")
        running = False
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("=" * 70)
    print("PolyArb v6 — TRIAD ARBITRAGE BOT")
    print("=" * 70)
    print(f"  Strategy: Team A NO + Team B NO + Draw NO = $2 guaranteed")
    print(f"  Bankroll:  ${MY_BANKROLL:,.0f}")
    print(f"  Mode:      {'DRY RUN' if DRY_RUN else 'LIVE'}")
    print(f"  Max deploy: {MAX_DEPLOY_PCT*100:.0f}%")
    print(f"  Triad edge: >{TRIAD_MIN_EDGE:+.2f} (target: {TRIAD_TARGET_EDGE:+.2f})")
    print(f"  Max cost:   ${TRIAD_MAX_COST:.2f}")
    print(f"  Supp NO:    {'ON' if SUPP_ENABLED else 'OFF'} (max {SUPP_MAX_PCT*100:.0f}%)")
    print(f"  Log:        {LOG_FILE}")
    print("=" * 70)
    
    # State
    markets = []
    triads: List[Triad] = []
    supp_markets = []
    last_refresh = 0
    last_scan = 0
    last_stats = 0
    cycle = 0
    
    while running:
        now = time.time()
        cycle += 1
        
        # ── REFRESH MARKETS ──
        if now - last_refresh >= MARKET_REFRESH_SEC:
            markets = fetch_markets()
            
            # Build triads
            triads = build_triads(markets)
            stats.triads_found = len(triads)
            
            # Identify supplementary markets (non-triad)
            triad_market_ids = set()
            for t in triads:
                triad_market_ids.add(t.win_a.get("id", ""))
                triad_market_ids.add(t.win_b.get("id", ""))
                triad_market_ids.add(t.draw.get("id", ""))
            
            supp_markets = [m for m in markets if m.get("id") not in triad_market_ids]
            
            log.info(f"DISCOVERY: {len(triads)} triads, {len(supp_markets)} supplementary markets")
            last_refresh = now
        
        # ── SCAN TRIADS ──
        if now - last_scan >= TRIAD_SCAN_SEC and triads:
            stats.scans += 1
            profitable_count = 0
            
            for triad in triads:
                if not running:
                    break
                
                # Fetch prices for all three legs
                triad.price_a, triad.depth_a = fetch_book(
                    triad.win_a.get("tokens", {}).get("no", ""))
                triad.price_b, triad.depth_b = fetch_book(
                    triad.win_b.get("tokens", {}).get("no", ""))
                triad.price_draw, triad.depth_draw = fetch_book(
                    triad.draw.get("tokens", {}).get("no", ""))
                
                cost = triad.total_cost_per_set
                edge = triad.edge
                
                if cost and edge is not None:
                    if edge > 0:
                        profitable_count += 1
                    
                    # Log interesting triads
                    if edge > TRIAD_MIN_EDGE:
                        log.info(f"TRIAD: {triad.team_a} v {triad.team_b} | "
                                f"A@{triad.price_a:.3f} B@{triad.price_b:.3f} D@{triad.price_draw:.3f} | "
                                f"Cost:${cost:.4f} Edge:{edge:+.4f} ({triad.edge_pct:+.1f}%)")
                    
                    # Execute if within parameters
                    process_triad(triad)
                
                time.sleep(0.15)  # rate limit between triad price fetches
            
            stats.triads_profitable = profitable_count
            last_scan = now
        
        # ── SUPPLEMENTARY ──
        if SUPP_ENABLED and supp_markets and now - last_scan < TRIAD_SCAN_SEC:
            # Process a batch of supplementary markets between triad scans
            import random
            batch = random.sample(supp_markets, min(10, len(supp_markets)))
            for m in batch:
                if not running:
                    break
                process_supplementary(m)
                time.sleep(0.2)
        
        # ── STATS ──
        if now - last_stats >= STATS_LOG_SEC:
            # Count triad completion
            complete_triads = 0
            partial_triads = 0
            for t in triads:
                tp = ledger.triad_positions(t.match_key)
                if len(tp) == 3:
                    complete_triads += 1
                elif len(tp) > 0:
                    partial_triads += 1
            
            log.info(f"STATS: {stats.summary()} | "
                     f"Active:{ledger.active_count()} | "
                     f"Triads[complete:{complete_triads} partial:{partial_triads}] | "
                     f"Supp:${ledger.supplementary_exposure():.0f} | "
                     f"BRL:${MY_BANKROLL:,.0f}")
            
            ledger.save()
            last_stats = now
        
        # ── SLEEP ──
        time.sleep(1)
    
    # Shutdown
    ledger.save()
    
    print(f"\n{'='*70}")
    print("SHUTDOWN COMPLETE")
    print(f"{'='*70}")
    print(f"  Runtime:  {(time.time()-stats.start)/60:.1f} minutes")
    print(f"  Triads:   {stats.triads_found} found, {stats.triads_profitable} profitable")
    print(f"  Fills:    T:{stats.triad_fills} S:{stats.supp_fills} A:{stats.accumulations}")
    print(f"  Deployed: ${ledger.total_deployed():.2f}")
    print(f"  Active:   {ledger.active_count()} positions")
    print(f"  Log:      {LOG_FILE}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
