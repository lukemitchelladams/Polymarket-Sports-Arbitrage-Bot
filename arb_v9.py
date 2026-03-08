#!/usr/bin/env python3
"""
PolyArb v9 — PURE ARB SPORTS TRADING ENGINE
═══════════════════════════════════════════════════════════════
Based on RN1's actual strategy ($5.2M lifetime):
  - Buys BOTH YES and NO when combined < $0.99 → guaranteed profit
  - Eats through entire order book on profitable arbs (50-100+ trades/market)
  - Keeps rescanning and building arb positions every cycle
  - If no arb exists, does NOTHING — no speculative one-sided bets

ARCHITECTURE:
  ARB ENGINE (runs every 50ms = 20x/second):
    Phase 1: BUILD existing arbs — re-buy more matched pairs
    Phase 2: SCAN all markets — find new arbs, enter both sides
    
  CROSS-MARKET SCANNER (runs every cycle):
    Triads: Win A + Win B + Draw (soccer) < $1.97 = guaranteed $2 payout
    Duos: Win A + Win B (basketball) < $0.97 = guaranteed $1 payout

  NO SPECULATIVE BETS. NO ONE-SIDED POSITIONS. ARBS ONLY.
  If no arb exists → bot sits idle (correct behavior).
  Every action verified through ROI gate before execution.

Usage:
  python3.11 arb_v9.py --bankroll 1000 --fresh                # dry run
  python3.11 arb_v9.py --bankroll 1000 --fresh --live          # real money
  python3.11 arb_v9.py --bankroll 1000 --fresh --scan          # scan only
"""

import os, sys, json, time, re, signal, logging, math, hashlib
from datetime import datetime, timezone
from collections import defaultdict, deque
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════
KEYS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "keys.env")
if os.path.exists(KEYS_FILE):
    with open(KEYS_FILE) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

PRIVATE_KEY = os.getenv("PK", "")
PROXY_WALLET = os.getenv("PROXY_WALLET", "")
CLOB        = "https://clob.polymarket.com"
GAMMA       = "https://gamma-api.polymarket.com"

# ─── Bankroll ───
BANKROLL        = float(os.getenv("BANKROLL", "1000"))
DRY_RUN         = True
MAX_DEPLOY_PCT  = 0.92       # max 92% deployed at once
MAX_PER_EVENT   = 0.08       # max 8% bankroll per event/match group
MAX_PER_MARKET  = 0.05       # max 5% bankroll per individual market

# ─── ARB-SPECIFIC LIMITS (higher — guaranteed profit!) ───
ARB_MAX_PER_MARKET = 0.10    # 10% bankroll per arb market
ARB_MAX_PER_EVENT  = 0.25    # 25% bankroll per arb event
ARB_HAMMER_PCT     = 0.10    # hammer up to 10% of bankroll per cycle

# ─── Market Filtering: STRICT SPORTS ONLY ───
# RN1 ONLY trades sports event markets that resolve within ~24h.
MAX_DAYS_OUT = 7  # skip markets resolving more than 7 days from now

# ─── Arb Lock (Pillar 5: LOCK GUARANTEED PROFIT) ───
# BINARY ARB: same market, NO_avg + YES_ask < threshold → lock it
ARB_LOCK_THRESHOLD = 0.99    # combined cost must be under this to lock (1% min profit)
ARB_LOCK_MIN_SPREAD = 0.005  # minimum 0.5% guaranteed profit to bother

# TRIAD ARB: 3 related markets (Win A NO + Win B NO + Draw NO)
# ANY outcome → exactly 2 of 3 NOs pay $1 each → guaranteed $2 payout
# If combined cost of all 3 NOs < $2.00, guaranteed profit
TRIAD_LOCK_THRESHOLD = 1.97  # 3 NOs must cost less than this (= 1.5% min profit)

# DUO ARB: 2 related markets (Win A NO + Win B NO) — no draw
# ANY outcome → exactly 1 NO pays $1 → guaranteed $1 payout
# Same as binary arb but across markets
DUO_LOCK_THRESHOLD = 0.97    # 2 NOs must cost less than this

# ─── Rate Limits ───
MAX_ORDERS_MIN  = 60
MAX_NEW_MKTS_HR = 150

# ═══════════════════════════════════════════════════════════════
# MARKET CLASSIFICATION — STRICT SPORTS GATE
# ═══════════════════════════════════════════════════════════════

def is_sports_market(title, end_date_str=""):
    """
    Soft sports filter. The REAL filter is endDate (within 48h) + volume.
    Sports markets naturally close within hours and have high volume.
    This just rejects obvious non-sports that slip through.
    
    ACCEPTS: anything with "vs", "win on DATE", "O/U", "draw", etc.
    ALSO ACCEPTS: anything that doesn't match reject keywords
    (because 48h endDate already removed most non-sports)
    
    REJECTS: crypto, politics, tech, entertainment keywords
    """
    if not title: return False
    tl = title.lower()
    
    # Hard reject: known non-sports keywords
    REJECT_KW = ['bitcoin','crypto','xrp','ethereum','solana','token','dip to',
        'all time high','acquired','federal','embassy','democrat','republican',
        'election','senate','governor','congress','president','openai','google',
        'apple','microsoft','amazon','tesla','stock','reserve','price by',
        'market cap','ceo ','ipo ','sec ','nato ','ukraine','russia',
        'china invade','israel','palestine','nuclear','gpt','chatgpt',
        'supreme court','legislation','tariff','immigration',
        'convicted','sentenced','prison','album','released before',
        'gta vi','rihanna','playboi','jesus christ','harvey weinstein',
        'return before','invades',
        'elon musk','musk post','tweets','tweet','iran','saudi arabia',
        'strike iran','ceasefire','qatar','opening weekend','scream ',
        'gold (gc)','up or down','oil (cl)','s&p 500','nasdaq','dow jones',
        'box office','movie','film ','emmy','oscar','grammy',
        'spotify','youtube','tiktok','instagram','follower','subscriber',
        'weather','temperature','earthquake','hurricane','tornado',
        'spacex','launch','mars','moon','asteroid','rocket',
        'pope','vatican','royal','king charles','queen',
        'trump','biden','harris','desantis','rfk','kennedy',
        'indictment','impeach','arrest','fbi','cia','doj',
        'fed rate','interest rate','inflation','gdp','unemployment',
        'uk strike','us x ','post 2','will .* post','china strike']
    if any(kw in tl for kw in REJECT_KW):
        return False
    
    # POSITIVE patterns — if it matches any of these, it's sports
    has_vs = bool(re.search(r'\bvs\.?\b', tl))
    has_win_date = bool(re.match(r'will .+ win on \d{4}-\d{2}-\d{2}', tl))
    has_ou = bool(re.search(r'o/u\s+\d', tl))
    has_draw = 'end in a draw' in tl
    has_btts = 'both teams to score' in tl
    has_spread = bool(re.search(r'spread\s+[+-]?\d', tl))
    has_esports = any(w in tl for w in ['counter-strike','dota','valorant','lol:',
        'league of legends','bo3','bo5','bo2','rocket league','overwatch'])
    has_game = bool(re.search(r'(game \d+ winner|map \d+ winner|set \d+ winner)', tl))
    
    if has_vs or has_win_date or has_ou or has_draw or has_btts or has_spread or has_esports or has_game:
        return True
    
    # If no positive pattern matched, reject (it's probably not sports)
    return False

def classify(title):
    """Classify a sports market into type. Only called AFTER is_sports_market passes."""
    if not title: return "other"
    tl = title.lower()
    if 'end in a draw' in tl: return "draw"
    if re.search(r'o/u\s+\d', tl): return "ou"
    if 'both teams to score' in tl: return "btts"
    if re.search(r'spread\s+[+-]?\d', tl): return "spread"
    if any(w in tl for w in ['counter-strike','dota','valorant','lol:',
        'league of legends','bo3','bo5','bo2','call of duty','rocket league',
        'rainbow six','overwatch','starcraft','mobile legends','pubg']): return "esports"
    if re.match(r'will .+ win on \d{4}-\d{2}-\d{2}', tl, re.I): return "win"
    if re.search(r'\bvs\.?\b', tl): return "match"
    if re.search(r'(game \d+ winner|map \d+ winner|set \d+ winner)', tl): return "match"
    return "other"
os.makedirs("logs", exist_ok=True)
_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = f"logs/v9_{_ts}.log"
log = logging.getLogger("v9")
log.setLevel(logging.DEBUG)
_fh = logging.FileHandler(LOG_FILE); _fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(_fh)
_sh = logging.StreamHandler(); _sh.setLevel(logging.INFO)
_sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(_sh)

# ═══════════════════════════════════════════════════════════════
# API LAYER
# ═══════════════════════════════════════════════════════════════
_sess_cache = {}

def api_get(url, timeout=15, retries=2):
    for attempt in range(retries + 1):
        try:
            req = Request(url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
            resp = urlopen(req, timeout=timeout)
            return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            if e.code == 429: time.sleep(2 ** attempt * 2); continue
            if e.code >= 500: time.sleep(1); continue
            return None
        except Exception:
            time.sleep(0.5); continue
    return None

def fetch_book(token_id):
    """Get best ask price and total depth for a token."""
    book = api_get(f"{CLOB}/book?token_id={token_id}", timeout=8)
    if not book: return None, 0, None, 0
    asks = book.get("asks", [])
    bids = book.get("bids", [])
    
    best_ask = None; ask_depth = 0
    for a in asks:
        try:
            p, s = float(a["price"]), float(a["size"])
            if p > 0 and s > 0:
                if best_ask is None or p < best_ask: best_ask = p
                ask_depth += s * p
        except: continue
    
    best_bid = None; bid_depth = 0
    for b in bids:
        try:
            p, s = float(b["price"]), float(b["size"])
            if p > 0 and s > 0:
                if best_bid is None or p > best_bid: best_bid = p
                bid_depth += s * p
        except: continue
    
    return best_ask, round(ask_depth, 2), best_bid, round(bid_depth, 2)

def batch_fetch_books(token_ids, max_workers=12):
    """Fetch order books for multiple tokens in PARALLEL using ThreadPoolExecutor.
    Returns dict: {token_id: (ask, ask_depth, bid, bid_depth)}"""
    cache = {}
    unique = list(set(t for t in token_ids if t))
    if not unique: return cache
    
    with ThreadPoolExecutor(max_workers=min(max_workers, len(unique))) as pool:
        futures = {pool.submit(fetch_book, tid): tid for tid in unique}
        for f in as_completed(futures):
            tid = futures[f]
            try:
                cache[tid] = f.result()
            except:
                cache[tid] = (None, 0, None, 0)
    return cache

# ─── WebSocket Price Stream (optional, falls back to HTTP) ───
_ws_cache = {}       # {token_id: (ask, ask_depth, bid, bid_depth, timestamp)}
_ws_subscribed = set()
_ws_thread = None
_ws_running = False
WS_STALE_SECS = 5   # consider WS data stale after 5 seconds

def ws_get(token_id):
    """Get cached WS price if fresh, else None."""
    entry = _ws_cache.get(token_id)
    if entry and (time.time() - entry[4]) < WS_STALE_SECS:
        return entry[:4]  # (ask, ask_depth, bid, bid_depth)
    return None

def ws_start(token_ids):
    """Start WebSocket price stream in background thread."""
    global _ws_thread, _ws_running
    try:
        import websockets
        import asyncio
    except ImportError:
        log.warning("⚠️ websockets not installed — using HTTP polling")
        log.warning("⚠️ Install with: pip install websockets")
        return False
    
    if _ws_running:
        # Just subscribe new tokens
        new_tokens = set(token_ids) - _ws_subscribed
        if new_tokens:
            _ws_subscribed.update(new_tokens)
        return True
    
    _ws_subscribed.update(token_ids)
    _ws_running = True
    
    async def _ws_loop():
        import websockets
        uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        while _ws_running:
            try:
                async with websockets.connect(uri, ping_interval=20) as ws:
                    # Subscribe in batches
                    for tid in list(_ws_subscribed):
                        msg = json.dumps({"type": "market", "assets_ids": [tid]})
                        await ws.send(msg)
                    
                    async for raw in ws:
                        try:
                            data = json.loads(raw)
                            if not isinstance(data, list): data = [data]
                            for evt in data:
                                aid = evt.get("asset_id", "")
                                if not aid: continue
                                bids = evt.get("bids", [])
                                asks = evt.get("asks", [])
                                
                                best_ask = None; ask_depth = 0
                                for a in asks:
                                    p, s = float(a.get("price",0)), float(a.get("size",0))
                                    if p > 0 and s > 0:
                                        if best_ask is None or p < best_ask: best_ask = p
                                        ask_depth += s * p
                                
                                best_bid = None; bid_depth = 0
                                for b in bids:
                                    p, s = float(b.get("price",0)), float(b.get("size",0))
                                    if p > 0 and s > 0:
                                        if best_bid is None or p > best_bid: best_bid = p
                                        bid_depth += s * p
                                
                                _ws_cache[aid] = (best_ask, round(ask_depth,2),
                                                  best_bid, round(bid_depth,2), time.time())
                        except: pass
            except Exception as e:
                log.debug(f"WS reconnecting: {e}")
                await asyncio.sleep(2)
    
    def _run():
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_ws_loop())
    
    import threading
    _ws_thread = threading.Thread(target=_run, daemon=True)
    _ws_thread.start()
    log.info(f"🔌 WebSocket started — subscribing {len(token_ids)} tokens")
    return True

def smart_fetch(token_id):
    """Try WS cache first, fall back to HTTP."""
    ws = ws_get(token_id)
    if ws: return ws
    return fetch_book(token_id)

def smart_batch_fetch(token_ids):
    """For each token: try WS cache, batch-HTTP the rest."""
    cache = {}
    need_http = []
    for tid in set(t for t in token_ids if t):
        ws = ws_get(tid)
        if ws:
            cache[tid] = ws
        else:
            need_http.append(tid)
    
    if need_http:
        http_cache = batch_fetch_books(need_http)
        cache.update(http_cache)
    
    return cache

# ═══════════════════════════════════════════════════════════════
# RATE LIMITER
# ═══════════════════════════════════════════════════════════════
_order_ts = deque()
_new_mkt_ts = deque()

def rate_ok(is_new=False):
    now = time.time()
    while _order_ts and now - _order_ts[0] > 60: _order_ts.popleft()
    while _new_mkt_ts and now - _new_mkt_ts[0] > 3600: _new_mkt_ts.popleft()
    if len(_order_ts) >= MAX_ORDERS_MIN: return False
    if is_new and len(_new_mkt_ts) >= MAX_NEW_MKTS_HR: return False
    return True

def record_order(is_new=False):
    _order_ts.append(time.time())
    if is_new: _new_mkt_ts.append(time.time())

# ═══════════════════════════════════════════════════════════════
# MARKET CLASSIFICATION
# ═══════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════
# POSITION TRACKER
# ═══════════════════════════════════════════════════════════════
class Position:
    """Tracks a position on one market — both NO and YES sides."""
    __slots__ = ['mid','title','typ','tok_no','tok_yes','group_key',
                 'cost','shares','n_buys','n_sells','avg_price',
                 'yes_cost','yes_shares','yes_avg_price','n_yes_buys',
                 'current_ask','current_bid','current_yes_ask','current_yes_bid',
                 'last_check','last_buy','last_sell',
                 'created','peak_price','trough_price','active',
                 'spread_at_entry',
                 'last_rebal_no_price','last_rebal_time']
    
    def __init__(self, mid, title, typ, tok_no, tok_yes="", group_key=""):
        self.mid = mid
        self.title = title
        self.typ = typ
        self.tok_no = tok_no
        self.tok_yes = tok_yes
        self.group_key = group_key or mid
        # NO side
        self.cost = 0.0
        self.shares = 0.0
        self.n_buys = 0
        self.n_sells = 0
        self.avg_price = 0.0
        # YES side
        self.yes_cost = 0.0
        self.yes_shares = 0.0
        self.yes_avg_price = 0.0
        self.n_yes_buys = 0
        # Prices
        self.current_ask = None
        self.current_bid = None
        self.current_yes_ask = None
        self.current_yes_bid = None
        self.last_check = 0.0
        self.last_buy = 0.0
        self.last_sell = 0.0
        self.created = time.time()
        self.peak_price = 0.0
        self.trough_price = 99.0
        self.active = True
        self.spread_at_entry = 0.0  # bid-ask spread % when first entered
        self.last_rebal_no_price = 0.0  # NO price at last rebalance
        self.last_rebal_time = 0.0      # when we last rebalanced
    
    def record_buy(self, price, amount, actual_shares=None):
        """Record a NO side buy. Uses actual_shares if provided (from fill), 
        otherwise estimates from amount/price."""
        self.cost += amount
        new_shares = actual_shares if actual_shares is not None else (amount / price if price > 0 else 0)
        self.shares += new_shares
        self.n_buys += 1
        self.avg_price = self.cost / self.shares if self.shares > 0 else price
        self.last_buy = time.time()
    
    def record_yes_buy(self, price, amount, actual_shares=None):
        """Record a YES side buy (for arb locking)."""
        self.yes_cost += amount
        new_shares = actual_shares if actual_shares is not None else (amount / price if price > 0 else 0)
        self.yes_shares += new_shares
        self.n_yes_buys += 1
        self.yes_avg_price = self.yes_cost / self.yes_shares if self.yes_shares > 0 else price
        self.last_buy = time.time()
    
    @property
    def total_cost(self):
        """Total invested in this market (both sides)."""
        return self.cost + self.yes_cost
    
    @property
    def is_arb_locked(self):
        """True arb: both sides held AND guaranteed profit is positive.
        If shares are unequal (manual sells), this returns False so
        the position gets managed normally (cuts/hedges)."""
        if self.shares <= 0 or self.yes_shares <= 0:
            return False
        gp = min(self.shares, self.yes_shares) - self.total_cost
        return gp > 0
    
    @property
    def is_two_sided(self):
        """Holds both YES and NO shares (may or may not be profitable)."""
        return self.shares > 0 and self.yes_shares > 0
    
    @property
    def guaranteed_profit(self):
        """
        Guaranteed profit regardless of outcome.
        If YES wins: yes_shares × $1 - total_cost
        If NO wins:  no_shares × $1 - total_cost
        Guaranteed = min of the two = min(yes_shares, no_shares) - total_cost
        Positive = locked profit. Negative = still at risk.
        """
        if not self.is_two_sided:
            return None  # one-sided, no guarantee
        return min(self.shares, self.yes_shares) - self.total_cost
    
    @property
    def guaranteed_roi(self):
        """Guaranteed ROI % for arb positions."""
        gp = self.guaranteed_profit
        if gp is None or self.total_cost <= 0: return 0
        return gp / self.total_cost
    
    def would_improve_roi(self, action, price, amount):
        """
        THE CORE ROI CHECK: would this action increase guaranteed profit?
        
        action: 'buy_no', 'buy_yes', 'buy_matched'
        price: price per share for the action
        amount: $ to spend
        
        Returns (improves: bool, new_guaranteed: float, old_guaranteed: float)
        """
        new_shares = amount / price if price > 0 else 0
        tc = self.total_cost
        
        if action == 'buy_no':
            new_no = self.shares + new_shares
            new_yes = self.yes_shares
            new_cost = tc + amount
        elif action == 'buy_yes':
            new_no = self.shares
            new_yes = self.yes_shares + new_shares
            new_cost = tc + amount
        elif action == 'buy_matched':
            # Buying equal $ of both sides
            no_shares_add = (amount/2) / (self.current_ask or price) if self.current_ask else 0
            yes_shares_add = (amount/2) / price if price > 0 else 0
            new_no = self.shares + no_shares_add
            new_yes = self.yes_shares + yes_shares_add
            new_cost = tc + amount
        else:
            return False, 0, 0
        
        old_gp = self.guaranteed_profit or 0
        new_gp = min(new_no, new_yes) - new_cost
        
        return new_gp > old_gp, new_gp, old_gp
    
    def record_sell(self, price, share_count, revenue):
        self.shares = max(0, self.shares - share_count)
        # Proportional cost reduction
        if self.shares > 0:
            frac_sold = share_count / (self.shares + share_count)
            self.cost *= (1 - frac_sold)
        else:
            self.cost = 0
        self.n_sells += 1
        self.last_sell = time.time()
    
    @property
    def pnl_pct(self):
        """Current unrealized P&L % based on bid price (what you'd actually get selling).
        Bid matches what Polymarket shows. Mid-price inflates values."""
        if self.avg_price <= 0: return 0
        if self.current_bid:
            return (self.current_bid - self.avg_price) / self.avg_price
        return 0
    
    @property
    def pnl_pct_bid(self):
        """Conservative P&L % based on bid only (what we'd actually get if selling)."""
        if not self.current_bid or self.avg_price <= 0: return 0
        return (self.current_bid - self.avg_price) / self.avg_price
    
    
    @property
    def age_hours(self):
        return (time.time() - self.created) / 3600
    
    @property
    def value(self):
        """Current estimated value (both sides) using BID price.
        Bid = what you'd actually get selling. Mid-price inflates values."""
        v = 0
        if self.shares > 0:
            if self.current_bid:
                v += self.current_bid * self.shares
            else:
                v += self.cost
        if self.yes_shares > 0:
            if self.current_yes_bid:
                v += self.current_yes_bid * self.yes_shares
            else:
                v += self.yes_cost
        return v
    
    def short(self):
        return self.title[:38] if self.title else self.mid[:12]

# ═══════════════════════════════════════════════════════════════
# PORTFOLIO
# ═══════════════════════════════════════════════════════════════
class Portfolio:
    def __init__(self):
        self.positions = {}      # mid → Position
        self.cash_deployed = 0.0
        self.total_realized = 0.0
        self.total_cost = 0.0
        self.n_entries = 0
        self.n_profit_takes = 0
        self.n_loss_cuts = 0
        self.n_dip_buys = 0
        self.n_arb_locks = 0
    
    def get(self, mid): return self.positions.get(mid)
    def has(self, mid): return mid in self.positions
    
    def add_position(self, pos):
        self.positions[pos.mid] = pos
    
    def deployed(self):
        return sum(p.total_cost for p in self.positions.values())
    
    def est_value(self):
        return sum(p.value for p in self.positions.values())
    
    def active_count(self):
        return sum(1 for p in self.positions.values() if p.shares > 0)
    
    def group_deployed(self, gk):
        return sum(p.total_cost for p in self.positions.values() if p.group_key == gk)
    
    def market_deployed(self, mid):
        p = self.positions.get(mid)
        return p.total_cost if p else 0
    
    def room_for(self, mid, group_key):
        """How much more can we deploy on this market?"""
        total_room = BANKROLL * MAX_DEPLOY_PCT - self.deployed()
        group_room = BANKROLL * MAX_PER_EVENT - self.group_deployed(group_key)
        mkt_room = BANKROLL * MAX_PER_MARKET - self.market_deployed(mid)
        return max(0, min(total_room, group_room, mkt_room))
    
    def arb_room_for(self, mid, group_key):
        """How much more can we deploy on this ARB? Much higher limits — guaranteed profit."""
        total_room = BANKROLL * MAX_DEPLOY_PCT - self.deployed()
        group_room = BANKROLL * ARB_MAX_PER_EVENT - self.group_deployed(group_key)
        mkt_room = BANKROLL * ARB_MAX_PER_MARKET - self.market_deployed(mid)
        return max(0, min(total_room, group_room, mkt_room))
    
    def sync_wallet(self):
        """
        Pillar 0: WALLET SYNC — fetch positions from data-api.polymarket.com
        Public endpoint, no auth needed. Works in dry and live mode.
        """
        log.info("🔄 Syncing wallet from Polymarket...")
        
        if not PROXY_WALLET:
            log.warning("  No PROXY_WALLET set — can't sync. Set: export PROXY_WALLET=0x...")
            return 0
        
        wallet = PROXY_WALLET.lower()
        synced = 0
        
        # data-api.polymarket.com/positions?user=WALLET — the one that works
        url = f"https://data-api.polymarket.com/positions?user={wallet}"
        data = api_get(url, timeout=20)
        
        if not data or not isinstance(data, list):
            log.warning(f"  Wallet API returned no data for {wallet[:12]}...")
            return 0
        
        log.info(f"  Found {len(data)} positions in wallet")
        
        for item in data:
            if not isinstance(item, dict): continue
            
            token_id = str(item.get("asset", ""))
            size = float(item.get("size", 0))
            if size <= 0 or not token_id: continue
            
            condition_id = str(item.get("conditionId", token_id[:16]))
            avg_price = float(item.get("avgPrice", 0))
            initial_val = float(item.get("initialValue", 0))
            current_val = float(item.get("currentValue", 0))
            cash_pnl = float(item.get("cashPnl", 0))
            
            # Skip resolved/dead positions (worth $0, already settled)
            if current_val <= 0.01 and cash_pnl <= 0:
                continue
            
            # Look up market title from Gamma API using token ID
            title = ""
            gamma_mid = ""
            is_yes_token = False  # track which side this token is
            tok_no_id = ""
            tok_yes_id = ""
            try:
                # Try lookup by CLOB token ID
                mkt_data = api_get(
                    f"{GAMMA}/markets?clob_token_ids={token_id}&limit=1", timeout=8)
                if not mkt_data or not isinstance(mkt_data, list) or len(mkt_data) == 0:
                    # Fallback: try condition_id
                    mkt_data = api_get(
                        f"{GAMMA}/markets?id={condition_id}&limit=1", timeout=8)
                
                if mkt_data and isinstance(mkt_data, list) and len(mkt_data) > 0:
                    m = mkt_data[0]
                    title = m.get("question", "") or m.get("title", "")
                    gamma_mid = m.get("id", condition_id)
                    clob_ids = m.get("clobTokenIds", [])
                    if isinstance(clob_ids, str):
                        try: clob_ids = json.loads(clob_ids)
                        except: clob_ids = []
                    if isinstance(clob_ids, list) and len(clob_ids) >= 2:
                        tok_yes_id = str(clob_ids[0])  # [0] = YES outcome
                        tok_no_id = str(clob_ids[1])   # [1] = NO outcome
                        if token_id == tok_yes_id:
                            is_yes_token = True
                        elif token_id == tok_no_id:
                            is_yes_token = False
                        else:
                            # Token doesn't match either — try outcomes
                            outcomes = m.get("outcomes", [])
                            if outcomes and len(outcomes) >= 2:
                                log.warning(f"  Token {token_id[:12]}... doesn't match clob_ids for {title[:30]}")
            except: pass
            
            if not title:
                title = f"pos_{condition_id[:12]}"
            
            # Use Gamma market ID if found, otherwise conditionId
            mid = gamma_mid if gamma_mid else condition_id
            
            # Check if we already have a position on this market
            # (could be other side of an existing position — arb!)
            if self.has(mid):
                existing = self.positions[mid]
                if is_yes_token and existing.yes_shares <= 0:
                    # We already have NO, this is the YES side → arb-locked!
                    existing.tok_yes = token_id
                    existing.yes_shares = size
                    existing.yes_cost = cost
                    existing.yes_avg_price = avg_price if avg_price > 0 else 0.5
                    existing.n_yes_buys = 1
                    self.total_cost += cost
                    synced += 1
                    log.info(f"  📥 +YES {title[:30]:<30s} {size:.0f}sh @{avg_price:.3f} "
                            f"→ ARB-LOCKED! [{typ}]")
                elif not is_yes_token and existing.shares <= 0:
                    # We already have YES, this is the NO side
                    existing.tok_no = token_id
                    existing.shares = size
                    existing.cost = cost
                    existing.avg_price = avg_price if avg_price > 0 else 0.5
                    existing.n_buys = 1
                    self.total_cost += cost
                    synced += 1
                    log.info(f"  📥 +NO  {title[:30]:<30s} {size:.0f}sh @{avg_price:.3f} "
                            f"→ ARB-LOCKED! [{typ}]")
                else:
                    log.debug(f"  Skip duplicate {title[:30]} (same side already tracked)")
                continue
            
            # Get live price (will be batch-fetched below after all positions loaded)
            ask, bid = None, None
            
            typ = classify(title)
            cost = initial_val if initial_val > 0 else (avg_price * size if avg_price > 0 else 0)
            
            # Create position on correct side
            if is_yes_token:
                pos = Position(mid, title, typ, tok_no_id, token_id, group_key=mid)
                # Put shares on YES side
                pos.yes_shares = size
                pos.yes_cost = cost
                pos.yes_avg_price = avg_price if avg_price > 0 else 0.5
                pos.n_yes_buys = 1
                side_label = "YES"
            else:
                pos = Position(mid, title, typ, token_id, tok_yes_id, group_key=mid)
                # Put shares on NO side (original behavior)
                pos.shares = size
                pos.cost = cost
                pos.avg_price = avg_price if avg_price > 0 else 0.5
                pos.n_buys = 1
                side_label = "NO"
            
            pos.active = True
            
            self.add_position(pos)
            self.total_cost += cost
            synced += 1
            
            pnl_s = f"${cash_pnl:+.2f}" if cash_pnl else ""
            log.info(f"  📥 {side_label:>3s} {title[:32]:<32s} {size:.0f}sh @{avg_price:.3f} "
                    f"val=${current_val:.2f} {pnl_s} [{typ}]")
        
        if synced > 0:
            # Batch refresh all live prices in parallel (instead of one-by-one)
            all_tokens = []
            for p in self.positions.values():
                if p.tok_no: all_tokens.append(p.tok_no)
                if p.tok_yes: all_tokens.append(p.tok_yes)
            if all_tokens:
                log.info(f"  ⚡ Batch refreshing {len(all_tokens)} token prices...")
                cache = batch_fetch_books(all_tokens)
                for p in self.positions.values():
                    if p.tok_no and p.tok_no in cache:
                        a, _, b, _ = cache[p.tok_no]
                        p.current_ask = a; p.current_bid = b
                        if b and a and a > 0:
                            p.spread_at_entry = (a - b) / a
                    if p.tok_yes and p.tok_yes in cache:
                        a, _, b, _ = cache[p.tok_yes]
                        p.current_yes_ask = a; p.current_yes_bid = b
            
            # Check for arb opportunities on synced positions
            arb_opps = 0
            for p in self.positions.values():
                if p.shares > 0 and not p.is_arb_locked and p.tok_yes:
                    yes_ask = p.current_yes_ask
                    if yes_ask and yes_ask > 0.001 and p.avg_price > 0:
                        combined = p.avg_price + yes_ask
                        if combined < ARB_LOCK_THRESHOLD:
                            spread = (1.0 - combined) * 100
                            log.info(f"  🎯 ARB OPPORTUNITY: {p.short()} "
                                    f"NO@{p.avg_price:.3f}+YES@{yes_ask:.3f}={combined:.3f} "
                                    f"({spread:.1f}% profit)")
                            arb_opps += 1
            
            log.info(f"  ✅ Synced {synced} positions (${self.deployed():,.2f} deployed)"
                     f"{f' — {arb_opps} arb opportunities!' if arb_opps else ''}")
        else:
            log.info("  No open positions with size > 0")
        
        return synced
    
    def save(self, path="portfolio_v9.json"):
        data = {}
        for mid, p in self.positions.items():
            data[mid] = {
                "title": p.title, "typ": p.typ,
                "tok_no": p.tok_no, "tok_yes": p.tok_yes, "group_key": p.group_key,
                "cost": p.cost, "shares": p.shares,
                "n_buys": p.n_buys, "n_sells": p.n_sells, "avg_price": p.avg_price,
                "yes_cost": p.yes_cost, "yes_shares": p.yes_shares,
                "yes_avg_price": p.yes_avg_price, "n_yes_buys": p.n_yes_buys,
                "created": p.created, "active": p.active,
                "spread_at_entry": p.spread_at_entry,
                "last_rebal_no_price": p.last_rebal_no_price,
            }
        with open(path, "w") as f:
            json.dump({"positions": data, "realized": self.total_realized,
                       "total_cost": self.total_cost, "entries": self.n_entries,
                       "profit_takes": self.n_profit_takes, "loss_cuts": self.n_loss_cuts,
                       "dip_buys": self.n_dip_buys, "arb_locks": self.n_arb_locks}, f, indent=2)
    
    def load(self, path="portfolio_v9.json"):
        if not os.path.exists(path): return
        with open(path) as f:
            data = json.load(f)
        for mid, d in data.get("positions", {}).items():
            p = Position(mid, d["title"], d["typ"], d["tok_no"],
                        d.get("tok_yes", ""), group_key=d.get("group_key",""))
            p.cost = d["cost"]; p.shares = d["shares"]; p.n_buys = d["n_buys"]
            p.n_sells = d.get("n_sells",0); p.avg_price = d["avg_price"]
            p.yes_cost = d.get("yes_cost", 0); p.yes_shares = d.get("yes_shares", 0)
            p.yes_avg_price = d.get("yes_avg_price", 0); p.n_yes_buys = d.get("n_yes_buys", 0)
            p.created = d.get("created", time.time()); p.active = d.get("active", True)
            p.spread_at_entry = d.get("spread_at_entry", 0.0)
            p.last_rebal_no_price = d.get("last_rebal_no_price", 0.0)
            self.positions[mid] = p
        self.total_realized = data.get("realized", 0)
        self.total_cost = data.get("total_cost", 0)
        self.n_entries = data.get("entries", 0)
        self.n_profit_takes = data.get("profit_takes", 0)
        self.n_loss_cuts = data.get("loss_cuts", 0)
        self.n_dip_buys = data.get("dip_buys", 0)
        self.n_arb_locks = data.get("arb_locks", 0)
        locked = sum(1 for p in self.positions.values() if p.is_arb_locked)
        log.info(f"Loaded portfolio: {len(self.positions)} positions "
                 f"({locked} arb-locked), ${self.deployed():.2f} deployed")

portfolio = Portfolio()

# ═══════════════════════════════════════════════════════════════
# EXECUTION ENGINE — LIMIT-ORDER-FIRST
# ═══════════════════════════════════════════════════════════════
# RN1 places limit orders at favorable prices, not market orders.
# We only get filled when someone crosses to OUR price → we enter
# already at a profitable level relative to the spread.
#
# MODES:
#   'limit'      → GTC at mid-price (default for entries, adds)
#   'aggressive'  → GTC at ask (for arb locks where speed matters)
#   'fok'        → FOK at specified price (for urgent exits)

_clob = None

def get_clob():
    global _clob
    if _clob: return _clob
    from py_clob_client import ClobClient
    # Match v4's working config: signature_type=1 with funder=PROXY_WALLET
    _clob = ClobClient(CLOB, key=PRIVATE_KEY, chain_id=137,
                        signature_type=1, funder=PROXY_WALLET)
    _clob.set_api_creds(_clob.create_or_derive_api_creds())
    log.info(f"  CLOB connected (sig_type=1, wallet:{PROXY_WALLET[:12]}...)")
    return _clob

def calc_limit_price(bid, ask, mode="limit"):
    """Calculate the price to place our order at.
    
    'limit'      → mid-price: (bid+ask)/2, rounded to 2 decimals
                   We sit in the middle of the book; filled when someone
                   crosses to us. Entry cost ≈ mid, not ask.
    'aggressive' → at the ask (take the liquidity, fill immediately)
    'bid'        → at the bid (for selling — sit on the bid)
    
    Returns price rounded to 0.01 (Polymarket tick size).
    """
    if mode == "aggressive":
        return round(ask, 2) if ask else None
    if mode == "bid":
        return round(bid, 2) if bid else None
    # 'limit' — mid-price
    if bid and ask:
        mid = (bid + ask) / 2
        # Round to nearest 0.01 but ensure we're at or below ask
        # (don't accidentally cross the ask)
        price = round(mid, 2)
        if price >= ask:
            price = round(ask - 0.01, 2)
        if price <= 0:
            price = round(ask, 2)  # fallback
        return price
    # Fallback: if we only have ask, place slightly below
    if ask:
        return round(ask - 0.01, 2) if ask > 0.02 else round(ask, 2)
    return None

def exec_buy(token_id, price, size_usd, label="", mode="limit", bid=None, ask=None):
    """Buy shares. Uses limit orders by default for price improvement.
    
    Args:
        token_id: token to buy
        price: the limit price to place at (pre-calculated by caller)
        size_usd: dollar amount to spend
        label: log label
        mode: 'limit' (GTC, default), 'aggressive' (GTC at ask), 'fok' (fill-or-kill)
        bid/ask: for logging context only
    
    Returns (order_id, shares_filled).
    """
    if not price or price <= 0: return None, 0
    shares = math.floor(size_usd / price * 100) / 100  # truncate to 2 decimals
    if shares < 5: return None, 0  # Polymarket minimum is 5 shares
    
    order_type = "GTC" if mode in ("limit", "aggressive") else "FOK"
    
    if DRY_RUN:
        tag = "LMT" if mode == "limit" else ("AGG" if mode == "aggressive" else "FOK")
        spread_s = ""
        if bid and ask:
            spread_s = f" [bid={bid:.3f} ask={ask:.3f}]"
        log.info(f"  📗 BUY {tag} @{price:.3f} ${size_usd:.2f} ({shares:.0f}sh){spread_s} | {label}")
        return f"dry_{int(time.time()*1000)}", shares
    
    try:
        from py_clob_client.order_builder.constants import BUY
        from py_clob_client.clob_types import OrderArgs, MarketOrderArgs, OrderType
        c = get_clob()
        if mode in ("aggressive", "fok"):
            # FAK: use MarketOrderArgs (pass dollar amount, lib handles precision)
            args = MarketOrderArgs(token_id=token_id, amount=round(size_usd, 2), side=BUY)
            signed = c.create_market_order(args)
            resp = c.post_order(signed, OrderType.FAK)
        else:
            # GTC limit: use OrderArgs with string-formatted decimals
            args = OrderArgs(
                token_id=token_id,
                price=float(f"{price:.2f}"),
                size=float(f"{shares:.2f}"),
                side=BUY
            )
            signed = c.create_order(args)
            resp = c.post_order(signed, OrderType.GTC)
        if resp and resp.get("orderID"):
            tag = "LMT" if mode == "limit" else ("AGG" if mode == "aggressive" else "FOK")
            
            # Try to get actual fill info from response
            actual_shares = shares  # default: assume full fill
            status = resp.get("status", "unknown")
            
            # FAK orders: check if anything actually matched
            if mode in ("aggressive", "fok"):
                # Some API versions return matched/filled info
                matched = resp.get("matchedAmount") or resp.get("filled") or resp.get("filledSize")
                if matched is not None:
                    try:
                        actual_shares = float(matched) / price if float(matched) > 0 else 0
                    except: pass
                # If status says not matched, zero out
                if status in ("CANCELLED", "EXPIRED", "REJECTED"):
                    log.warning(f"  ⚠️ Order {status}: {label}")
                    actual_shares = 0
            
            if actual_shares > 0:
                log.info(f"  📗 LIVE BUY {tag} @{price:.3f} ${size_usd:.2f} | {label}")
            else:
                log.warning(f"  ⚠️ BUY placed but 0 fill: {label}")
            
            return resp["orderID"], actual_shares
    except Exception as e:
        log.error(f"  BUY ERR: {e}")
    return None, 0

def exec_sell(token_id, price, shares, label="", mode="limit"):
    """Sell shares. Uses limit orders by default.
    
    Args:
        price: the price to sell at
        shares: number of shares to sell
        mode: 'limit' (GTC at bid), 'fok' (FOK for urgent exits)
    """
    if not price or price <= 0: return None, 0
    shares = math.floor(shares * 100) / 100  # truncate to 2 decimals
    if shares < 5: return None, 0  # Polymarket minimum
    revenue = round(shares * price, 2)
    
    order_type = "GTC" if mode == "limit" else "FOK"
    
    if DRY_RUN:
        tag = "LMT" if mode == "limit" else "FOK"
        log.info(f"  📕 SELL {tag} @{price:.3f} {shares:.0f}sh (${revenue:.2f}) | {label}")
        return f"dry_{int(time.time()*1000)}", revenue
    
    try:
        from py_clob_client.order_builder.constants import SELL
        from py_clob_client.clob_types import OrderArgs, MarketOrderArgs, OrderType
        c = get_clob()
        if mode == "fok":
            args = MarketOrderArgs(token_id=token_id, amount=round(revenue, 2), side=SELL)
            signed = c.create_market_order(args)
            resp = c.post_order(signed, OrderType.FAK)
        else:
            args = OrderArgs(
                token_id=token_id,
                price=float(f"{price:.2f}"),
                size=float(f"{shares:.2f}"),
                side=SELL
            )
            signed = c.create_order(args)
            resp = c.post_order(signed, OrderType.GTC)
        if resp and resp.get("orderID"):
            tag = "LMT" if mode == "limit" else "FOK"
            log.info(f"  📕 LIVE SELL {tag} @{price:.3f} {shares:.0f}sh | {label}")
            return resp["orderID"], revenue
    except Exception as e:
        err_str = str(e)
        if "not enough balance" in err_str or "not enough allowance" in err_str:
            log.warning(f"  ⚠️ SELL FAILED — no shares on-chain: {label}")
            return "NO_BALANCE", 0  # special marker: position doesn't exist
        log.error(f"  SELL ERR: {e}")
    return None, 0

# ═══════════════════════════════════════════════════════════════
# MARKET DISCOVERY
# ═══════════════════════════════════════════════════════════════
def discover_markets():
    """Fetch active sports markets — v4 PROVEN approach.
    Orders by 24h volume (sports naturally rank high) and
    filters by endDate (within 48h = live/upcoming matches).
    NO TAG FILTERING — Gamma tags are unreliable."""
    log.info("🔍 Discovering markets...")
    markets = []
    seen_ids = set()
    t0 = time.time()
    rejected = {"no_clob": 0, "not_sports": 0, "too_far": 0, "low_vol": 0,
                "no_end_date": 0, "accepted": 0}
    now_utc = datetime.now(timezone.utc)
    
    # V4's approach: fetch by volume, filter by end date
    for offset in [0, 200, 400, 600]:
        url = (f"{GAMMA}/markets?limit=200&offset={offset}"
               f"&active=true&closed=false"
               f"&order=volume24hr&ascending=false")
        
        data = api_get(url, timeout=15)
        if not data or not isinstance(data, list):
            log.warning(f"  offset={offset}: no data")
            continue
        
        log.info(f"  Fetched {len(data)} markets (offset={offset})")
        
        for m in data:
            mid = m.get("id", "")
            if mid in seen_ids: continue
            if not m.get("active") or m.get("closed"): continue
            seen_ids.add(mid)
            
            # End date filter: must close within 48h (sports matches)
            end_str = m.get("endDate") or m.get("end_date_iso") or ""
            hours_left = 48  # default: assume far out if no date
            if end_str:
                try:
                    end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    hours_left = (end_dt - now_utc).total_seconds() / 3600
                    if hours_left > 48 or hours_left < -1:
                        rejected["too_far"] += 1
                        continue
                except:
                    pass
            
            # CLOB tokens
            clob_ids = m.get("clobTokenIds", [])
            if isinstance(clob_ids, str):
                try: clob_ids = json.loads(clob_ids)
                except: clob_ids = []
            if not isinstance(clob_ids, list) or len(clob_ids) < 2:
                rejected["no_clob"] += 1
                continue
            
            # Volume filter: need some activity
            vol24 = float(m.get("volume24hr") or m.get("volume_24h") or 0)
            if vol24 < 100:
                rejected["low_vol"] += 1
                continue
            
            title = m.get("question", "") or m.get("title", "")
            
            # Soft sports filter — reject obvious non-sports but don't require patterns
            # This catches crypto/politics that have high volume
            if not is_sports_market(title, end_str):
                rejected["not_sports"] += 1
                continue
            
            typ = classify(title)
            rejected["accepted"] += 1
            
            markets.append({
                "mid": mid, "title": title, "typ": typ,
                "tok_yes": str(clob_ids[0]), "tok_no": str(clob_ids[1]),
                "volume": float(m.get("volume", 0) or 0),
                "volume24": vol24,
                "liquidity": float(m.get("liquidity", 0) or 0),
                "group": m.get("groupItemTitle", "") or m.get("conditionId", "") or mid,
                "event_slug": m.get("event_slug", m.get("groupSlug", "")),
                "hours_left": hours_left,
            })
        
        time.sleep(0.1)
        
        # If we have enough markets, stop fetching
        if len(markets) >= 100:
            break
    
    elapsed = time.time() - t0
    from collections import Counter
    tc = Counter(m["typ"] for m in markets)
    log.info(f"  ✅ {len(markets):,} sports markets in {elapsed:.1f}s | {dict(tc)}")
    log.info(f"  Filter: accepted={rejected['accepted']} too_far={rejected['too_far']} "
             f"not_sports={rejected['not_sports']} low_vol={rejected['low_vol']} "
             f"no_clob={rejected['no_clob']}")
    
    if markets:
        # Sort by soonest ending first — live/ending games get capital first
        markets.sort(key=lambda m: m["hours_left"])
        for m in markets[:5]:
            h = m["hours_left"]
            time_s = f"LIVE" if h <= 0 else (f"{h:.1f}h" if h < 24 else f"{h/24:.0f}d")
            log.info(f"    ✓ {m['typ']:<8s} {time_s:>5s}  vol24=${m['volume24']:>8,.0f}  {m['title'][:45]}")
    else:
        log.error("  ⚠ STILL 0 MARKETS — endDate filter may be too strict")
        log.error("  Retrying WITHOUT endDate filter...")
        
        # Emergency fallback: skip endDate, just use is_sports_market
        for offset in [0, 200]:
            url = (f"{GAMMA}/markets?limit=200&offset={offset}"
                   f"&active=true&closed=false"
                   f"&order=volume24hr&ascending=false")
            data = api_get(url, timeout=15)
            if not data: continue
            for m in data:
                mid = m.get("id", "")
                if mid in seen_ids: continue
                seen_ids.add(mid)
                clob_ids = m.get("clobTokenIds", [])
                if isinstance(clob_ids, str):
                    try: clob_ids = json.loads(clob_ids)
                    except: clob_ids = []
                if not isinstance(clob_ids, list) or len(clob_ids) < 2: continue
                title = m.get("question", "") or m.get("title", "")
                if not is_sports_market(title, ""):  continue
                typ = classify(title)
                markets.append({
                    "mid": mid, "title": title, "typ": typ,
                    "tok_yes": str(clob_ids[0]), "tok_no": str(clob_ids[1]),
                    "volume": float(m.get("volume", 0) or 0),
                    "volume24": float(m.get("volume24hr") or 0),
                    "liquidity": float(m.get("liquidity", 0) or 0),
                    "group": m.get("groupItemTitle", "") or m.get("conditionId", "") or mid,
                    "event_slug": m.get("event_slug", m.get("groupSlug", "")),
                    "hours_left": 48,  # unknown end date, put at back of queue
                })
            time.sleep(0.1)
        
        # Sort fallback markets too
        markets.sort(key=lambda m: m["hours_left"])
        log.info(f"  Fallback found {len(markets)} markets (no endDate filter)")
        for m in markets[:5]:
            log.info(f"    ✓ {m['typ']:<8s} {m['title'][:55]}")
    
    return markets

def arb_engine(all_markets):
    """DEDICATED ARB ENGINE — runs EVERY cycle (~50ms), ZERO sleeps.
    
    CORE profit generator. Models RN1's behavior:
    RN1 hammers 50-150+ trades per market, eating entire order book.
    
    PHASE 1: BUILD EXISTING ARBS
      Path A: Unmatched NO → buy YES to LOCK (ROI: would_improve_roi check)
      Path B: Already locked → add matched pairs (ROI: would_improve_roi + pair_cost < 1.0)
    
    PHASE 2: SCAN ALL MARKETS for new arbs
      ROI: combined < 0.99 = guaranteed 1%+ profit per pair
    
    ALL arb actions verified through ROI gate before execution.
    Returns: number of arb trades executed this cycle
    """
    trades = 0
    
    # ═══ PHASE 1: BUILD / LOCK EXISTING POSITIONS ═══
    # Get all positions that have a YES token (arb candidates)
    arb_positions = [(mid, pos) for mid, pos in portfolio.positions.items()
                     if pos.shares > 0 and pos.tok_yes]
    
    if arb_positions:
        # Batch fetch ALL prices in one parallel call
        arb_tokens = []
        for mid, pos in arb_positions:
            arb_tokens.append(pos.tok_no)
            arb_tokens.append(pos.tok_yes)
        arb_cache = smart_batch_fetch(arb_tokens)
        
        for mid, pos in arb_positions:
            no_ask, _, no_bid, _ = arb_cache.get(pos.tok_no, (None, 0, None, 0))
            yes_ask, _, yes_bid, _ = arb_cache.get(pos.tok_yes, (None, 0, None, 0))
            if not no_ask or not yes_ask or yes_ask <= 0.001: continue
            
            # Update position cache
            pos.current_ask = no_ask
            pos.current_bid = no_bid
            pos.current_yes_ask = yes_ask
            pos.current_yes_bid = yes_bid
            
            # ── PATH A: NOT YET LOCKED — buy YES to create arb ──
            if not pos.is_arb_locked and pos.avg_price > 0:
                combined = pos.avg_price + yes_ask
                spread_pct = 1.0 - combined
                if combined < ARB_LOCK_THRESHOLD and spread_pct >= ARB_LOCK_MIN_SPREAD:
                    unmatched = max(0, pos.shares - pos.yes_shares)
                    if unmatched > 0:
                        room = portfolio.arb_room_for(mid, pos.group_key)
                        target = min(unmatched, room / yes_ask if yes_ask > 0 else 0)
                        if target >= 1:
                            yes_cost = round(yes_ask * target, 2)
                            improves, _, _ = pos.would_improve_roi('buy_yes', yes_ask, yes_cost)
                            if improves and yes_cost >= 1 and rate_ok():
                                label = f"🔒ARB-LOCK|{spread_pct*100:.1f}%|{pos.short()}"
                                oid, sh = exec_buy(pos.tok_yes, yes_ask, yes_cost,
                                                   label, mode="aggressive")
                                if oid and oid != "NO_BALANCE" and sh > 0:
                                    pos.record_yes_buy(yes_ask, yes_cost)
                                    gp = pos.guaranteed_profit or 0
                                    log.info(f"  🔒 LOCKED! {pos.short()} "
                                            f"NO@{pos.avg_price:.3f}+YES@{yes_ask:.3f}="
                                            f"{combined:.3f} → ${gp:.2f} guaranteed")
                                    portfolio.total_cost += yes_cost
                                    portfolio.n_arb_locks += 1
                                    record_order()
                                    trades += 1
                                    # NO SLEEP — keep going
            
            # ── PATH B: ALREADY LOCKED — eat more of the book ──
            elif pos.is_arb_locked:
                pair_cost = no_ask + yes_ask
                if pair_cost < 1.0:  # still profitable at current prices
                    room = portfolio.arb_room_for(mid, pos.group_key)
                    max_pairs = room / pair_cost if pair_cost > 0 else 0
                    if max_pairs >= 1:
                        batch_no_cost = round(no_ask * max_pairs, 2)
                        batch_yes_cost = round(yes_ask * max_pairs, 2)
                        batch_total = batch_no_cost + batch_yes_cost
                        batch_profit = max_pairs - batch_total
                        
                        # ROI GATE: verify this actually increases guaranteed profit
                        improves, new_gp, old_gp = pos.would_improve_roi(
                            'buy_matched', no_ask, batch_total)
                        
                        if improves and batch_profit > 0.10 and batch_no_cost >= 1 and rate_ok():
                            oid_no, sh_no = exec_buy(pos.tok_no, no_ask, batch_no_cost,
                                f"🔒ARB+NO|{pair_cost:.3f}|{pos.short()}",
                                mode="aggressive")
                            if oid_no and sh_no > 0:
                                matched_yes_cost = round(yes_ask * sh_no, 2)
                                oid_yes, sh_yes = exec_buy(pos.tok_yes, yes_ask,
                                    matched_yes_cost,
                                    f"🔒ARB+YES|{pair_cost:.3f}|{pos.short()}",
                                    mode="aggressive")
                                pos.record_buy(no_ask, batch_no_cost)
                                if oid_yes and sh_yes > 0:
                                    pos.record_yes_buy(yes_ask, matched_yes_cost)
                                    portfolio.total_cost += batch_no_cost + matched_yes_cost
                                else:
                                    portfolio.total_cost += batch_no_cost
                                gp = pos.guaranteed_profit or 0
                                log.info(f"  🔒 BUILD! {pos.short()} "
                                        f"+{sh_no:.0f} pairs @{pair_cost:.3f} "
                                        f"→ ${gp:.2f} guaranteed")
                                record_order()
                                trades += 1
                                # NO SLEEP — keep eating
    
    # ═══ PHASE 2: SCAN ALL MARKETS FOR NEW ARBS ═══
    if not all_markets:
        if trades: log.info(f"  🔒 ARB ENGINE: {trades} trades (build only)")
        return trades
    
    # Only markets we're NOT in and that have YES tokens
    new_markets = [m for m in all_markets
                   if not portfolio.has(m["mid"]) and m.get("tok_yes")]
    if not new_markets:
        if trades: log.info(f"  🔒 ARB ENGINE: {trades} trades (build only)")
        return trades
    
    # Batch fetch all new market prices
    scan_tokens = []
    for m in new_markets:
        scan_tokens.append(m["tok_no"])
        scan_tokens.append(m["tok_yes"])
    scan_cache = smart_batch_fetch(scan_tokens)
    
    # Find and sort arb opportunities
    arbs = []
    for m in new_markets:
        no_ask, _, no_bid, _ = scan_cache.get(m["tok_no"], (None, 0, None, 0))
        yes_ask, _, _, _ = scan_cache.get(m["tok_yes"], (None, 0, None, 0))
        if not no_ask or not yes_ask or yes_ask <= 0.001: continue
        combined = no_ask + yes_ask
        spread_pct = 1.0 - combined
        if combined < ARB_LOCK_THRESHOLD and spread_pct >= ARB_LOCK_MIN_SPREAD:
            arbs.append((combined, m, no_ask, no_bid, yes_ask, spread_pct))
    
    # Sort by best margin first
    arbs.sort(key=lambda x: x[0])
    
    for combined, m, no_ask, no_bid, yes_ask, spread_pct in arbs:
        if not rate_ok(is_new=True): break
        
        mid = m["mid"]
        arb_room = portfolio.arb_room_for(mid, m["group"])
        if arb_room < 2: continue
        
        pair_cost = no_ask + yes_ask
        target_shares = arb_room / pair_cost if pair_cost > 0 else 0
        no_size = round(no_ask * target_shares, 2)
        if no_size < 1: continue
        
        label_no = f"🔒NEW-NO|{spread_pct*100:.1f}%|{m['title'][:25]}"
        oid_no, sh_no = exec_buy(m["tok_no"], no_ask, no_size, label_no,
                                 mode="aggressive", bid=no_bid, ask=no_ask)
        if not oid_no or sh_no <= 0: continue
        
        yes_size = round(yes_ask * sh_no, 2)
        label_yes = f"🔒NEW-YES|{spread_pct*100:.1f}%|{m['title'][:25]}"
        oid_yes, sh_yes = exec_buy(m["tok_yes"], yes_ask, yes_size,
                                   label_yes, mode="aggressive")
        
        pos = Position(mid, m["title"], m["typ"], m["tok_no"],
                     m["tok_yes"], m["group"])
        pos.record_buy(no_ask, no_size)
        total_spent = no_size
        if oid_yes and sh_yes > 0:
            pos.record_yes_buy(yes_ask, yes_size)
            total_spent += yes_size
        
        guaranteed = min(pos.shares, pos.yes_shares) - pos.total_cost
        log.info(f"  🔒 NEW ARB! {m['title'][:45]}")
        log.info(f"     NO@{no_ask:.3f}+YES@{yes_ask:.3f}={combined:.3f} "
                f"({spread_pct*100:.1f}% → ${guaranteed:.2f} locked)")
        pos.current_ask = no_ask
        pos.current_bid = no_bid
        if no_bid and no_ask > 0:
            pos.spread_at_entry = (no_ask - no_bid) / no_ask
        portfolio.add_position(pos)
        portfolio.n_entries += 1
        portfolio.n_arb_locks += 1
        portfolio.total_cost += total_spent
        record_order(is_new=True)
        trades += 1
        # Subscribe new tokens to WS for instant prices next cycle
        ws_start([m["tok_no"], m["tok_yes"]])
        # NO SLEEP — keep scanning
    
    if trades:
        log.info(f"  🔒 ARB ENGINE: {trades} trades this cycle")
    return trades


# ═══════════════════════════════════════════════════════════════
# PILLAR 5B: TRIAD/DUO ARB SCANNER
# ═══════════════════════════════════════════════════════════════
def scan_cross_market_arbs(all_markets):
    """
    Find guaranteed profit across related markets (parallel fetch):
    
    TRIAD (soccer): Win A NO + Win B NO + Draw NO → payout $2 guaranteed
      If combined < $1.97 → lock it (1.5%+ guaranteed)
    
    DUO (basketball/hockey): Win A NO + Win B NO → payout $1 guaranteed
      If combined < $0.97 → lock it
    
    Groups markets by event, batch fetches all NO prices, executes if profitable.
    """
    from collections import defaultdict
    event_groups = defaultdict(list)
    for m in all_markets:
        group = m.get("group", "")
        if not group or group == m["mid"]: continue
        event_groups[group].append(m)
    
    # Pre-fetch ALL tokens for all candidate groups in ONE batch
    all_tokens = []
    for event, mkts in event_groups.items():
        if len(mkts) >= 2:
            for m in mkts:
                all_tokens.append(m["tok_no"])
    
    if not all_tokens:
        return 0
    
    price_cache = smart_batch_fetch(all_tokens)
    locked = 0
    
    for event, mkts in event_groups.items():
        if len(mkts) < 2: continue
        if not rate_ok(is_new=True): break
        
        wins = [m for m in mkts if m["typ"] == "win"]
        draws = [m for m in mkts if m["typ"] == "draw"]
        matches = [m for m in mkts if m["typ"] == "match"]
        
        all_held = all(portfolio.has(m["mid"]) for m in mkts)
        if all_held: continue
        
        candidates = []
        if len(wins) >= 2 and len(draws) >= 1:
            candidates.append((wins[:2] + draws[:1], TRIAD_LOCK_THRESHOLD, 2.0, "TRIAD"))
        if len(wins) >= 2 and len(draws) == 0:
            candidates.append((wins[:2], DUO_LOCK_THRESHOLD, 1.0, "DUO"))
        if len(matches) >= 1 and len(wins) >= 1:
            candidates.append((matches[:1] + wins[:1], DUO_LOCK_THRESHOLD, 1.0, "DUO"))
        
        for group_mkts, threshold, payout, label in candidates:
            # Use pre-fetched prices (no serial HTTP!)
            prices = []
            for m in group_mkts:
                no_ask, depth, _, _ = price_cache.get(m["tok_no"], (None, 0, None, 0))
                if not no_ask or no_ask <= 0 or depth < 3:
                    break
                prices.append((m, no_ask))
            
            if len(prices) != len(group_mkts): continue
            
            combined = sum(p for _, p in prices)
            spread = payout - combined
            
            if combined < threshold and spread >= ARB_LOCK_MIN_SPREAD * payout:
                budget_per_leg = min(BANKROLL * ARB_MAX_PER_MARKET,
                                    portfolio.arb_room_for(prices[0][0]["mid"],
                                    event) / len(prices))
                min_shares = min(budget_per_leg / p for _, p in prices) if all(p > 0 for _, p in prices) else 0
                
                if min_shares < 5: continue
                
                total_cost = 0
                all_ok = True
                positions_created = []
                
                for m, no_ask in prices:
                    cost = no_ask * min_shares
                    if cost < 1:
                        all_ok = False; break
                    leg_label = f"🔒{label}-NO|{spread/payout*100:.1f}%|{m['title'][:25]}"
                    oid, shares = exec_buy(m["tok_no"], no_ask, cost, leg_label,
                                           mode="aggressive")
                    if not oid:
                        all_ok = False; break
                    
                    if not portfolio.has(m["mid"]):
                        pos = Position(m["mid"], m["title"], m["typ"],
                                      m["tok_no"], m["tok_yes"], event)
                        pos.record_buy(no_ask, cost)
                        pos.current_ask = no_ask
                        portfolio.add_position(pos)
                        portfolio.n_entries += 1
                    else:
                        pos = portfolio.get(m["mid"])
                        pos.record_buy(no_ask, cost)
                    
                    total_cost += cost
                    positions_created.append(pos)
                    record_order(is_new=True)
                
                if all_ok and positions_created:
                    guaranteed = payout * min_shares - total_cost
                    roi = guaranteed / total_cost * 100
                    log.info(f"  🔒 {label} ARB LOCKED! {event[:40]}")
                    log.info(f"     {len(prices)} legs × {min_shares:.0f}sh = ${total_cost:.2f} "
                            f"→ ${payout*min_shares:.2f} payout")
                    log.info(f"     💰 Guaranteed: ${guaranteed:.2f} ({roi:.1f}% ROI)")
                    portfolio.n_arb_locks += 1
                    portfolio.total_cost += total_cost
                    locked += 1
    
    if locked > 0:
        log.info(f"  🔒 Locked {locked} cross-market arbs!")
    return locked

# ═══════════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════════
_start_time = time.time()

def dashboard():
    os.system("clear" if os.name != "nt" else "cls")
    now_s = datetime.now().strftime("%H:%M:%S")
    runtime = (time.time() - _start_time) / 60
    pf = portfolio
    
    print(f"\033[1;96m╔═══════════════════════════════════════════════════════════════════════════╗\033[0m")
    print(f"\033[1;96m║  PolyArb v9 — FULL RN1 STRATEGY   {now_s}  {'DRY' if DRY_RUN else 'LIVE':>4s}  ⏱ {runtime:.0f}m       ║\033[0m")
    print(f"\033[1;96m╚═══════════════════════════════════════════════════════════════════════════╝\033[0m")
    
    deployed = pf.deployed()
    est_val = pf.est_value()
    active = pf.active_count()
    unreal_pnl = est_val - deployed
    locked_list = [p for p in pf.positions.values() if p.is_arb_locked]
    locked_total = sum((p.guaranteed_profit or 0) for p in locked_list)
    
    print(f"  💰 Bank: ${BANKROLL:,.0f}  Deployed: ${deployed:,.2f} ({deployed/max(BANKROLL,1)*100:.0f}%)  "
          f"Est Value: ${est_val:,.2f}  Unrealized: ${unreal_pnl:+,.2f}")
    if locked_list:
        print(f"  🔒 Locked: {len(locked_list)} positions  Guaranteed: \033[92m${locked_total:+,.2f}\033[0m")
    
    print(f"  📊 Positions: {active}  Entries: {pf.n_entries}  "
          f"Hedges: {pf.n_profit_takes}  Cuts: {pf.n_loss_cuts}  Adds: {pf.n_dip_buys}  "
          f"🔒Arbs: {pf.n_arb_locks}  "
          f"Realized: ${pf.total_realized:,.2f}")
    
    # Type breakdown
    type_counts = defaultdict(int)
    type_cost = defaultdict(float)
    for p in pf.positions.values():
        if p.shares > 0:
            type_counts[p.typ] += 1
            type_cost[p.typ] += p.total_cost
    
    if type_counts:
        print(f"\n  \033[1mBY TYPE:\033[0m")
        for typ in sorted(type_counts, key=lambda t: -type_counts[t]):
            print(f"    {typ:<10s}: {type_counts[typ]:>3d} pos  ${type_cost[typ]:>8,.2f}")
    
    # Top positions by value
    active_pos = sorted([p for p in pf.positions.values() if p.shares > 0],
                        key=lambda p: -p.total_cost)
    locked_pos = [p for p in active_pos if p.is_arb_locked]
    open_pos = [p for p in active_pos if not p.is_arb_locked]
    
    # Total guaranteed profit across all locked positions
    total_guaranteed = sum(p.guaranteed_profit or 0 for p in locked_pos)
    
    if locked_pos:
        print(f"\n  \033[92;1m🔒 ARB-LOCKED POSITIONS ({len(locked_pos)}) "
              f"— Guaranteed: ${total_guaranteed:+.2f}\033[0m")
        print(f"  {'Market':<34s} {'NO':>10s} {'YES':>10s} {'Cost':>7s} {'G.Profit':>9s} {'ROI':>6s}")
        for p in locked_pos[:8]:
            gp = p.guaranteed_profit or 0
            roi = p.guaranteed_roi * 100
            clr = "\033[92m" if gp > 0 else "\033[91m"
            print(f"  {p.short():<34s} "
                  f"{p.shares:.0f}@{p.avg_price:.2f} "
                  f"{p.yes_shares:.0f}@{p.yes_avg_price:.2f} "
                  f"${p.total_cost:>6.2f} "
                  f"{clr}${gp:>+7.2f} {roi:>+5.1f}%\033[0m")
    
    if open_pos:
        print(f"\n  \033[1mOPEN POSITIONS ({len(open_pos)}):\033[0m")
        print(f"  {'Market':<38s} {'Type':>7s} {'Cost':>8s} {'Bid':>6s} {'P&L':>7s} {'Buys':>4s}")
        for p in open_pos[:25]:
            pnl = p.pnl_pct * 100
            clr = "\033[92m" if pnl > 10 else ("\033[91m" if pnl < -20 else "\033[0m")
            bid_s = f"@{p.current_bid:.3f}" if p.current_bid else "  ???"
            print(f"  {p.short():<38s} {p.typ:>7s} ${p.cost:>7.2f} {bid_s} "
                  f"{clr}{pnl:>+6.1f}%\033[0m {p.n_buys:>4d}")
        if len(open_pos) > 25:
            print(f"  \033[2m  ... and {len(open_pos) - 25} more positions\033[0m")
    
    # Near arbs (positions that could be locked)
    near_arbs = [(p, p.avg_price + (p.current_yes_ask or 1))
                 for p in open_pos
                 if p.tok_yes and p.current_yes_ask and p.avg_price > 0
                 and not p.is_arb_locked
                 and (p.avg_price + p.current_yes_ask) < 1.03]
    near_arbs.sort(key=lambda x: x[1])
    
    if near_arbs:
        print(f"\n  \033[1mNEAR ARB OPPORTUNITIES:\033[0m")
        for p, comb in near_arbs[:5]:
            gap = (comb - ARB_LOCK_THRESHOLD) * 100
            print(f"  \033[96m  🔒 {p.short():<30s} "
                  f"combined={comb:.3f} ({gap:+.1f}% to lock)\033[0m")
    
    churn = (pf.total_cost + pf.total_realized) / max(BANKROLL, 1)
    print(f"\n  {'═'*70}")
    print(f"  Churn: {churn:.1f}x bankroll  |  Rate: {len(_order_ts)}/min  {len(_new_mkt_ts)}/hr new")

# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════
def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--bankroll", type=float, default=1000)
    p.add_argument("--fresh", action="store_true")
    p.add_argument("--live", action="store_true")
    p.add_argument("--scan", action="store_true", help="Scan only, no trades")
    p.add_argument("--turbo", action="store_true", help="Faster cycles")
    p.add_argument("--no-dash", action="store_true")
    args = p.parse_args()
    
    global BANKROLL, DRY_RUN
    BANKROLL = args.bankroll
    if args.live: DRY_RUN = False
    # --turbo is legacy flag, arb engine already runs at max speed (50ms cycle)
    
    if args.fresh:
        for f in ["portfolio_v9.json", "ledger.json"]:
            if os.path.exists(f): os.remove(f)
    else:
        portfolio.load()
    
    # Always try wallet sync (works in live mode, gracefully fails in dry)
    if not args.scan:
        portfolio.sync_wallet()
    
    running = True
    def _sig(s, f):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)
    
    log.info("=" * 60)
    log.info(f"PolyArb v9 — PURE ARB ENGINE")
    log.info(f"  Bankroll: ${BANKROLL:,.0f}  Mode: {'DRY' if DRY_RUN else 'LIVE'}  "
             f"Turbo: {args.turbo}  Scan: {args.scan}")
    log.info(f"  Arb limits: {ARB_MAX_PER_MARKET*100:.0f}%/market {ARB_MAX_PER_EVENT*100:.0f}%/event "
             f"(${BANKROLL*ARB_MAX_PER_MARKET:.0f}/${BANKROLL*ARB_MAX_PER_EVENT:.0f})")
    log.info(f"  Arb threshold: YES+NO < {ARB_LOCK_THRESHOLD} (min {ARB_LOCK_MIN_SPREAD*100:.1f}% spread)")
    log.info(f"  Strategy: ARB-ONLY — no speculative one-sided bets")
    log.info(f"  Orders: AGGRESSIVE at ask — speed over price")
    log.info("=" * 60)
    
    # Run first discovery IMMEDIATELY before starting loop
    # so user can see what markets we find
    log.info("Running initial market discovery...")
    all_markets = discover_markets()
    # Markets already sorted by hours_left (soonest first) in discover_markets
    last_discover = time.time()
    last_save = 0
    last_dash = time.time()  # delay first dashboard by 5s so logs are visible
    
    if all_markets:
        log.info(f"  Ready to trade {len(all_markets)} sports markets! (soonest-ending first)")
        # Show first 5 as preview
        for m in all_markets[:5]:
            h = m.get("hours_left", 99)
            time_s = f"LIVE" if h <= 0 else (f"{h:.1f}h" if h < 24 else f"{h/24:.0f}d")
            log.info(f"    → {m['typ']:<8s} {time_s:>5s}  {m['title'][:50]}")
        
        # Start WebSocket for real-time prices on all NO+YES tokens
        ws_tokens = []
        for m in all_markets:
            ws_tokens.append(m["tok_no"])
            if m.get("tok_yes"):
                ws_tokens.append(m["tok_yes"])
        ws_start(ws_tokens)
    else:
        log.warning("  ⚠ NO SPORTS MARKETS FOUND — check logs/v9_*.log for details")
    
    time.sleep(3)  # pause so user can read discovery output
    
    while running:
        now = time.time()
        
        # ─── DISCOVER: refresh market list every 30s (find new arbs fast) ───
        if now - last_discover >= 30:
            all_markets = discover_markets()
            last_discover = now
            # Subscribe any new tokens to WebSocket
            if all_markets:
                new_ws = []
                for m in all_markets:
                    new_ws.append(m["tok_no"])
                    if m.get("tok_yes"): new_ws.append(m["tok_yes"])
                ws_start(new_ws)
        
        # ─── ARB ENGINE — runs EVERY cycle (~50ms) ───
        # Binary arbs (YES+NO same market) + cross-market triads/duos
        # This is the ONLY thing that buys. No speculative one-sided bets.
        if all_markets and not args.scan:
            arb_engine(all_markets)
            scan_cross_market_arbs(all_markets)
        
        # ─── SCAN MODE: batch price-check (no trading) ───
        if args.scan and all_markets:
            # Fetch ALL prices in one batch
            all_tokens = []
            for m in all_markets:
                all_tokens.append(m["tok_no"])
                if m.get("tok_yes"): all_tokens.append(m["tok_yes"])
            cache = smart_batch_fetch(all_tokens)
            arb_found = 0
            for m in all_markets:
                no_ask, _, no_bid, _ = cache.get(m["tok_no"], (None, 0, None, 0))
                yes_ask, _, _, _ = cache.get(m.get("tok_yes", ""), (None, 0, None, 0))
                if no_ask and yes_ask and yes_ask > 0.001:
                    combined = no_ask + yes_ask
                    if combined < ARB_LOCK_THRESHOLD:
                        spread = (1.0 - combined) * 100
                        log.info(f"  🔒 ARB: {m['title'][:45]} "
                                f"NO@{no_ask:.3f}+YES@{yes_ask:.3f}={combined:.3f} "
                                f"({spread:.1f}%)")
                        arb_found += 1
            if arb_found:
                log.info(f"  Found {arb_found} arb opportunities")
            else:
                log.info(f"  No arbs found in {len(all_markets)} markets")
        
        # ─── SAVE every 30s ───
        if now - last_save >= 30:
            portfolio.save()
            last_save = now
        
        # ─── DASHBOARD every 5s ───
        if not args.no_dash and now - last_dash >= 5:
            dashboard()
            last_dash = now
        
        time.sleep(0.05)  # 50ms cycle = 20 arb checks/second
    
    portfolio.save()
    log.info(f"SHUTDOWN | Positions: {portfolio.active_count()} "
             f"Deployed: ${portfolio.deployed():.2f} Realized: ${portfolio.total_realized:.2f}")

if __name__ == "__main__":
    main()
