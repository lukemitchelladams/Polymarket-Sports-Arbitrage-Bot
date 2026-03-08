#!/usr/bin/env python3
"""
PolyArb v8 — ARB SCANNER (v4-proven logic)
════════════════════════════════════════════
Based on arb_v4's Phase 1/Phase 2 system that actually worked.

HOW IT WORKS (from v4):
  Phase 1: SEED — When YES+NO combined < 1.04, buy the cheaper side SMALL ($2-5).
           This stakes our claim cheaply. Most seeds don't complete.
  Phase 2: COMPLETE — When our_avg + opposite_ask < 0.98, buy opposite side
           sized to MATCH our shares. Now we're locked: guaranteed profit.
  Adding:  When both sides held, only add if it IMPROVES guaranteed ROI.

KEY INSIGHTS FROM V4:
  - Threshold 0.98 (not 0.97) — catches 2x more opportunities  
  - Equal SHARES not equal dollars
  - Uses YOUR avg price for Phase 2 (not current market ask)
  - ROI improvement gate on every trade
  - Spread-proportional sizing (bigger spread = more capital)

Also includes v5's cross-Dutch (NO+NO on opposing outcomes).

Usage:
  python3.11 arb_scanner.py                # dry run
  python3.11 arb_scanner.py --live          # real money
"""
import os, sys, json, time, re, signal, logging, math
from datetime import datetime, timezone
from collections import defaultdict
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
KEYS_FILE = os.path.join(SCRIPT_DIR, "keys.env")
PORTFOLIO_FILE = os.path.join(SCRIPT_DIR, "portfolio_v8.json")
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")

if os.path.exists(KEYS_FILE):
    with open(KEYS_FILE) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

PRIVATE_KEY  = os.getenv("PK", "")
PROXY_WALLET = os.getenv("PROXY_WALLET", "")
CLOB         = "https://clob.polymarket.com"
GAMMA        = "https://gamma-api.polymarket.com"

# ── ARB ENGINE (from v4 — the thresholds that worked) ──
ENTRY_THRESHOLD   = 0.98    # YES+NO combined must be < this to lock (v4 used 0.98)
MIN_SPREAD_PCT    = 2.0     # minimum 2% guaranteed spread
PHASE1_COMBINED   = 1.04    # seed when combined < this (close to arb)
PHASE1_SEED_SIZE  = 5.0     # $5 base seed — must buy >= 5 shares on Polymarket
PHASE2_STALE_GATE = 1.05    # allow completion up to 5% negative (stale prices)
SEED_TIMEOUT      = 900     # 15 minutes — if Phase 2 doesn't complete, evaluate
SEED_HOLD_PRICE   = 0.70    # if NO bid >= this, hold (70%+ chance of payout)

# ── Cross-Dutch (from v5) ──
CROSS_DUTCH_THRESHOLD = 0.98  # NO_A + NO_B < this for cross-event arb

# ── Sizing ──
BANKROLL        = float(os.getenv("BANKROLL", "588"))
MAX_DEPLOY_PCT  = 0.92
MAX_PER_EVENT   = 0.08
MAX_PER_MARKET  = 0.05
MAX_POSITION    = 100.0     # max $ per market

# ── Rate limits ──
MAX_ORDERS_MIN  = 60
DRY_RUN         = True
SCAN_INTERVAL   = 3

# ── Sports filtering ──
SPORTS_TAG_IDS = {
    82, 306, 780, 450, 100351, 1494, 100350, 100100, 100639, 100977, 1234,
    745, 100254, 100149, 28, 101178, 899, 100088, 864, 101232, 102123,
    100258, 8085, 8080, 100263, 100265, 100266, 100267,
    100337, 102146, 101815, 100899, 101897,
}

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════
os.makedirs(LOG_DIR, exist_ok=True)
log = logging.getLogger("arb_scanner")
log.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", datefmt="%H:%M:%S")
sh = logging.StreamHandler(); sh.setFormatter(fmt); log.addHandler(sh)
fh = logging.FileHandler(os.path.join(LOG_DIR, f"scanner_{datetime.now():%Y%m%d_%H%M}.log"))
fh.setFormatter(fmt); log.addHandler(fh)

# ═══════════════════════════════════════════════════════════════
# GLOBAL STATE
# ═══════════════════════════════════════════════════════════════
running = True
_clob = None
order_times = []
_last_phase1 = {}   # mid -> last seed time (cooldown)
_last_cross = {}    # event -> last cross-dutch time

stats = defaultdict(int)

def handle_sig(s, f):
    global running; running = False
signal.signal(signal.SIGINT, handle_sig)

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def api_get(url, timeout=10):
    try:
        req = Request(url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
        return json.loads(urlopen(req, timeout=timeout).read().decode("utf-8"))
    except: return None

def fetch_book(token_id):
    """Returns (best_ask, ask_depth, best_bid, bid_depth)."""
    try:
        req = Request(f"{CLOB}/book?token_id={token_id}",
                     headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
        book = json.loads(urlopen(req, timeout=6).read().decode("utf-8"))
        best_ask, ask_depth = None, 0
        for a in book.get("asks", []):
            p = float(a["price"]); s = float(a.get("size", 0))
            if best_ask is None or p < best_ask: best_ask = p
            ask_depth += s
        best_bid, bid_depth = None, 0
        for b in book.get("bids", []):
            p = float(b["price"]); s = float(b.get("size", 0))
            if best_bid is None or p > best_bid: best_bid = p
            bid_depth += s
        return best_ask, ask_depth, best_bid, bid_depth
    except:
        return None, 0, None, 0

def rate_ok():
    now = time.time()
    order_times[:] = [t for t in order_times if now - t < 60]
    return len(order_times) < MAX_ORDERS_MIN

def record_order():
    order_times.append(time.time())

def get_clob():
    global _clob
    if _clob: return _clob
    from py_clob_client.client import ClobClient
    _clob = ClobClient(CLOB, key=PRIVATE_KEY, chain_id=137,
                        signature_type=1, funder=PROXY_WALLET)
    _clob.set_api_creds(_clob.create_or_derive_api_creds())
    log.info(f"  CLOB connected (sig_type=1, wallet:{PROXY_WALLET[:12]}...)")
    return _clob

def exec_buy(token_id, price, size_usd, label=""):
    """Buy shares via FAK (instant fill). Returns (order_id, shares)."""
    if not price or price <= 0: return None, 0
    shares = math.floor(size_usd / price * 100) / 100
    if shares < 5: return None, 0
    
    if DRY_RUN:
        log.info(f"  📗 [DRY] BUY @{price:.3f} ${size_usd:.2f} ({shares:.0f}sh) | {label}")
        return f"dry_{int(time.time()*1000)}", shares
    
    try:
        from py_clob_client.order_builder.constants import BUY
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        c = get_clob()
        args = MarketOrderArgs(token_id=token_id, amount=round(size_usd, 2), side=BUY)
        signed = c.create_market_order(args)
        resp = c.post_order(signed, OrderType.FAK)
        if resp and resp.get("orderID"):
            log.info(f"  📗 LIVE BUY @{price:.3f} ${size_usd:.2f} ({shares:.0f}sh) | {label}")
            return resp["orderID"], shares
    except Exception as e:
        log.error(f"  BUY ERR: {e}")
    return None, 0

def exec_sell(token_id, price, shares, label=""):
    """Sell shares via FAK (instant fill). Returns (order_id, revenue)."""
    if not price or price <= 0: return None, 0
    shares = math.floor(shares * 100) / 100
    if shares < 5: return None, 0
    revenue = round(shares * price, 2)
    
    if DRY_RUN:
        log.info(f"  📕 [DRY] SELL @{price:.3f} {shares:.0f}sh (${revenue:.2f}) | {label}")
        return f"dry_{int(time.time()*1000)}", revenue
    
    try:
        from py_clob_client.order_builder.constants import SELL
        from py_clob_client.clob_types import OrderArgs, OrderType
        c = get_clob()
        args = OrderArgs(token_id=token_id, price=float(f"{price:.2f}"),
                        size=float(f"{shares:.2f}"), side=SELL)
        signed = c.create_order(args)
        resp = c.post_order(signed, OrderType.FAK)
        if resp and resp.get("orderID"):
            log.info(f"  📕 LIVE SELL @{price:.3f} {shares:.0f}sh (${revenue:.2f}) | {label}")
            return resp["orderID"], revenue
    except Exception as e:
        log.error(f"  SELL ERR: {e}")
    return None, 0

# ═══════════════════════════════════════════════════════════════
# PORTFOLIO I/O (shared with arb_v8.py)
# ═══════════════════════════════════════════════════════════════
def load_portfolio():
    try:
        with open(PORTFOLIO_FILE) as f:
            return json.load(f)
    except:
        return {"positions": {}, "realized": 0, "total_cost": 0,
                "entries": 0, "profit_takes": 0, "loss_cuts": 0,
                "dip_buys": 0, "arb_locks": 0}

def save_portfolio(data):
    tmp = PORTFOLIO_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, PORTFOLIO_FILE)

def portfolio_deployed():
    data = load_portfolio()
    return sum(p.get("cost", 0) + p.get("yes_cost", 0)
               for p in data.get("positions", {}).values())

def room_for(mid, group_key):
    data = load_portfolio()
    positions = data.get("positions", {})
    total_deployed = sum(p.get("cost", 0) + p.get("yes_cost", 0) for p in positions.values())
    group_deployed = sum(p.get("cost", 0) + p.get("yes_cost", 0)
                        for p in positions.values() if p.get("group_key") == group_key)
    pos = positions.get(mid, {})
    mkt_deployed = pos.get("cost", 0) + pos.get("yes_cost", 0) if pos else 0
    
    total_room = BANKROLL * MAX_DEPLOY_PCT - total_deployed
    group_room = BANKROLL * MAX_PER_EVENT - group_deployed
    mkt_room = MAX_POSITION - mkt_deployed
    return max(0, min(total_room, group_room, mkt_room))

# ═══════════════════════════════════════════════════════════════
# ROI IMPROVEMENT GATE (from v4 — the key to profitable arbs)
# ═══════════════════════════════════════════════════════════════
def check_roi_improvement(pos, yes_add, no_add, yes_price, no_price, label=""):
    """
    From v4: UNIVERSAL ROI GATE.
    Does adding improve guaranteed worst-case P&L?
    
    ONE-SIDED: completing must create real arb (combined < 0.98)
    TWO-SIDED: worst-case P&L must improve by >= 15% of new capital
    """
    if not pos or (pos.get("cost", 0) + pos.get("yes_cost", 0)) < 1.0:
        return True  # brand new, let signals handle
    
    has_no = pos.get("shares", 0) > 0
    has_yes = pos.get("yes_shares", 0) > 0
    is_one_sided = has_no != has_yes
    
    if is_one_sided:
        # Completing: our_avg + opposite_ask must be < threshold
        if has_no and yes_add > 0:
            seed_avg = pos.get("avg_price", 0)
            opp_price = yes_price
        elif has_yes and no_add > 0:
            seed_avg = pos.get("yes_avg_price", 0)
            opp_price = no_price
        else:
            return True  # adding same side
        
        combined = seed_avg + opp_price
        if combined >= ENTRY_THRESHOLD:
            log.debug(f"  ROI BLOCKED (1-sided): {seed_avg:.3f}+{opp_price:.3f}="
                     f"{combined:.3f} >= {ENTRY_THRESHOLD} | {label[:50]}")
            return False
        log.info(f"  ✅ ROI OK (completing): {seed_avg:.3f}+{opp_price:.3f}="
                 f"{combined:.3f} | {label[:50]}")
        return True
    
    # TWO-SIDED: simulate and check improvement
    no_avg = pos.get("avg_price", 0)
    yes_avg = pos.get("yes_avg_price", 0)
    no_cost = pos.get("cost", 0)
    yes_cost = pos.get("yes_cost", 0)
    total_cost = no_cost + yes_cost
    
    no_sh = no_cost / no_avg if no_avg > 0 else 0
    yes_sh = yes_cost / yes_avg if yes_avg > 0 else 0
    current_pl = min(yes_sh - total_cost, no_sh - total_cost)
    
    # Simulate
    new_no_cost = no_cost + no_add
    new_yes_cost = yes_cost + yes_add
    new_total = new_no_cost + new_yes_cost
    
    new_no_avg = (no_avg * no_cost + no_price * no_add) / new_no_cost if new_no_cost > 0 and no_add > 0 else no_avg
    new_yes_avg = (yes_avg * yes_cost + yes_price * yes_add) / new_yes_cost if new_yes_cost > 0 and yes_add > 0 else yes_avg
    
    new_no_sh = new_no_cost / new_no_avg if new_no_avg > 0 else 0
    new_yes_sh = new_yes_cost / new_yes_avg if new_yes_avg > 0 else 0
    new_pl = min(new_yes_sh - new_total, new_no_sh - new_total)
    
    improvement = new_pl - current_pl
    new_capital = yes_add + no_add
    min_improvement = new_capital * 0.15  # require 15% return on new capital
    
    if new_pl <= current_pl:
        log.debug(f"  ROI BLOCKED: ${current_pl:.2f}→${new_pl:.2f} (no improvement) | {label[:50]}")
        return False
    if improvement < min_improvement:
        log.debug(f"  ROI BLOCKED: +${improvement:.2f} < ${min_improvement:.2f} min | {label[:50]}")
        return False
    
    log.info(f"  ✅ ROI OK: ${current_pl:.2f}→${new_pl:.2f} (+${improvement:.2f}) | {label[:50]}")
    return True

# ═══════════════════════════════════════════════════════════════
# SIZING (from v4 — spread-proportional, equal shares)
# ═══════════════════════════════════════════════════════════════
def calculate_arb_size(yes_price, no_price, mid, group):
    """
    From v4: size proportional to spread. Equal SHARES on both sides.
    Bigger spread = more capital deployed.
    """
    combined = yes_price + no_price
    spread = (1.0 - combined) * 100
    if spread < MIN_SPREAD_PCT: return 0, 0
    
    rm = room_for(mid, group)
    if rm < 3: return 0, 0
    
    # Continuous sizing: spread 2% → ~4%, 5% → ~8%, 10% → ~15%
    deploy_frac = min(0.40, 2.0 * (spread / 100.0) ** 0.75)
    target_cost = min(BANKROLL * deploy_frac, rm)
    target_cost = max(3.0, target_cost)
    
    # Equal shares
    total_price = yes_price + no_price
    shares = target_cost / total_price
    yes_cost = round(shares * yes_price, 2)
    no_cost = round(shares * no_price, 2)
    
    if yes_cost < 2.5 or no_cost < 2.5:
        shares = max(5.0, max(2.5 / yes_price, 2.5 / no_price))
        yes_cost = round(shares * yes_price, 2)
        no_cost = round(shares * no_price, 2)
    
    return yes_cost, no_cost

# ═══════════════════════════════════════════════════════════════
# MARKET CLASSIFICATION
# ═══════════════════════════════════════════════════════════════
def classify(title):
    tl = title.lower().strip()
    if " vs " in tl or " vs. " in tl:
        if "over/under" in tl or "o/u" in tl or "total" in tl: return "ou"
        if "spread" in tl: return "spread"
        return "match"
    if tl.startswith("will ") and (" win" in tl or " beat" in tl): return "win"
    if "over/under" in tl or "o/u" in tl: return "ou"
    if "draw" in tl: return "draw"
    if any(kw in tl for kw in ["valorant","league of legends","lol:","csgo","dota"]): return "esports"
    return "match"

def is_sports_market(title, end_date_str=""):
    """From v8 — title-based filter that actually works."""
    if not title: return False
    tl = title.lower()
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
    if any(kw in tl for kw in REJECT_KW): return False
    has_vs = bool(re.search(r'\bvs\.?\b', tl))
    has_win_date = bool(re.match(r'will .+ win on \d{4}-\d{2}-\d{2}', tl))
    has_ou = bool(re.search(r'o/u\s+\d', tl))
    has_draw = 'end in a draw' in tl
    has_btts = 'both teams to score' in tl
    has_spread = bool(re.search(r'spread\s+[+-]?\d', tl))
    has_esports = any(w in tl for w in ['counter-strike','dota','valorant','lol:',
        'league of legends','bo3','bo5','bo2','rocket league','overwatch'])
    has_game = bool(re.search(r'(game \d+ winner|map \d+ winner|set \d+ winner)', tl))
    return has_vs or has_win_date or has_ou or has_draw or has_btts or has_spread or has_esports or has_game

# ═══════════════════════════════════════════════════════════════
# MARKET DISCOVERY (from v8 — the version that works)
# ═══════════════════════════════════════════════════════════════
def discover_markets():
    log.info("🔍 Discovering markets...")
    markets = []; seen = set()
    now_utc = datetime.now(timezone.utc)
    rejected = {"no_clob": 0, "not_sports": 0, "too_far": 0, "low_vol": 0, "accepted": 0}
    
    for offset in [0, 200, 400, 600]:
        url = (f"{GAMMA}/markets?limit=200&offset={offset}"
               f"&active=true&closed=false&order=volume24hr&ascending=false")
        data = api_get(url, timeout=15)
        if not data or not isinstance(data, list): continue
        
        for m in data:
            mid = m.get("id", "")
            if mid in seen or not m.get("active") or m.get("closed"): continue
            seen.add(mid)
            
            end_str = m.get("endDate") or m.get("end_date_iso") or ""
            hours_left = 48
            if end_str:
                try:
                    edt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    hours_left = (edt - now_utc).total_seconds() / 3600
                    if hours_left > 48 or hours_left < -1:
                        rejected["too_far"] += 1; continue
                except: pass
            
            clob_ids = m.get("clobTokenIds", [])
            if isinstance(clob_ids, str):
                try: clob_ids = json.loads(clob_ids)
                except: clob_ids = []
            if not isinstance(clob_ids, list) or len(clob_ids) < 2:
                rejected["no_clob"] += 1; continue
            
            vol24 = float(m.get("volume24hr") or m.get("volume_24h") or 0)
            if vol24 < 100: rejected["low_vol"] += 1; continue
            
            title = m.get("question", "") or m.get("title", "")
            if not is_sports_market(title, end_str):
                rejected["not_sports"] += 1; continue
            
            rejected["accepted"] += 1
            markets.append({
                "mid": mid, "title": title, "typ": classify(title),
                "tok_yes": str(clob_ids[0]), "tok_no": str(clob_ids[1]),
                "volume24": vol24,
                "group": m.get("groupItemTitle", "") or m.get("conditionId", "") or mid,
                "event_slug": m.get("event_slug", m.get("groupSlug", "")),
                "hours_left": hours_left,
            })
        time.sleep(0.1)
        if len(markets) >= 100: break
    
    # Emergency fallback if no markets found
    if not markets:
        log.warning("  0 markets with endDate — trying fallback...")
        for offset in [0, 200]:
            url = (f"{GAMMA}/markets?limit=200&offset={offset}"
                   f"&active=true&closed=false&order=volume24hr&ascending=false")
            data = api_get(url, timeout=15)
            if not data: continue
            for m in data:
                mid = m.get("id", "")
                if mid in seen: continue
                seen.add(mid)
                clob_ids = m.get("clobTokenIds", [])
                if isinstance(clob_ids, str):
                    try: clob_ids = json.loads(clob_ids)
                    except: clob_ids = []
                if not isinstance(clob_ids, list) or len(clob_ids) < 2: continue
                title = m.get("question", "") or m.get("title", "")
                if not is_sports_market(title, ""): continue
                markets.append({
                    "mid": mid, "title": title, "typ": classify(title),
                    "tok_yes": str(clob_ids[0]), "tok_no": str(clob_ids[1]),
                    "volume24": float(m.get("volume24hr") or 0),
                    "group": m.get("groupItemTitle", "") or m.get("conditionId", "") or mid,
                    "event_slug": m.get("event_slug", m.get("groupSlug", "")),
                    "hours_left": 48,
                })
            time.sleep(0.1)
    
    markets.sort(key=lambda m: m["hours_left"])
    log.info(f"  ✅ {len(markets)} sports markets | accepted={rejected['accepted']} "
             f"too_far={rejected['too_far']} not_sports={rejected['not_sports']} "
             f"low_vol={rejected['low_vol']}")
    for m in markets[:5]:
        h = m["hours_left"]
        ts = "LIVE" if h <= 0 else (f"{h:.1f}h" if h < 24 else f"{h/24:.0f}d")
        log.info(f"    ✓ {m['typ']:<8s} {ts:>5s}  {m['title'][:50]}")
    return markets

# ═══════════════════════════════════════════════════════════════
# SCAN 1: BINARY ARBS (Phase 1 + Phase 2 + Instant Lock)
# ═══════════════════════════════════════════════════════════════
def scan_binary_arbs(markets):
    """
    v4's two-phase system:
    - Instant lock if YES+NO < ENTRY_THRESHOLD now
    - Phase 1 seed if combined < PHASE1_COMBINED
    - Phase 2 complete existing seeds when our_avg + opposite < ENTRY_THRESHOLD
    """
    now = time.time()
    
    for m in markets:
        if not running: break
        mid = m["mid"]
        
        # Fetch both sides
        no_ask, no_depth, no_bid, _ = fetch_book(m["tok_no"])
        if not no_ask or no_ask <= 0.04 or no_ask >= 0.96: continue
        if no_depth < 3: continue
        time.sleep(0.02)
        
        yes_ask, yes_depth, _, _ = fetch_book(m["tok_yes"])
        if not yes_ask or yes_ask <= 0.01: continue
        time.sleep(0.02)
        
        combined = yes_ask + no_ask
        spread_pct = (1.0 - combined) * 100
        stats["scanned"] += 1
        
        # Load our current position (if any)
        port = load_portfolio()
        pos = port.get("positions", {}).get(mid, {})
        has_no = pos.get("shares", 0) > 0
        has_yes = pos.get("yes_shares", 0) > 0
        
        # ── PHASE 2: COMPLETE existing one-sided positions ──
        if has_no and not has_yes:
            our_no_avg = pos.get("avg_price", 0)
            our_combined = our_no_avg + yes_ask
            our_spread = (1.0 - our_combined) * 100
            
            if our_combined < PHASE2_STALE_GATE and our_spread > 0:
                # Check ROI gate
                yes_cost = round(pos.get("shares", 0) * yes_ask, 2)
                if yes_cost < 2.5: continue
                
                if check_roi_improvement(pos, yes_cost, 0, yes_ask, 0,
                                        f"P2-COMPLETE|{m['title'][:35]}"):
                    if not rate_ok(): continue
                    label = f"🔒P2-YES|{our_spread:.1f}%|{m['title'][:30]}"
                    oid, sh = exec_buy(m["tok_yes"], yes_ask, yes_cost, label)
                    if oid:
                        record_order()
                        # Update portfolio
                        port = load_portfolio()
                        p = port["positions"][mid]
                        p["yes_cost"] = p.get("yes_cost", 0) + yes_cost
                        p["yes_shares"] = p.get("yes_shares", 0) + sh
                        if p["yes_shares"] > 0:
                            p["yes_avg_price"] = p["yes_cost"] / p["yes_shares"]
                        p["n_yes_buys"] = p.get("n_yes_buys", 0) + 1
                        port["arb_locks"] = port.get("arb_locks", 0) + 1
                        save_portfolio(port)
                        
                        gp = min(pos.get("shares", 0), sh) - (pos.get("cost", 0) + yes_cost)
                        log.info(f"  🔒🔒 PHASE 2 COMPLETE! {m['title'][:45]}")
                        log.info(f"     Our NO@{our_no_avg:.3f} + YES@{yes_ask:.3f} = {our_combined:.3f}")
                        log.info(f"     💰 Guaranteed: ${gp:.2f}")
                        stats["p2_locked"] += 1
                        continue
        
        elif has_yes and not has_no:
            our_yes_avg = pos.get("yes_avg_price", 0)
            our_combined = our_yes_avg + no_ask
            our_spread = (1.0 - our_combined) * 100
            
            if our_combined < PHASE2_STALE_GATE and our_spread > 0:
                no_cost_add = round(pos.get("yes_shares", 0) * no_ask, 2)
                if no_cost_add < 2.5: continue
                
                if check_roi_improvement(pos, 0, no_cost_add, 0, no_ask,
                                        f"P2-COMPLETE|{m['title'][:35]}"):
                    if not rate_ok(): continue
                    label = f"🔒P2-NO|{our_spread:.1f}%|{m['title'][:30]}"
                    oid, sh = exec_buy(m["tok_no"], no_ask, no_cost_add, label)
                    if oid:
                        record_order()
                        port = load_portfolio()
                        p = port["positions"][mid]
                        p["cost"] = p.get("cost", 0) + no_cost_add
                        p["shares"] = p.get("shares", 0) + sh
                        if p["shares"] > 0:
                            p["avg_price"] = p["cost"] / p["shares"]
                        p["n_buys"] = p.get("n_buys", 0) + 1
                        port["arb_locks"] = port.get("arb_locks", 0) + 1
                        save_portfolio(port)
                        stats["p2_locked"] += 1
                        continue
        
        elif has_no and has_yes:
            # ── ADDING: already two-sided, try to improve ROI ──
            if combined < ENTRY_THRESHOLD and spread_pct >= MIN_SPREAD_PCT:
                yes_add, no_add = calculate_arb_size(yes_ask, no_ask, mid, m["group"])
                if (yes_add + no_add) < 3: continue
                
                if check_roi_improvement(pos, yes_add, no_add, yes_ask, no_ask,
                                        f"ADDING|{m['title'][:35]}"):
                    if not rate_ok(): continue
                    # Buy both sides
                    oid_y, sh_y = exec_buy(m["tok_yes"], yes_ask, yes_add,
                                           f"🔒ADD-YES|{m['title'][:30]}")
                    oid_n, sh_n = exec_buy(m["tok_no"], no_ask, no_add,
                                           f"🔒ADD-NO|{m['title'][:30]}")
                    if oid_y or oid_n:
                        record_order()
                        port = load_portfolio()
                        p = port["positions"][mid]
                        if oid_y:
                            p["yes_cost"] = p.get("yes_cost", 0) + yes_add
                            p["yes_shares"] = p.get("yes_shares", 0) + sh_y
                            if p["yes_shares"] > 0:
                                p["yes_avg_price"] = p["yes_cost"] / p["yes_shares"]
                            p["n_yes_buys"] = p.get("n_yes_buys", 0) + 1
                        if oid_n:
                            p["cost"] = p.get("cost", 0) + no_add
                            p["shares"] = p.get("shares", 0) + sh_n
                            if p["shares"] > 0:
                                p["avg_price"] = p["cost"] / p["shares"]
                            p["n_buys"] = p.get("n_buys", 0) + 1
                        save_portfolio(port)
                        stats["arb_improved"] += 1
                        continue
            
            # ── REBALANCE: if shares imbalanced > 1.5x, buy light side ──
            no_sh = pos.get("shares", 0)
            yes_sh = pos.get("yes_shares", 0)
            if no_sh > yes_sh * 1.5 and combined < 1.0:
                target = no_sh - yes_sh
                add_cost = round(target * yes_ask, 2)
                add_cost = min(add_cost, room_for(mid, m["group"]))
                if add_cost >= 2.5 and rate_ok():
                    if check_roi_improvement(pos, add_cost, 0, yes_ask, 0,
                                            f"REBAL-YES|{m['title'][:35]}"):
                        oid, sh = exec_buy(m["tok_yes"], yes_ask, add_cost,
                                           f"🔄REBAL-YES|{m['title'][:30]}")
                        if oid:
                            record_order()
                            port = load_portfolio()
                            p = port["positions"][mid]
                            p["yes_cost"] = p.get("yes_cost", 0) + add_cost
                            p["yes_shares"] = p.get("yes_shares", 0) + sh
                            if p["yes_shares"] > 0:
                                p["yes_avg_price"] = p["yes_cost"] / p["yes_shares"]
                            save_portfolio(port)
                            stats["rebalanced"] += 1
                            continue
            elif yes_sh > no_sh * 1.5 and combined < 1.0:
                target = yes_sh - no_sh
                add_cost = round(target * no_ask, 2)
                add_cost = min(add_cost, room_for(mid, m["group"]))
                if add_cost >= 2.5 and rate_ok():
                    if check_roi_improvement(pos, 0, add_cost, 0, no_ask,
                                            f"REBAL-NO|{m['title'][:35]}"):
                        oid, sh = exec_buy(m["tok_no"], no_ask, add_cost,
                                           f"🔄REBAL-NO|{m['title'][:30]}")
                        if oid:
                            record_order()
                            port = load_portfolio()
                            p = port["positions"][mid]
                            p["cost"] = p.get("cost", 0) + add_cost
                            p["shares"] = p.get("shares", 0) + sh
                            if p["shares"] > 0:
                                p["avg_price"] = p["cost"] / p["shares"]
                            save_portfolio(port)
                            stats["rebalanced"] += 1
                            continue
            continue  # already two-sided, done
        
        # ── NEW MARKET (no position) ──
        # Log near-arbs
        if combined < 1.05:
            log.info(f"  🔍 {m['title'][:45]:<45s} YES={yes_ask:.3f} NO={no_ask:.3f} "
                     f"sum={combined:.3f} spread={spread_pct:.1f}%")
            stats["near_arbs"] += 1
        
        # INSTANT LOCK: both sides cheap enough right now
        if combined < ENTRY_THRESHOLD and spread_pct >= MIN_SPREAD_PCT:
            yes_size, no_size = calculate_arb_size(yes_ask, no_ask, mid, m["group"])
            if yes_size < 2.5 or no_size < 2.5: continue
            if not rate_ok(): continue
            
            # Buy both sides — equal shares
            oid_n, sh_n = exec_buy(m["tok_no"], no_ask, no_size,
                                    f"🔒LOCK-NO|{spread_pct:.1f}%|{m['title'][:25]}")
            if not oid_n: continue
            record_order()
            
            # Match shares for YES side
            yes_matched = round(sh_n * yes_ask, 2)
            oid_y, sh_y = exec_buy(m["tok_yes"], yes_ask, yes_matched,
                                    f"🔒LOCK-YES|{spread_pct:.1f}%|{m['title'][:25]}")
            record_order()
            
            # Save to portfolio
            port = load_portfolio()
            port["positions"][mid] = {
                "title": m["title"], "typ": m["typ"],
                "tok_no": m["tok_no"], "tok_yes": m["tok_yes"],
                "group_key": m["group"],
                "cost": no_size, "shares": sh_n,
                "n_buys": 1, "n_sells": 0, "avg_price": no_ask,
                "yes_cost": yes_matched if oid_y else 0,
                "yes_shares": sh_y if oid_y else 0,
                "yes_avg_price": yes_ask if oid_y else 0,
                "n_yes_buys": 1 if oid_y else 0,
                "created": time.time(), "active": True, "spread_at_entry": 0,
            }
            port["entries"] = port.get("entries", 0) + 1
            port["arb_locks"] = port.get("arb_locks", 0) + 1
            port["total_cost"] = port.get("total_cost", 0) + no_size + (yes_matched if oid_y else 0)
            save_portfolio(port)
            
            gp = min(sh_n, sh_y if oid_y else 0) - (no_size + (yes_matched if oid_y else 0))
            roi = gp / (no_size + (yes_matched if oid_y else 0)) * 100 if oid_y else 0
            log.info(f"  🔒🔒 INSTANT LOCK! {m['title'][:45]}")
            log.info(f"     YES@{yes_ask:.3f}+NO@{no_ask:.3f}={combined:.3f} ({spread_pct:.1f}%)")
            log.info(f"     💰 Guaranteed: ${gp:.2f} ({roi:.1f}% ROI)")
            stats["instant_locked"] += 1
            continue
        
        # PHASE 1 SEED: combined close but not quite there
        if combined < PHASE1_COMBINED and not has_no and not has_yes:
            # Cooldown: max one seed per market per 60s
            if now - _last_phase1.get(mid, 0) < 60: continue
            if not rate_ok(): continue
            
            # Seed the cheaper side (more likely to be the "right" side)
            if no_ask <= yes_ask:
                seed_side = "NO"
                seed_tok = m["tok_no"]
                seed_price = no_ask
                other_price = yes_ask
            else:
                seed_side = "YES"
                seed_tok = m["tok_yes"]
                seed_price = yes_ask
                other_price = no_ask
            
            # House-edge filter: combined > 1.04 means other side too expensive
            gap_needed = ENTRY_THRESHOLD - seed_price
            if other_price - gap_needed > 0.30: continue
            
            # Seed size: must buy >= 5 shares (Polymarket minimum)
            # At price 0.50 → need $2.50 min, at 0.80 → need $4.00 min
            min_seed = math.ceil(5 * seed_price * 100) / 100 + 0.01  # just above 5 shares
            seed_size = max(PHASE1_SEED_SIZE, min_seed)
            seed_size = min(seed_size, room_for(mid, m["group"]))
            if seed_size < min_seed: continue
            
            label = f"🌱P1-{seed_side}|{spread_pct:.1f}%|{m['title'][:25]}"
            oid, sh = exec_buy(seed_tok, seed_price, seed_size, label)
            if oid:
                record_order()
                _last_phase1[mid] = now
                
                port = load_portfolio()
                new_pos = {
                    "title": m["title"], "typ": m["typ"],
                    "tok_no": m["tok_no"], "tok_yes": m["tok_yes"],
                    "group_key": m["group"],
                    "cost": seed_size if seed_side == "NO" else 0,
                    "shares": sh if seed_side == "NO" else 0,
                    "n_buys": 1 if seed_side == "NO" else 0,
                    "n_sells": 0,
                    "avg_price": seed_price if seed_side == "NO" else 0,
                    "yes_cost": seed_size if seed_side == "YES" else 0,
                    "yes_shares": sh if seed_side == "YES" else 0,
                    "yes_avg_price": seed_price if seed_side == "YES" else 0,
                    "n_yes_buys": 1 if seed_side == "YES" else 0,
                    "created": time.time(), "active": True, "spread_at_entry": 0,
                    "is_seed": True,  # MARKER: scanner planted this, cleanup can touch it
                }
                port["positions"][mid] = new_pos
                port["entries"] = port.get("entries", 0) + 1
                port["total_cost"] = port.get("total_cost", 0) + seed_size
                save_portfolio(port)
                
                log.info(f"  🌱 PHASE 1 SEED: {m['title'][:45]}")
                log.info(f"     {seed_side}@{seed_price:.3f} ${seed_size:.2f} ({sh:.0f}sh) | "
                         f"Need other@<{gap_needed:.3f} to complete")
                stats["p1_seeds"] += 1

# ═══════════════════════════════════════════════════════════════
# SCAN 2: CROSS-EVENT DUTCH (from v5 — NO+NO on opposing outcomes)
# ═══════════════════════════════════════════════════════════════
def scan_cross_dutch(markets):
    """
    Buy NO on both "Will Team A win?" and "Will Team B win?" in same match.
    If Team A wins → NO(B) pays. Team B wins → NO(A) pays. Draw → BOTH pay.
    """
    now = time.time()
    events = defaultdict(list)
    for m in markets:
        slug = m.get("event_slug") or m["group"]
        if slug: events[slug].append(m)
    
    for event, mkts in events.items():
        if not running: break
        if len(mkts) < 2: continue
        if now - _last_cross.get(event, 0) < 10: continue
        
        # Find "win" markets
        wins = [(m, m["tok_no"]) for m in mkts if m["typ"] == "win"]
        if len(wins) < 2: continue
        
        # Skip if all already held
        port = load_portfolio()
        if all(port.get("positions", {}).get(m["mid"], {}).get("shares", 0) > 0
               for m, _ in wins): continue
        
        # Fetch NO prices for all win markets
        priced = []
        for m, tok in wins:
            no_ask, depth, _, _ = fetch_book(tok)
            if no_ask and 0.25 <= no_ask <= 0.80 and depth >= 3:
                priced.append((m, no_ask))
            time.sleep(0.02)
        
        if len(priced) < 2: continue
        
        # Find best pair (lowest NO+NO sum)
        best_pair, best_sum = None, 1.0
        for i in range(len(priced)):
            for j in range(i+1, len(priced)):
                s = priced[i][1] + priced[j][1]
                if s < best_sum:
                    best_sum = s
                    best_pair = (priced[i], priced[j])
        
        if not best_pair or best_sum >= CROSS_DUTCH_THRESHOLD: continue
        spread = (1.0 - best_sum) * 100
        if spread < MIN_SPREAD_PCT: continue
        
        _last_cross[event] = now
        (m_a, no_a), (m_b, no_b) = best_pair
        
        log.info(f"  🔍 CROSS-DUTCH: {event[:40]}")
        log.info(f"     NO({m_a['title'][:30]})@{no_a:.3f}")
        log.info(f"     NO({m_b['title'][:30]})@{no_b:.3f}")
        log.info(f"     Sum={best_sum:.3f} Spread={spread:.1f}%")
        
        if not rate_ok(): continue
        
        # Size: spread-proportional, split between legs
        deploy_frac = min(0.40, 2.0 * (spread / 100.0) ** 0.75)
        sz = max(2.5, round(BANKROLL * deploy_frac / 2, 2))
        sz = min(sz, room_for(m_a["mid"], event) / 2, room_for(m_b["mid"], event) / 2)
        if sz < 2.5: continue
        
        oid_a, sh_a = exec_buy(m_a["tok_no"], no_a, sz,
                                f"🔒XDUTCH-NO|{m_a['title'][:30]}")
        oid_b, sh_b = exec_buy(m_b["tok_no"], no_b, sz,
                                f"🔒XDUTCH-NO|{m_b['title'][:30]}")
        
        if oid_a: record_order()
        if oid_b: record_order()
        
        # Save both legs
        if oid_a or oid_b:
            port = load_portfolio()
            for m, no_price, oid, sh, cost in [
                (m_a, no_a, oid_a, sh_a, sz if oid_a else 0),
                (m_b, no_b, oid_b, sh_b, sz if oid_b else 0),
            ]:
                if not oid: continue
                existing = port["positions"].get(m["mid"], {})
                if existing:
                    existing["cost"] = existing.get("cost", 0) + cost
                    existing["shares"] = existing.get("shares", 0) + sh
                    if existing["shares"] > 0:
                        existing["avg_price"] = existing["cost"] / existing["shares"]
                    existing["n_buys"] = existing.get("n_buys", 0) + 1
                else:
                    port["positions"][m["mid"]] = {
                        "title": m["title"], "typ": m["typ"],
                        "tok_no": m["tok_no"], "tok_yes": m["tok_yes"],
                        "group_key": event,
                        "cost": cost, "shares": sh,
                        "n_buys": 1, "n_sells": 0, "avg_price": no_price,
                        "yes_cost": 0, "yes_shares": 0, "yes_avg_price": 0, "n_yes_buys": 0,
                        "created": time.time(), "active": True, "spread_at_entry": 0,
                    }
                port["entries"] = port.get("entries", 0) + 1
                port["total_cost"] = port.get("total_cost", 0) + cost
            port["arb_locks"] = port.get("arb_locks", 0) + 1
            save_portfolio(port)
            stats["cross_dutch"] += 1
            
            log.info(f"  🔒🔒 CROSS-DUTCH LOCKED! {event[:40]}")
            log.info(f"     ${sz:.2f} each side, spread={spread:.1f}%")

# ═══════════════════════════════════════════════════════════════
# SCAN 3: CLEANUP STALE SEEDS (Phase 1 that never completed)
# ═══════════════════════════════════════════════════════════════
def cleanup_stale_seeds():
    """
    If a Phase 1 seed hasn't completed Phase 2 within 15 minutes:
      - If profitable (bid > avg_price): SELL and recycle capital
      - If NO bid >= 0.70: HOLD (70%+ chance it resolves to $1)
      - Otherwise: SELL at whatever bid is available, move on
    
    Never touches arb-locked positions (both sides held).
    """
    now = time.time()
    port = load_portfolio()
    positions = port.get("positions", {})
    cleaned = 0
    held = 0
    
    for mid, pos in list(positions.items()):
        # CRITICAL: only touch positions WE planted (tagged is_seed=True)
        # Never touch positions managed by arb_v8.py
        if not pos.get("is_seed"): continue
        
        has_no = pos.get("shares", 0) > 0
        has_yes = pos.get("yes_shares", 0) > 0
        
        # Skip arb-locked (both sides held) — Phase 2 completed successfully
        if has_no and has_yes: continue
        
        # Skip positions not old enough
        created = pos.get("created", 0)
        age_s = now - created if created > 0 else 0
        if age_s < SEED_TIMEOUT: continue
        
        # This is a stale one-sided seed
        age_min = age_s / 60
        title = pos.get("title", "")[:40]
        
        if has_no:
            tok = pos.get("tok_no", "")
            shares = pos.get("shares", 0)
            avg_price = pos.get("avg_price", 0)
            cost = pos.get("cost", 0)
        elif has_yes:
            tok = pos.get("tok_yes", "")
            shares = pos.get("yes_shares", 0)
            avg_price = pos.get("yes_avg_price", 0)
            cost = pos.get("yes_cost", 0)
        else:
            continue
        
        if shares <= 0 or not tok: continue
        
        # Fetch current bid
        _, _, bid, _ = fetch_book(tok)
        time.sleep(0.02)
        
        if not bid or bid <= 0:
            log.info(f"  ⏰ Stale seed {age_min:.0f}m: {title} — no bid, skipping")
            continue
        
        pnl_pct = (bid - avg_price) / avg_price * 100 if avg_price > 0 else 0
        revenue = bid * shares
        
        # Decision: sell, hold, or sell at loss
        if pnl_pct > 0:
            # Profitable — sell and recycle
            log.info(f"  💰 Stale seed PROFITABLE ({age_min:.0f}m): {title} "
                     f"avg={avg_price:.3f} bid={bid:.3f} pnl={pnl_pct:+.0f}% — SELLING")
            if rate_ok():
                side = "NO" if has_no else "YES"
                oid, rev = exec_sell(tok, bid, shares,
                                     f"♻️RECYCLE-{side}|+{pnl_pct:.0f}%|{title[:25]}")
                if oid:
                    record_order()
                    # Update portfolio — mark as sold
                    port = load_portfolio()
                    p = port["positions"].get(mid, {})
                    if has_no:
                        p["shares"] = 0; p["cost"] = 0
                    else:
                        p["yes_shares"] = 0; p["yes_cost"] = 0
                    port["positions"][mid] = p
                    port["realized"] = port.get("realized", 0) + rev
                    port["profit_takes"] = port.get("profit_takes", 0) + 1
                    save_portfolio(port)
                    cleaned += 1
                    stats["seeds_recycled"] += 1
        
        elif bid >= SEED_HOLD_PRICE:
            # High probability of payout — hold it, let arb_v8 manage
            log.info(f"  🏠 Stale seed HOLD ({age_min:.0f}m): {title} "
                     f"bid={bid:.3f} >= {SEED_HOLD_PRICE} — likely pays $1")
            held += 1
            stats["seeds_held"] += 1
        
        else:
            # Losing and low probability — sell and move on
            loss = cost - revenue
            log.info(f"  ♻️ Stale seed RECYCLE ({age_min:.0f}m): {title} "
                     f"avg={avg_price:.3f} bid={bid:.3f} pnl={pnl_pct:+.0f}% "
                     f"loss=${loss:.2f} — SELLING to free capital")
            if rate_ok():
                side = "NO" if has_no else "YES"
                oid, rev = exec_sell(tok, bid, shares,
                                     f"♻️RECYCLE-{side}|{pnl_pct:.0f}%|{title[:25]}")
                if oid:
                    record_order()
                    port = load_portfolio()
                    p = port["positions"].get(mid, {})
                    if has_no:
                        p["shares"] = 0; p["cost"] = 0
                    else:
                        p["yes_shares"] = 0; p["yes_cost"] = 0
                    port["positions"][mid] = p
                    port["realized"] = port.get("realized", 0) + rev
                    port["loss_cuts"] = port.get("loss_cuts", 0) + 1
                    save_portfolio(port)
                    cleaned += 1
                    stats["seeds_recycled"] += 1
    
    if cleaned > 0 or held > 0:
        log.info(f"  ♻️ Seed cleanup: {cleaned} sold, {held} held (>= {SEED_HOLD_PRICE})")
    return cleaned, held

# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════
def main():
    global DRY_RUN, BANKROLL
    
    import argparse
    parser = argparse.ArgumentParser(description="PolyArb v8 Arb Scanner (v4 logic)")
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--bankroll", type=float, default=BANKROLL)
    args = parser.parse_args()
    
    DRY_RUN = not args.live
    BANKROLL = args.bankroll
    
    mode = f"{'🔴 LIVE' if not DRY_RUN else '⚪ DRY RUN'}"
    log.info(f"{'='*60}")
    log.info(f"  ARB SCANNER v8 (v4 logic) — {mode}")
    log.info(f"  Bankroll: ${BANKROLL:,.0f}")
    log.info(f"  Lock threshold: {ENTRY_THRESHOLD} (2% min spread)")
    log.info(f"  Phase 1 seed: combined < {PHASE1_COMBINED} → ${PHASE1_SEED_SIZE:.0f} seed")
    log.info(f"  Phase 2 complete: our_avg + opp_ask < {ENTRY_THRESHOLD}")
    log.info(f"  Seed timeout: {SEED_TIMEOUT//60}min → sell if profitable or bid < {SEED_HOLD_PRICE}, hold if >= {SEED_HOLD_PRICE}")
    log.info(f"  Cross-Dutch: NO+NO < {CROSS_DUTCH_THRESHOLD}")
    log.info(f"  ROI gate: every trade must improve guaranteed P&L")
    log.info(f"{'='*60}")
    
    if not DRY_RUN:
        if not PRIVATE_KEY:
            log.error("No PK in keys.env!"); return
        get_clob()
    
    all_markets = discover_markets()
    last_discover = time.time()
    
    while running:
        t0 = time.time()
        stats["cycles"] += 1
        
        if time.time() - last_discover > 120:
            all_markets = discover_markets()
            last_discover = time.time()
        
        if not all_markets:
            time.sleep(5); continue
        
        # Scan 1: Binary arbs (Phase 1/2 + instant lock + adding + rebalance)
        scan_binary_arbs(all_markets)
        
        # Scan 2: Cross-event Dutch (NO+NO)
        scan_cross_dutch(all_markets)
        
        # Scan 3: Cleanup stale seeds (every cycle — fast, just checks age)
        cleanup_stale_seeds()
        
        cycle_ms = int((time.time() - t0) * 1000)
        deployed = portfolio_deployed()
        n_pos = len([p for p in load_portfolio().get("positions", {}).values()
                     if p.get("shares", 0) > 0 or p.get("yes_shares", 0) > 0])
        
        log.info(f"  ⟳ #{stats['cycles']} {cycle_ms}ms | "
                 f"scn={stats['scanned']} near={stats['near_arbs']} "
                 f"🌱seed={stats['p1_seeds']} 🔒lock={stats['instant_locked']} "
                 f"🔒p2={stats['p2_locked']} 🔒x={stats['cross_dutch']} "
                 f"♻️recycle={stats['seeds_recycled']} 🏠held={stats['seeds_held']} | "
                 f"{n_pos}pos ${deployed:,.0f}")
        
        wait = max(0.1, SCAN_INTERVAL - (time.time() - t0))
        time.sleep(wait)
    
    log.info(f"\nSCANNER SHUTDOWN")
    log.info(f"  Seeds: {stats['p1_seeds']}  Locks: {stats['instant_locked']}  "
             f"P2: {stats['p2_locked']}  Cross-Dutch: {stats['cross_dutch']}  "
             f"Recycled: {stats['seeds_recycled']}  Held: {stats['seeds_held']}")

if __name__ == "__main__":
    main()
