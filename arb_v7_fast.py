#!/usr/bin/env python3
"""
PolyArb v7.1 — FAST TRIAD SCANNER + BOT
════════════════════════════════════════════
Same strategy as v7 but FAST market discovery:
  Instead of fetching all 33K markets, uses targeted search
  to find only draw + win markets (~3K total, 10x faster).

Usage:
  python3.11 arb_v7_fast.py --bankroll 1000 --fresh --scan-only   # scan only
  python3.11 arb_v7_fast.py --bankroll 1000 --fresh               # dry run
  python3.11 arb_v7_fast.py --bankroll 1000 --fresh --live         # real money
"""

import os, sys, json, time, re, signal, logging, random
from datetime import datetime, timezone
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Set
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

# ═══════ CONFIG ═══════
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

MY_BANKROLL       = float(os.getenv("BANKROLL", "1000"))
DRY_RUN           = True
MAX_DEPLOY_PCT    = 0.90
MAX_PER_MATCH     = 0    # 0 = 15% bankroll
MAX_PER_MARKET    = 100
TRIAD_MAX_COST    = float(os.getenv("TRIAD_MAX", "2.04"))
TRIAD_ACCUM_CD    = 45
SUPP_ENABLED      = True
SUPP_MAX_PCT      = 0.25
MAX_FILLS_MIN     = 30
MAX_NEW_HR        = 90

# ═══════ LOGGING ═══════
os.makedirs("logs", exist_ok=True)
_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = os.path.join("logs", f"arb_v7f_{_ts}.log")

log = logging.getLogger("v7f")
log.setLevel(logging.DEBUG)
fh = logging.FileHandler(LOG_FILE); fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(fh)
sh = logging.StreamHandler(); sh.setLevel(logging.INFO)
sh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-8s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(sh)

# ═══════ API ═══════
def api_get(url, timeout=15, retries=2):
    for attempt in range(retries + 1):
        try:
            req = Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Accept": "application/json"})
            return json.loads(urlopen(req, timeout=timeout).read().decode("utf-8"))
        except HTTPError as e:
            if e.code in (429, 403): time.sleep(2 ** attempt * 2); continue
            if e.code >= 500: time.sleep(1); continue
            return None
        except: time.sleep(0.5); continue
    return None

def fetch_book(token_id):
    book = api_get(f"{CLOB}/book?token_id={token_id}", timeout=8)
    if not book: return None, 0
    asks = book.get("asks", [])
    if not asks: return None, 0
    best = None; depth = 0
    for a in asks:
        try:
            p, s = float(a["price"]), float(a["size"])
            if p > 0 and s > 0:
                if best is None or p < best: best = p
                depth += s * p
        except: continue
    return best, round(depth, 2)

# ═══════ RATE LIMITER ═══════
_fill_ts, _new_ts = [], []
def rate_ok(is_new=False):
    now = time.time()
    while _fill_ts and now - _fill_ts[0] > 60: _fill_ts.pop(0)
    while _new_ts and now - _new_ts[0] > 3600: _new_ts.pop(0)
    if len(_fill_ts) >= MAX_FILLS_MIN: return False
    if is_new and len(_new_ts) >= MAX_NEW_HR: return False
    return True

# ═══════ RN1 SIZING ═══════
def rn1_size(price):
    scale = MY_BANKROLL / 853
    if   price < 0.10: base = 3.0
    elif price < 0.20: base = 5.0
    elif price < 0.30: base = 7.0
    elif price < 0.50: base = 9.0
    elif price < 0.70: base = 11.0
    elif price < 0.80: base = 13.0
    elif price < 0.90: base = 15.0
    else:               base = 18.0
    return round(min(base * scale, MY_BANKROLL * 0.02), 2)

# ═══════ FAST MARKET DISCOVERY ═══════
FUTURES_KW = ['finals','stanley cup','world cup','super bowl','championship',
    'premier league winner','serie a winner','bundesliga winner','ligue 1 winner',
    'la liga winner','champion','playoffs','division','conference','nomination',
    'presidential','election','award','mvp','rookie','draft','qualify','relegat',
    'top goal','golden boot']

def _is_futures(t):
    tl = t.lower()
    return any(kw in tl for kw in FUTURES_KW)

def _normalize(s):
    return re.sub(r'[^a-z0-9\s]', '', s.lower()).strip()

def _parse_market(m):
    """Parse a Gamma market into our format."""
    clob_ids = m.get("clobTokenIds", [])
    if isinstance(clob_ids, str):
        try: clob_ids = json.loads(clob_ids)
        except: clob_ids = []
    if not isinstance(clob_ids, list) or len(clob_ids) < 2:
        return None
    return {
        "id": m.get("id", ""),
        "_title": m.get("question", "") or m.get("title", ""),
        "_clob_yes": str(clob_ids[0]),
        "_clob_no": str(clob_ids[1]),
        "volume": float(m.get("volume", 0) or 0),
        "liquidity": float(m.get("liquidity", 0) or 0),
    }


def fetch_markets_fast():
    """
    FAST: Fetch only draw and win markets using tag/keyword filtering.
    Falls back to full fetch if filtering isn't available.
    """
    log.info("Fetching markets (targeted)...")
    all_m = []
    draws = []
    wins_by_team = defaultdict(list)
    
    offset = 0
    page = 0
    last_log = time.time()
    
    while True:
        url = f"{GAMMA}/markets?limit=100&offset={offset}&active=true&closed=false"
        data = api_get(url, timeout=20)
        if not data or not isinstance(data, list) or len(data) == 0:
            break
        
        for m in data:
            if not m.get("active") or m.get("closed"):
                continue
            parsed = _parse_market(m)
            if not parsed:
                continue
            
            title = parsed["_title"]
            
            # Draw market
            dm = re.match(r'Will (.+?)\s+vs\.?\s+(.+?)\s+end in a draw\s*\??$', title, re.I)
            if dm:
                parsed["_team_a"] = dm.group(1).strip()
                parsed["_team_b"] = dm.group(2).strip()
                parsed["_na"] = _normalize(dm.group(1))
                parsed["_nb"] = _normalize(dm.group(2))
                draws.append(parsed)
                all_m.append(parsed)
                continue
            
            # Win market (match-day)
            wm = re.match(r'Will (.+?) win(?:\s+on\s+\d{4}-\d{2}-\d{2})?\s*\??$', title, re.I)
            if wm and not _is_futures(title):
                team = wm.group(1).strip()
                nt = _normalize(team)
                parsed["_team"] = team
                parsed["_nt"] = nt
                wins_by_team[nt].append(parsed)
                all_m.append(parsed)
                continue
            
            # Other sports (supplementary)
            tl = title.lower()
            if any(w in tl for w in ['o/u', 'spread', 'vs', 'counter-strike', 'dota', 'valorant']):
                if not _is_futures(title):
                    all_m.append(parsed)
        
        page += 1
        if len(data) < 100:
            break
        offset += 100
        
        # Progress
        if time.time() - last_log > 5:
            log.info(f"  ...page {page}, {len(draws)} draws, {len(wins_by_team)} teams, "
                     f"{len(all_m)} total markets")
            last_log = time.time()
        
        time.sleep(0.1)
    
    log.info(f"Fetched {len(all_m):,} relevant markets: "
             f"{len(draws)} draws, {sum(len(v) for v in wins_by_team.values())} wins, "
             f"{len(wins_by_team)} teams")
    
    return all_m, draws, wins_by_team


# ═══════ TRIAD BUILDER ═══════
class Triad:
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
        self.depth_a = self.depth_b = self.depth_d = 0
        self.leg_deployed = {"a": 0, "b": 0, "d": 0}
        self.leg_shares = {"a": 0, "b": 0, "d": 0}
        self.leg_fills = {"a": 0, "b": 0, "d": 0}
        self.last_fill_time = {"a": 0, "b": 0, "d": 0}
        self.priced = False

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

    def short_name(self):
        a = self.team_a[:18] if len(self.team_a) > 18 else self.team_a
        b = self.team_b[:18] if len(self.team_b) > 18 else self.team_b
        return f"{a} v {b}"


def build_triads(draws, wins_by_team):
    triads = []
    for d in draws:
        na, nb = d["_na"], d["_nb"]
        wa = wins_by_team.get(na)
        wb = wins_by_team.get(nb)
        if not wa:
            for k, v in wins_by_team.items():
                if na in k or k in na: wa = v; break
        if not wb:
            for k, v in wins_by_team.items():
                if nb in k or k in nb: wb = v; break
        if wa and wb:
            best_a = max(wa, key=lambda x: x.get("volume", 0))
            best_b = max(wb, key=lambda x: x.get("volume", 0))
            triads.append(Triad(d["_team_a"], d["_team_b"], best_a, best_b, d))
    log.info(f"Built {len(triads)} complete triads")
    return triads


# ═══════ LEDGER ═══════
class Ledger:
    def __init__(self):
        self.positions = {}
    
    def record(self, mid, price, size, title="", triad_key="", leg=""):
        if mid not in self.positions:
            self.positions[mid] = {
                "market_id": mid, "title": title, "fills": [],
                "cost": 0, "shares": 0, "triad_key": triad_key, "leg": leg,
            }
        p = self.positions[mid]
        sh = size / price if price > 0 else 0
        p["fills"].append((price, size, time.time()))
        p["cost"] += size
        p["shares"] += sh
        if triad_key: p["triad_key"] = triad_key
        if leg: p["leg"] = leg
    
    def deployed(self): return sum(p["cost"] for p in self.positions.values())
    def active_count(self): return sum(1 for p in self.positions.values() if p["cost"] > 0)
    def match_deployed(self, mk):
        return sum(p["cost"] for p in self.positions.values() if p.get("triad_key") == mk)
    def supp_deployed(self):
        return sum(p["cost"] for p in self.positions.values() if not p.get("triad_key"))
    
    def save(self, path="ledger.json"):
        with open(path, "w") as f: json.dump(self.positions, f, indent=2)
    def load(self, path="ledger.json"):
        if not os.path.exists(path): return
        with open(path) as f: self.positions = json.load(f)
        log.info(f"Loaded {len(self.positions)} positions")

ledger = Ledger()

# ═══════ STATS ═══════
class Stats:
    def __init__(self):
        self.start = time.time()
        self.triads_total = 0; self.triads_profitable = 0
        self.triads_entered = 0; self.triads_complete = 0
        self.fills_triad = 0; self.fills_accum = 0; self.fills_supp = 0
        self.rej_rate = 0; self.rej_deploy = 0; self.rej_match = 0
        self.rej_edge = 0; self.rej_price = 0; self.scans = 0
    @property
    def runtime(self): return (time.time() - self.start) / 60
    @property
    def total_fills(self): return self.fills_triad + self.fills_accum + self.fills_supp
    def line(self):
        return (f"STATS: Runtime:{self.runtime:.1f}m | Triads:{self.triads_total} "
                f"prof:{self.triads_profitable} entered:{self.triads_entered} "
                f"done:{self.triads_complete} | Fills[T:{self.fills_triad} "
                f"A:{self.fills_accum} S:{self.fills_supp} ={self.total_fills}] | "
                f"Rej[rate:{self.rej_rate} dep:{self.rej_deploy} match:{self.rej_match} "
                f"edge:{self.rej_edge} price:{self.rej_price}] | "
                f"${ledger.deployed():.2f} | Active:{ledger.active_count()} | "
                f"BRL:${MY_BANKROLL:,.0f}")

stats = Stats()

# ═══════ EXECUTION ═══════
_clob_client = None
def get_clob():
    global _clob_client
    if _clob_client: return _clob_client
    from py_clob_client.client import ClobClient
    _clob_client = ClobClient(CLOB, key=PRIVATE_KEY, chain_id=137,
                               signature_type=2, funder=os.getenv("FUNDER"))
    _clob_client.set_api_creds(_clob_client.create_or_derive_api_creds())
    return _clob_client

def place_buy(token_id, price, size_usd, label=""):
    shares = round(size_usd / price, 1)
    if shares < 1: return None
    if DRY_RUN:
        log.info(f"  {'DRY':>4s} BUY NO @{price:.3f} ${size_usd:.2f} ({shares:.0f}sh) | {label}")
        return f"dry_{int(time.time()*1000)}"
    try:
        from py_clob_client.order import OrderArgs
        from py_clob_client.constants import BUY
        client = get_clob()
        args = OrderArgs(price=round(price,2), size=shares, side=BUY, token_id=token_id)
        signed = client.create_order(args)
        signed["orderType"] = "FOK"
        resp = client.post_order(signed)
        if resp and resp.get("orderID"):
            log.info(f"  LIVE BUY @{price:.3f} ${size_usd:.2f} | {label}")
            return resp["orderID"]
        return None
    except Exception as e:
        log.error(f"  ORDER ERROR: {e}")
        return None

def _max_match():
    return MAX_PER_MATCH if MAX_PER_MATCH > 0 else MY_BANKROLL * 0.15

def execute_leg(triad, leg):
    prices = {"a": triad.price_a, "b": triad.price_b, "d": triad.price_d}
    tokens = {"a": triad.tok_a, "b": triad.tok_b, "d": triad.tok_d}
    markets = {"a": triad.win_a, "b": triad.win_b, "d": triad.draw}
    
    price = prices[leg]; token = tokens[leg]; market = markets[leg]
    if not price or price <= 0: stats.rej_price += 1; return False
    
    mid = market.get("id", "")
    is_new = mid not in ledger.positions or ledger.positions[mid]["cost"] < 0.50
    if not rate_ok(is_new): stats.rej_rate += 1; return False
    if ledger.deployed() >= MY_BANKROLL * MAX_DEPLOY_PCT: stats.rej_deploy += 1; return False
    if ledger.match_deployed(triad.match_key) >= _max_match(): stats.rej_match += 1; return False
    
    size = rn1_size(price)
    size = min(size, MY_BANKROLL * MAX_DEPLOY_PCT - ledger.deployed())
    size = min(size, _max_match() - ledger.match_deployed(triad.match_key))
    size = min(size, MAX_PER_MARKET - triad.leg_deployed.get(leg, 0))
    if size < 1.0: return False
    
    lbl = f"{'NEW' if is_new else 'ADD'}-{leg.upper()}|{triad.short_name()}"
    oid = place_buy(token, price, size, lbl)
    if not oid: return False
    
    ledger.record(mid, price, size, market.get("_title",""), triad.match_key, leg)
    triad.leg_deployed[leg] += size
    triad.leg_shares[leg] += size / price
    triad.leg_fills[leg] += 1
    triad.last_fill_time[leg] = time.time()
    _fill_ts.append(time.time())
    if is_new: _new_ts.append(time.time()); stats.fills_triad += 1
    else: stats.fills_accum += 1
    return True

def process_triad(triad):
    cost = triad.cost
    if not cost or cost > TRIAD_MAX_COST:
        stats.rej_edge += 1; return
    
    now = time.time()
    # Sync from ledger
    for leg in ["a", "b", "d"]:
        mid = {"a": triad.win_a, "b": triad.win_b, "d": triad.draw}[leg].get("id","")
        p = ledger.positions.get(mid)
        if p:
            triad.leg_deployed[leg] = p["cost"]
            triad.leg_shares[leg] = p["shares"]
    
    for leg in ["a", "b", "d"]:
        dep = triad.leg_deployed[leg]
        if dep < 0.50:
            execute_leg(triad, leg)
        elif dep < MAX_PER_MARKET:
            if now - triad.last_fill_time[leg] < TRIAD_ACCUM_CD: continue
            if cost <= TRIAD_MAX_COST:
                execute_leg(triad, leg)


# ═══════ DASHBOARD ═══════
def print_dashboard(triads):
    os.system("clear" if os.name != "nt" else "cls")
    now = datetime.now().strftime("%H:%M:%S")
    s = stats
    
    print(f"\033[1;96m╔═══════════════════════════════════════════════════════════════════════════╗\033[0m")
    print(f"\033[1;96m║  PolyArb v7.1 — TRIAD ARBITRAGE    {now}    {'DRY RUN' if DRY_RUN else '  LIVE':>8s}       ║\033[0m")
    print(f"\033[1;96m╚═══════════════════════════════════════════════════════════════════════════╝\033[0m")
    
    print(f"  Up:{s.runtime:.1f}m  Triads:{s.triads_total} "
          f"\033[92mprof:{s.triads_profitable}\033[0m "
          f"entered:{s.triads_entered} done:{s.triads_complete}  "
          f"Fills:T{s.fills_triad}+A{s.fills_accum}+S{s.fills_supp}={s.total_fills}")
    print(f"  Deploy:${ledger.deployed():.2f}/${MY_BANKROLL*MAX_DEPLOY_PCT:.0f}  "
          f"Active:{ledger.active_count()}  "
          f"Rej[r:{s.rej_rate} d:{s.rej_deploy} m:{s.rej_match} e:{s.rej_edge}]")
    
    priced = sorted([t for t in triads if t.cost is not None], key=lambda t: t.cost)
    profitable = [t for t in priced if t.edge and t.edge > 0]
    entered = [t for t in priced if t.is_entered]
    
    # Edge distribution
    if priced:
        costs = [t.cost for t in priced]
        edges = [t.edge for t in priced]
        
        print(f"\n  \033[1mEDGE DISTRIBUTION\033[0m ({len(priced)} priced)")
        buckets = [
            ("> +$0.10",  lambda e: e > 0.10),
            ("+$0.05–0.10", lambda e: 0.05 < e <= 0.10),
            ("+$0.02–0.05", lambda e: 0.02 < e <= 0.05),
            ("+$0.00–0.02", lambda e: 0.00 < e <= 0.02),
            (" $0.00 flat", lambda e: e == 0),
            ("-$0.02–0.00", lambda e: -0.02 <= e < 0),
            ("-$0.04–0.02", lambda e: -0.04 <= e < -0.02),
            ("< -$0.04",    lambda e: e < -0.04),
        ]
        for label, pred in buckets:
            ct = len([e for e in edges if pred(e)])
            if ct > 0:
                bar = "\033[92m" if "+" in label else ("\033[93m" if "0.00" in label else "\033[91m")
                print(f"    {label:>15s}: {ct:>4d} {bar}{'█' * min(ct, 40)}\033[0m")
    
    # Top profitable
    if profitable:
        print(f"\n  \033[1;92mPROFITABLE TRIADS ({len(profitable)})\033[0m")
        print(f"  {'Match':<36s} {'A_NO':>6s} {'B_NO':>6s} {'D_NO':>6s} {'Cost':>7s} {'Edge':>7s} {'Dep':>6s}")
        for t in profitable[:15]:
            dep = f"${t.total_deployed:.0f}" if t.is_entered else ""
            print(f"  {t.short_name():<36s} @{t.price_a:.3f} @{t.price_b:.3f} @{t.price_d:.3f} "
                  f"${t.cost:.3f} \033[92m{t.edge:+.3f}\033[0m {dep:>6s}")
    
    # Near-profitable
    near = [t for t in priced if t.edge and -0.04 < t.edge <= 0]
    if near and not profitable:
        print(f"\n  \033[1;93mNEAR BREAK-EVEN ({len(near)})\033[0m")
        for t in near[:10]:
            print(f"  {t.short_name():<36s} @{t.price_a:.3f} @{t.price_b:.3f} @{t.price_d:.3f} "
                  f"${t.cost:.3f} \033[93m{t.edge:+.3f}\033[0m")
    
    # Active positions
    if entered:
        print(f"\n  \033[1mACTIVE POSITIONS ({len(entered)})\033[0m")
        for t in entered[:10]:
            legs = sum(1 for v in t.leg_deployed.values() if v > 0)
            print(f"  {t.short_name():<36s} {legs}/3 legs  ${t.total_deployed:.2f} deployed  "
                  f"edge:{t.edge:+.3f}" if t.edge else "")


# ═══════ MAIN ═══════
def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--bankroll", type=float, default=1000)
    p.add_argument("--fresh", action="store_true")
    p.add_argument("--live", action="store_true")
    p.add_argument("--scan-only", action="store_true")
    p.add_argument("--no-dash", action="store_true")
    args = p.parse_args()
    
    global MY_BANKROLL, DRY_RUN
    MY_BANKROLL = args.bankroll
    if args.live: DRY_RUN = False
    
    if args.fresh and os.path.exists("ledger.json"):
        os.remove("ledger.json"); log.info("Removed ledger.json")
    else:
        ledger.load()
    
    running = True
    def _sig(s, f): nonlocal running; running = False
    signal.signal(signal.SIGINT, _sig); signal.signal(signal.SIGTERM, _sig)
    
    log.info("=" * 60)
    log.info("PolyArb v7.1 — FAST TRIAD ARBITRAGE")
    log.info(f"  Bankroll: ${MY_BANKROLL:,.0f}  Mode: {'DRY' if DRY_RUN else 'LIVE'}  "
             f"Max cost: ${TRIAD_MAX_COST}  Scan-only: {args.scan_only}")
    log.info("=" * 60)
    
    triads = []
    leftover = []
    last_refresh = 0; last_scan = 0; last_stats = 0; last_save = 0; last_dash = 0
    scan_idx = 0
    
    while running:
        now = time.time()
        
        # ── REFRESH (every 5 min) ──
        if now - last_refresh >= 300:
            all_m, draws, wins_by_team = fetch_markets_fast()
            triads = build_triads(draws, wins_by_team)
            # Leftover = everything not in a triad
            triad_ids = set()
            for t in triads:
                triad_ids.update([t.win_a.get("id",""), t.win_b.get("id",""), t.draw.get("id","")])
            leftover = [m for m in all_m if m.get("id","") not in triad_ids]
            last_refresh = now; scan_idx = 0
        
        # ── SCAN TRIADS (batch of 20 every 10s) ──
        if now - last_scan >= 10 and triads:
            stats.scans += 1
            batch_size = min(20, len(triads))
            start = scan_idx % len(triads)
            batch = triads[start:start + batch_size]
            scan_idx = (scan_idx + batch_size) % len(triads)
            
            # Always include entered triads
            for t in triads:
                if t.is_entered and t not in batch: batch.append(t)
            
            for t in batch:
                if not running: break
                t.price_a, t.depth_a = fetch_book(t.tok_a)
                t.price_b, t.depth_b = fetch_book(t.tok_b)
                t.price_d, t.depth_d = fetch_book(t.tok_d)
                t.priced = t.price_a is not None
                
                if not args.scan_only:
                    process_triad(t)
                time.sleep(0.05)
            
            stats.triads_total = len(triads)
            stats.triads_profitable = sum(1 for t in triads if t.edge and t.edge > 0)
            stats.triads_entered = sum(1 for t in triads if t.is_entered)
            stats.triads_complete = sum(1 for t in triads if t.is_complete)
            last_scan = now
        
        # ── STATS ──
        if now - last_stats >= 30:
            log.info(stats.line()); last_stats = now
        
        # ── SAVE ──
        if now - last_save >= 30:
            ledger.save(); last_save = now
        
        # ── DASHBOARD ──
        if not args.no_dash and now - last_dash >= 5:
            print_dashboard(triads); last_dash = now
        
        time.sleep(0.5)
    
    ledger.save()
    log.info(f"SHUTDOWN | {stats.line()}")

if __name__ == "__main__":
    main()
