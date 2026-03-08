#!/usr/bin/env python3
"""
PolyArb v7.2 — UNIVERSAL NO STRATEGY
═════════════════════════════════════════
RN1's REAL strategy decoded from 1.1M trades:

  ★ BUY CHEAP NOs ACROSS ALL MARKETS ★

He's THE HOUSE. Retail bets YES on their team/player/outcome.
YES is systematically overpriced → NO is underpriced.
Buy NO everywhere, diversify massively, collect $1 per share
when the "popular" outcome doesn't happen.

MARKET TYPES (his actual allocation):
  28% — Match winner (2-outcome: "X vs Y")
  26% — Win markets (soccer "Will X win?")
  21% — Esports (2-outcome: BO3/BO5)
  15% — O/U (2-outcome: over/under goals)
   8% — Draw (soccer 3rd outcome)
   2% — Spreads

STRATEGY BY TYPE:
  2-outcome: Buy the cheap NO. Done.
  3-outcome: Heavy NO on primary, lighter hedge, tiny draw.
  Pure arb:  When 3 NOs < $2.00, guaranteed profit.

SIZING (from his $853 start):
  Day 1: avg $6.60/trade, 180 trades
  Median: $10.98  |  Mean: $84.78 (skewed by big positions)
  p10=$0.67 (test buys) → p90=$216 (conviction plays)

Usage:
  python3.11 arb_v72.py --bankroll 1000 --fresh --scan-only
  python3.11 arb_v72.py --bankroll 1000 --fresh           # dry run
  python3.11 arb_v72.py --bankroll 1000 --fresh --live     # real $
  python3.11 arb_v72.py --bankroll 1000 --arb-only         # arbs only
"""

import os, sys, json, time, re, signal, logging, math
from datetime import datetime, timezone
from collections import defaultdict
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

PRIVATE_KEY = os.getenv("PK", "")
CLOB = "https://clob.polymarket.com"
GAMMA = "https://gamma-api.polymarket.com"

MY_BANKROLL    = float(os.getenv("BANKROLL", "1000"))
DRY_RUN        = True
MAX_DEPLOY_PCT = 0.90
MAX_PER_MATCH  = 0.15   # 15% bankroll per match/event
MAX_PER_MARKET = 0.10   # 10% bankroll per individual market

# NO price range we buy
NO_MIN_PRICE   = 0.05   # skip penny NOs (outcome too likely)
NO_MAX_PRICE   = 0.92   # skip expensive NOs (outcome unlikely, NO overpriced)
NO_SWEET_SPOT  = (0.20, 0.60)  # his favorite range — most volume here

# Arb threshold
ARB_MAX_COST   = 2.00

# Accumulation
ACCUM_CD       = 45     # seconds cooldown between adds
MAX_FILLS_MIN  = 30
MAX_NEW_HR     = 90

# Futures keywords to skip
FUTURES_KW = ['finals','stanley cup','world cup','super bowl','championship',
    'premier league winner','serie a winner','bundesliga winner','ligue 1 winner',
    'la liga winner','champion','playoffs','division','conference','nomination',
    'presidential','election','award','mvp','rookie','draft','qualify','relegat',
    'golden boot','best player','ballon']

# ═══════ LOGGING ═══════
os.makedirs("logs", exist_ok=True)
_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = f"logs/arb_v72_{_ts}.log"
log = logging.getLogger("v72")
log.setLevel(logging.DEBUG)
fh = logging.FileHandler(LOG_FILE); fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(fh)
sh = logging.StreamHandler(); sh.setLevel(logging.INFO)
sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(sh)

# ═══════ API ═══════
def api_get(url, timeout=15, retries=2):
    for attempt in range(retries+1):
        try:
            req = Request(url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
            return json.loads(urlopen(req, timeout=timeout).read().decode("utf-8"))
        except HTTPError as e:
            if e.code in (429,403): time.sleep(2**attempt*2); continue
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
    while _fill_ts and now-_fill_ts[0]>60: _fill_ts.pop(0)
    while _new_ts and now-_new_ts[0]>3600: _new_ts.pop(0)
    if len(_fill_ts)>=MAX_FILLS_MIN: return False
    if is_new and len(_new_ts)>=MAX_NEW_HR: return False
    return True

# ═══════ SIZING ═══════
def calc_size(price, weight="normal"):
    """
    Calibrated from RN1 first week: ~$7/trade at $853 bankroll.
    Scale to our bankroll. Weight adjusts for conviction.
    """
    scale = MY_BANKROLL / 853
    # Base: price-dependent (cheap NO = smaller position)
    if   price < 0.10: base = 1.5
    elif price < 0.20: base = 3.0
    elif price < 0.30: base = 5.0
    elif price < 0.50: base = 7.0   # sweet spot
    elif price < 0.70: base = 9.0
    elif price < 0.85: base = 11.0
    else:               base = 14.0
    
    mult = {"heavy":1.5, "normal":1.0, "light":0.7, "tiny":0.25}.get(weight, 1.0)
    size = base * scale * mult
    return round(min(size, MY_BANKROLL * 0.02), 2)

# ═══════ MARKET CLASSIFICATION ═══════
def _is_futures(t):
    return any(kw in t.lower() for kw in FUTURES_KW)

def _normalize(s):
    return re.sub(r'[^a-z0-9\s]', '', s.lower()).strip()

def classify_market(title):
    """Classify market and extract metadata."""
    if not title: return None
    
    # Draw: "Will X vs Y end in a draw?"
    dm = re.match(r'Will (.+?)\s+vs\.?\s+(.+?)\s+end in a draw\s*\??$', title, re.I)
    if dm:
        return {"type":"draw", "a":dm.group(1).strip(), "b":dm.group(2).strip(),
                "na":_normalize(dm.group(1)), "nb":_normalize(dm.group(2))}
    
    # Win: "Will X win on DATE?"
    wm = re.match(r'Will (.+?) win(?:\s+on\s+\d{4}-\d{2}-\d{2})?\s*\??$', title, re.I)
    if wm and not _is_futures(title):
        return {"type":"win", "team":wm.group(1).strip(), "nt":_normalize(wm.group(1))}
    
    # O/U
    if re.search(r'o/u\s+\d', title, re.I):
        return {"type":"ou"}
    
    # Spread
    if 'spread' in title.lower():
        return {"type":"spread"}
    
    # Esports
    if any(w in title.lower() for w in ['counter-strike','dota','valorant','lol:',
                                         'league of legends','bo3','bo5','bo2']):
        return {"type":"esports"}
    
    # Generic match "X vs Y"
    vm = re.match(r'^(.+?)\s+vs\.?\s+(.+?)(?:\s*\(|$)', title, re.I)
    if vm and not _is_futures(title):
        return {"type":"match", "a":vm.group(1).strip(), "b":vm.group(2).strip()}
    
    if _is_futures(title):
        return {"type":"futures"}
    
    return {"type":"other"}

# ═══════ OPPORTUNITY ═══════
class Opp:
    """A single NO buying opportunity."""
    def __init__(self, market, cls):
        self.market = market
        self.cls = cls
        self.mid = market.get("id","")
        self.title = market.get("_title","")
        self.tok_no = market.get("_clob_no","")
        self.type = cls["type"]
        self.price = None
        self.depth = 0
        self.deployed = 0
        self.shares = 0
        self.fills = 0
        self.last_fill = 0
        self.match_key = None  # for grouping soccer legs
    
    def short(self):
        return self.title[:40] if self.title else self.mid[:12]

# ═══════ MATCH GROUP ═══════
class MatchGroup:
    """Groups related markets (e.g., WinA + WinB + Draw for soccer)."""
    def __init__(self, key):
        self.key = key
        self.opps = {}  # leg → Opp
    
    def add(self, leg, opp):
        self.opps[leg] = opp
        opp.match_key = self.key
    
    def cost3(self):
        if len(self.opps) < 3: return None
        prices = [o.price for o in self.opps.values() if o.price]
        return round(sum(prices), 4) if len(prices)==3 else None
    
    def is_arb(self):
        c = self.cost3()
        return c is not None and c < ARB_MAX_COST
    
    def deployed(self):
        return sum(o.deployed for o in self.opps.values())

# ═══════ LEDGER ═══════
class Ledger:
    def __init__(self):
        self.pos = {}
    def record(self, mid, price, size, title="", mk=""):
        if mid not in self.pos:
            self.pos[mid]={"cost":0,"shares":0,"fills":0,"mk":mk,"title":title}
        p=self.pos[mid]
        p["cost"]+=size; p["shares"]+=size/price if price>0 else 0; p["fills"]+=1
    def deployed(self): return sum(p["cost"] for p in self.pos.values())
    def active(self): return sum(1 for p in self.pos.values() if p["cost"]>0)
    def mkt_dep(self, mid): return self.pos[mid]["cost"] if mid in self.pos else 0
    def grp_dep(self, mk):
        if not mk: return 0
        return sum(p["cost"] for p in self.pos.values() if p.get("mk")==mk)
    def save(self, path="ledger.json"):
        with open(path,"w") as f: json.dump(self.pos, f, indent=2)
    def load(self, path="ledger.json"):
        if os.path.exists(path):
            with open(path) as f: self.pos=json.load(f)

ledger = Ledger()

# ═══════ STATS ═══════
class Stats:
    def __init__(self):
        self.start=time.time()
        self.total_opps=0; self.priced=0; self.arbs=0; self.cheap_nos=0
        self.fills_arb=0; self.fills_dir=0; self.fills_2way=0
        self.rej_rate=0; self.rej_dep=0; self.rej_grp=0; self.rej_price=0
        self.scans=0
    @property
    def runtime(self): return (time.time()-self.start)/60
    @property
    def total_fills(self): return self.fills_arb+self.fills_dir+self.fills_2way
    def line(self):
        return (f"Up:{self.runtime:.1f}m Opps:{self.total_opps} Priced:{self.priced} "
                f"Arbs:{self.arbs} Cheap:{self.cheap_nos} "
                f"Fills[arb:{self.fills_arb} dir:{self.fills_dir} 2w:{self.fills_2way} "
                f"={self.total_fills}] ${ledger.deployed():.2f}/{MY_BANKROLL*MAX_DEPLOY_PCT:.0f}")

stats = Stats()

# ═══════ EXECUTION ═══════
_clob = None
def get_clob():
    global _clob
    if _clob: return _clob
    from py_clob_client.client import ClobClient
    _clob = ClobClient(CLOB, key=PRIVATE_KEY, chain_id=137, signature_type=2,
                        funder=os.getenv("FUNDER"))
    _clob.set_api_creds(_clob.create_or_derive_api_creds())
    return _clob

def place_buy(token_id, price, size_usd, label=""):
    shares = round(size_usd/price, 1)
    if shares < 1: return None
    if DRY_RUN:
        log.info(f"  DRY BUY NO @{price:.3f} ${size_usd:.2f} ({shares:.0f}sh) | {label}")
        return f"dry_{int(time.time()*1000)}"
    try:
        from py_clob_client.order import OrderArgs
        from py_clob_client.constants import BUY
        c=get_clob()
        args=OrderArgs(price=round(price,2),size=shares,side=BUY,token_id=token_id)
        signed=c.create_order(args); signed["orderType"]="FOK"
        resp=c.post_order(signed)
        if resp and resp.get("orderID"):
            log.info(f"  LIVE BUY @{price:.3f} ${size_usd:.2f} | {label}")
            return resp["orderID"]
    except Exception as e:
        log.error(f"  ORDER ERR: {e}")
    return None

def buy_opp(opp, weight="normal", mode="2way"):
    """Buy NO on an opportunity."""
    if not opp.price or opp.price < NO_MIN_PRICE or opp.price > NO_MAX_PRICE:
        stats.rej_price += 1; return False
    
    mid = opp.mid
    is_new = ledger.mkt_dep(mid) < 0.50
    if not rate_ok(is_new): stats.rej_rate+=1; return False
    if ledger.deployed() >= MY_BANKROLL*MAX_DEPLOY_PCT: stats.rej_dep+=1; return False
    
    mk = opp.match_key or mid
    max_grp = MY_BANKROLL * MAX_PER_MATCH
    max_mkt = MY_BANKROLL * MAX_PER_MARKET
    if ledger.grp_dep(mk) >= max_grp: stats.rej_grp+=1; return False
    
    size = calc_size(opp.price, weight)
    size = min(size, MY_BANKROLL*MAX_DEPLOY_PCT - ledger.deployed())
    size = min(size, max_grp - ledger.grp_dep(mk))
    size = min(size, max_mkt - ledger.mkt_dep(mid))
    if size < 0.50: return False
    
    lbl = f"{mode}|{weight}|{opp.short()}"
    oid = place_buy(opp.tok_no, opp.price, size, lbl)
    if not oid: return False
    
    ledger.record(mid, opp.price, size, opp.title, mk)
    opp.deployed += size
    opp.shares += size/opp.price
    opp.fills += 1
    opp.last_fill = time.time()
    _fill_ts.append(time.time())
    if is_new: _new_ts.append(time.time())
    
    if mode=="arb": stats.fills_arb+=1
    elif mode=="dir": stats.fills_dir+=1
    else: stats.fills_2way+=1
    return True

# ═══════ MARKET DISCOVERY ═══════
def fetch_and_classify():
    """Fetch all active markets, classify, group into matches."""
    log.info("Fetching markets...")
    opps = []
    groups = {}  # match_key → MatchGroup
    draw_index = {}  # (na,nb) → key
    
    offset = 0; last_log = time.time()
    while True:
        data = api_get(f"{GAMMA}/markets?limit=100&offset={offset}&active=true&closed=false", timeout=20)
        if not data or not isinstance(data, list) or len(data)==0: break
        
        for m in data:
            if not m.get("active") or m.get("closed"): continue
            clob_ids = m.get("clobTokenIds", [])
            if isinstance(clob_ids, str):
                try: clob_ids = json.loads(clob_ids)
                except: clob_ids = []
            if not isinstance(clob_ids, list) or len(clob_ids)<2: continue
            
            market = {
                "id": m.get("id",""),
                "_title": m.get("question","") or m.get("title",""),
                "_clob_yes": str(clob_ids[0]),
                "_clob_no": str(clob_ids[1]),
                "volume": float(m.get("volume",0) or 0),
            }
            
            cls = classify_market(market["_title"])
            if not cls or cls["type"] in ("futures","other"): continue
            
            opp = Opp(market, cls)
            opps.append(opp)
            
            # Build match groups for soccer
            if cls["type"] == "draw":
                na, nb = cls["na"], cls["nb"]
                mk = f"{min(na,nb)}|{max(na,nb)}"
                draw_index[(na,nb)] = mk; draw_index[(nb,na)] = mk
                if mk not in groups: groups[mk] = MatchGroup(mk)
                groups[mk].add("d", opp)
            
            elif cls["type"] == "win":
                nt = cls["nt"]
                for (na,nb), mk in draw_index.items():
                    if nt==na:
                        if mk not in groups: groups[mk]=MatchGroup(mk)
                        groups[mk].add("a", opp); break
                    elif nt==nb:
                        if mk not in groups: groups[mk]=MatchGroup(mk)
                        groups[mk].add("b", opp); break
                else:
                    # Fuzzy
                    for (na,nb), mk in draw_index.items():
                        if nt in na or na in nt:
                            if mk not in groups: groups[mk]=MatchGroup(mk)
                            groups[mk].add("a", opp); break
                        elif nt in nb or nb in nt:
                            if mk not in groups: groups[mk]=MatchGroup(mk)
                            groups[mk].add("b", opp); break
        
        if len(data)<100: break
        offset += 100
        if time.time()-last_log > 8:
            log.info(f"  ...{len(opps)} opps, {len(groups)} groups")
            last_log = time.time()
        time.sleep(0.1)
    
    # Count types
    from collections import Counter
    tc = Counter(o.type for o in opps)
    log.info(f"Found {len(opps):,} opportunities: {dict(tc)}")
    log.info(f"Match groups: {len(groups)} ({sum(1 for g in groups.values() if len(g.opps)==3)} complete triads)")
    
    return opps, groups

# ═══════ STRATEGY ENGINE ═══════
def process_opportunities(opps, groups, scan_only=False, arb_only=False):
    """Price and potentially trade a batch of opportunities."""
    now = time.time()
    
    # Check arbs first
    for mk, grp in groups.items():
        if len(grp.opps) < 3: continue
        # Make sure all are priced
        all_priced = all(o.price is not None for o in grp.opps.values())
        if not all_priced: continue
        
        if grp.is_arb():
            c = grp.cost3()
            edge = round(2.00 - c, 4)
            log.info(f"  💰 ARB: {mk} cost=${c:.4f} edge={edge:+.4f}")
            if not scan_only:
                for leg, opp in grp.opps.items():
                    if opp.deployed < MY_BANKROLL * MAX_PER_MARKET:
                        buy_opp(opp, "normal", "arb")
    
    if arb_only or scan_only:
        return
    
    # Directional: 3-outcome soccer
    for mk, grp in groups.items():
        if len(grp.opps) < 2: continue
        priced = {l:o for l,o in grp.opps.items() if o.price and NO_MIN_PRICE <= o.price <= NO_MAX_PRICE}
        if len(priced) < 2: continue
        
        # Find cheapest NO (= market's favorite outcome)
        sorted_legs = sorted(priced.items(), key=lambda x: x[1].price)
        primary_leg, primary_opp = sorted_legs[0]
        secondary_leg, secondary_opp = sorted_legs[1]
        
        # Only enter if primary NO is in sweet spot
        if primary_opp.price > 0.60: continue
        
        # Primary (heavy)
        if now - primary_opp.last_fill >= ACCUM_CD:
            buy_opp(primary_opp, "heavy", "dir")
        
        # Secondary (lighter) — after primary has position
        if primary_opp.deployed > 3:
            if now - secondary_opp.last_fill >= ACCUM_CD:
                buy_opp(secondary_opp, "light", "dir")
        
        # Draw (tiny) — if available and both wins have positions
        if "d" in grp.opps and primary_opp.deployed > 5 and secondary_opp.deployed > 2:
            draw_opp = grp.opps["d"]
            if draw_opp.price and now - draw_opp.last_fill >= ACCUM_CD * 3:
                buy_opp(draw_opp, "tiny", "dir")
    
    # 2-outcome: esports, O/U, spreads, generic matches
    for opp in opps:
        if opp.match_key: continue  # already handled in groups
        if opp.type in ("draw","win"): continue  # soccer handled above
        if not opp.price: continue
        if opp.price < NO_MIN_PRICE or opp.price > NO_MAX_PRICE: continue
        
        # Only buy if NO is in sweet spot (cheap = likely to pay)
        if opp.price > 0.60: continue
        
        if now - opp.last_fill >= ACCUM_CD:
            buy_opp(opp, "normal", "2way")

# ═══════ DASHBOARD ═══════
def dashboard(opps, groups):
    os.system("clear" if os.name!="nt" else "cls")
    now = datetime.now().strftime("%H:%M:%S")
    s = stats
    
    print(f"\033[1;96m╔═══════════════════════════════════════════════════════════════════════════╗\033[0m")
    print(f"\033[1;96m║  PolyArb v7.2 — UNIVERSAL NO STRATEGY  {now}  {'DRY RUN' if DRY_RUN else '  LIVE':>8s}    ║\033[0m")
    print(f"\033[1;96m╚═══════════════════════════════════════════════════════════════════════════╝\033[0m")
    print(f"  {s.line()}")
    
    priced = [o for o in opps if o.price is not None]
    
    # Arb section
    arbs = [(mk,g) for mk,g in groups.items() if g.is_arb()]
    if arbs:
        print(f"\n  \033[1;92m💰 ARB OPPORTUNITIES ({len(arbs)})\033[0m")
        for mk, g in sorted(arbs, key=lambda x: x[1].cost3()):
            prices = " + ".join(f"@{o.price:.3f}" for l,o in sorted(g.opps.items()) if o.price)
            print(f"  {mk:<38s} {prices} = ${g.cost3():.4f} \033[92m+{2-g.cost3():.4f}\033[0m")
    
    # Cheap NOs by type
    from collections import Counter
    cheap = [o for o in priced if o.price <= 0.60 and o.price >= NO_MIN_PRICE]
    type_counts = Counter(o.type for o in cheap)
    
    if cheap:
        print(f"\n  \033[1mCHEAP NOs ≤ 60¢ ({len(cheap)} across types)\033[0m")
        for typ, ct in type_counts.most_common():
            print(f"    {typ}: {ct}")
        
        # Show top by cheapest
        print(f"\n  {'Market':<40s} {'Type':>8s} {'NO':>6s} {'Depth':>7s}")
        for o in sorted(cheap, key=lambda o: o.price)[:15]:
            clr = "\033[92m" if o.price < 0.30 else ("\033[93m" if o.price < 0.50 else "\033[0m")
            dep = f"${o.deployed:.0f}" if o.deployed > 0 else ""
            print(f"  {o.short():<40s} {o.type:>8s} {clr}@{o.price:.3f}\033[0m ${o.depth:>6.0f} {dep}")
    
    # Active positions
    entered = [o for o in opps if o.deployed > 0]
    if entered:
        print(f"\n  \033[1mACTIVE ({len(entered)})\033[0m")
        for o in sorted(entered, key=lambda o: -o.deployed)[:10]:
            print(f"  {o.short():<40s} {o.type:>8s} @{o.price:.3f} ${o.deployed:.2f} "
                  f"({o.fills} fills, {o.shares:.0f}sh)")
    
    print(f"\n  {'═'*70}")
    total_sh = sum(p["shares"] for p in ledger.pos.values())
    print(f"  DEPLOYED: ${ledger.deployed():.2f}  SHARES: {total_sh:.0f}  POSITIONS: {ledger.active()}")

# ═══════ MAIN ═══════
def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--bankroll", type=float, default=1000)
    p.add_argument("--fresh", action="store_true")
    p.add_argument("--live", action="store_true")
    p.add_argument("--scan-only", action="store_true")
    p.add_argument("--arb-only", action="store_true")
    p.add_argument("--no-dash", action="store_true")
    args = p.parse_args()
    
    global MY_BANKROLL, DRY_RUN
    MY_BANKROLL = args.bankroll
    if args.live: DRY_RUN = False
    
    if args.fresh and os.path.exists("ledger.json"):
        os.remove("ledger.json")
    else:
        ledger.load()
    
    running = True
    def _sig(s,f): nonlocal running; running=False
    signal.signal(signal.SIGINT, _sig); signal.signal(signal.SIGTERM, _sig)
    
    log.info("="*60)
    log.info(f"PolyArb v7.2 — UNIVERSAL NO STRATEGY")
    log.info(f"  Bankroll:${MY_BANKROLL:,.0f} Mode:{'DRY' if DRY_RUN else 'LIVE'} "
             f"Scan:{args.scan_only} ArbOnly:{args.arb_only}")
    log.info("="*60)
    
    opps = []; groups = {}
    last_refresh=0; last_scan=0; last_stats=0; last_save=0; last_dash=0
    scan_idx = 0
    
    while running:
        now = time.time()
        
        # Refresh every 5 min
        if now - last_refresh >= 300:
            opps, groups = fetch_and_classify()
            last_refresh = now; scan_idx = 0
        
        # Scan batch every 8s
        if now - last_scan >= 8 and opps:
            stats.scans += 1
            batch_size = min(30, len(opps))
            start = scan_idx % len(opps)
            batch = opps[start:start+batch_size]
            scan_idx = (scan_idx + batch_size) % len(opps)
            
            # Always include entered
            for o in opps:
                if o.deployed > 0 and o not in batch: batch.append(o)
            
            for o in batch:
                if not running: break
                o.price, o.depth = fetch_book(o.tok_no)
                time.sleep(0.04)
            
            # Process
            process_opportunities(opps, groups, args.scan_only, args.arb_only)
            
            # Update stats
            stats.total_opps = len(opps)
            stats.priced = sum(1 for o in opps if o.price is not None)
            stats.arbs = sum(1 for g in groups.values() if g.is_arb())
            stats.cheap_nos = sum(1 for o in opps if o.price and NO_MIN_PRICE<=o.price<=0.60)
            last_scan = now
        
        if now-last_stats>=30: log.info(stats.line()); last_stats=now
        if now-last_save>=30: ledger.save(); last_save=now
        if not args.no_dash and now-last_dash>=5:
            dashboard(opps, groups); last_dash=now
        
        time.sleep(0.5)
    
    ledger.save()
    log.info(f"SHUTDOWN | {stats.line()}")

if __name__=="__main__":
    main()
