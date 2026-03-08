#!/usr/bin/env python3
"""
Polymarket Sports Data Collector
=================================
Standalone program that watches ALL sports markets on Polymarket 24/7.
Records price ticks every ~30 seconds for every market, detects resolutions,
and logs everything to SQLite for later analysis.

Run:  python3 collector.py
Stop: Ctrl+C (graceful shutdown)

Data stored in: collector_data/sports.db
"""

import os
import re
import sys
import json
import time
import signal
import sqlite3
import logging
import asyncio
import threading
from datetime import datetime, timedelta
from collections import defaultdict

try:
    import requests
except ImportError:
    print("pip install requests"); sys.exit(1)
try:
    import websockets
except ImportError:
    print("pip install websockets"); sys.exit(1)

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

GAMMA = "https://gamma-api.polymarket.com"
WS_URI = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
WS_BATCH_SIZE = 500  # max tokens per WS connection

DISCOVERY_INTERVAL = 120    # re-scan gamma for new markets every 2 min
SNAPSHOT_INTERVAL = 30      # record prices every 30 sec
RESOLUTION_CHECK = 60       # check for resolved markets every 60 sec
MAX_DAYS_OUT = 7            # only track markets ending within 7 days
DB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "collector_data")
DB_FILE = os.path.join(DB_DIR, "sports.db")

# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

os.makedirs(DB_DIR, exist_ok=True)
os.makedirs("logs", exist_ok=True)
_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
log = logging.getLogger("collector")
log.setLevel(logging.DEBUG)
_fh = logging.FileHandler(f"logs/collector_{_ts}.log"); _fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(_fh)
_sh = logging.StreamHandler(); _sh.setLevel(logging.INFO)
_sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(_sh)

# ═══════════════════════════════════════════════════════════════
# SPORTS DETECTION (same logic as arb_v10.py)
# ═══════════════════════════════════════════════════════════════

FUTURES_KW = [
    'win the 20', 'win 20', 'champion', 'championship',
    'win the nba', 'win the nfl', 'win the mlb', 'win the nhl',
    'win the world cup', 'win the cup', 'win the premier',
    'win the champions league', 'mvp', 'rookie of the year',
    'win the title', 'win the league', 'win the tournament',
    'be relegated', 'relegation', 'make the playoffs',
    'reach the final', 'win the final', 'medal at',
    'top scorer', 'leading scorer', 'scoring title',
]

REJECT_KW = [
    'conflict', 'ceasefire', 'tariff', 'legislation',
    'best picture', 'oscar', 'emmy', 'grammy',
    'bitcoin', 'crypto', 'ethereum', 'solana',
    'election', 'president', 'congress', 'senate',
    'democrat', 'republican', 'trump', 'biden',
    'spacex', 'tesla stock', 'fed rate', 'inflation',
    'ai model', 'gpt', 'openai', 'temperature record',
]

SPORTS_PATTERNS = [
    r'\bvs\.?\b', r'o/u\s+\d', r'over/under', r'^spread[:\s]',
    r'nba', r'nfl', r'mlb', r'nhl', r'ncaa', r'mls',
    r'premier league', r'la liga', r'bundesliga', r'serie a', r'ligue 1',
    r'atp', r'wta', r'ufc\s', r'mma', r'boxing',
    r'(basketball|tennis|soccer|football|hockey|baseball)',
    r'(counter-strike|dota|valorant|lol:|league of legends)',
    r'(blazers|celtics|lakers|warriors|heat|bulls|knicks|nets|bucks)',
    r'(chiefs|eagles|cowboys|49ers|bills|ravens|dolphins)',
    r'(yankees|dodgers|braves|astros|phillies|mets)',
    r'(avalanche|bruins|rangers|oilers|panthers|stars)',
    r'(spread|handicap|moneyline|match winner)',
    r'(bnp paribas|roland garros|wimbledon|us open|australian open)',
    r'(champions league|europa league|fa cup|copa america)',
    r'(grand slam|masters 1000|atp \d{3,4}|wta \d{3,4})',
    r'(bo3|bo5|map \d|game \d)',
    r'(hornets|mavericks|rockets|pacers|hawks|spurs|kings|pistons)',
]

def is_sports_market(title):
    if not title: return False
    tl = title.lower()
    if any(kw in tl for kw in FUTURES_KW): return False
    if any(kw in tl for kw in REJECT_KW): return False
    for pat in SPORTS_PATTERNS:
        if re.search(pat, tl, re.I): return True
    return False

def classify(title):
    if not title: return "other"
    tl = title.lower()
    if 'end in a draw' in tl or 'draw?' in tl: return "draw"
    if re.search(r'o/u\s+\d', tl) or 'over/under' in tl: return "ou"
    if re.search(r'total (points|goals|runs|sets|games)', tl): return "ou"
    if 'both teams to score' in tl: return "btts"
    if tl.startswith('spread:') or tl.startswith('spread ') or 'handicap' in tl: return "spread"
    if re.search(r'(by (ko|tko|submission|decision)|go the distance)', tl): return "mma"
    if any(w in tl for w in ['counter-strike','dota','valorant','lol:',
        'league of legends','bo3','bo5','cs2']): return "esports"
    if re.search(r'\bvs\.?\b', tl): return "match"
    return "other"

def detect_sport(title):
    """Detect which sport this market is for."""
    if not title: return "unknown"
    tl = title.lower()
    if any(w in tl for w in ['nba', 'basketball', 'blazers', 'celtics', 'lakers',
        'warriors', 'heat', 'bulls', 'knicks', 'nets', 'bucks', 'hornets',
        'mavericks', 'rockets', 'pacers', 'hawks', 'spurs', 'kings', 'pistons',
        'cavaliers', 'nuggets', 'clippers', 'thunder', 'grizzlies', 'timberwolves',
        'pelicans', 'wizards', 'raptors', 'suns', 'magic', 'ncaab']): return "basketball"
    if any(w in tl for w in ['nfl', 'chiefs', 'eagles', 'cowboys', '49ers',
        'bills', 'ravens', 'dolphins', 'bengals', 'lions', 'packers']): return "football"
    if any(w in tl for w in ['atp', 'wta', 'tennis', 'bnp paribas', 'roland garros',
        'wimbledon', 'australian open', 'us open', 'grand slam', 'masters 1000']): return "tennis"
    if any(w in tl for w in ['nhl', 'hockey', 'avalanche', 'bruins', 'rangers',
        'oilers', 'panthers', 'stars', 'flames', 'canadiens', 'penguins']): return "hockey"
    if any(w in tl for w in ['mlb', 'baseball', 'yankees', 'dodgers', 'braves',
        'astros', 'phillies', 'mets', 'cubs', 'red sox']): return "baseball"
    if any(w in tl for w in ['premier league', 'la liga', 'bundesliga', 'serie a',
        'ligue 1', 'mls', 'soccer', 'fc', 'united', 'champions league',
        'europa league', 'fa cup']): return "soccer"
    if any(w in tl for w in ['ufc', 'mma', 'boxing', 'fight']): return "combat"
    if any(w in tl for w in ['counter-strike', 'dota', 'valorant', 'lol',
        'league of legends', 'cs2', 'esport']): return "esports"
    return "other"

# ═══════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════

def init_db():
    """Create SQLite database and tables."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Markets table — one row per market
    c.execute("""CREATE TABLE IF NOT EXISTS markets (
        mid TEXT PRIMARY KEY,
        title TEXT,
        typ TEXT,
        sport TEXT,
        tok_no TEXT,
        tok_yes TEXT,
        vol24 REAL DEFAULT 0,
        end_date TEXT,
        first_seen REAL,
        resolved_at REAL DEFAULT 0,
        winner TEXT DEFAULT '',
        no_bid_final REAL DEFAULT 0,
        yes_bid_final REAL DEFAULT 0
    )""")

    # Price ticks — the main data table
    c.execute("""CREATE TABLE IF NOT EXISTS ticks (
        ts REAL,
        mid TEXT,
        no_bid REAL,
        no_ask REAL,
        no_depth REAL,
        yes_bid REAL,
        yes_ask REAL,
        yes_depth REAL,
        FOREIGN KEY (mid) REFERENCES markets(mid)
    )""")

    # Index for fast queries
    c.execute("CREATE INDEX IF NOT EXISTS idx_ticks_mid_ts ON ticks(mid, ts)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_markets_resolved ON markets(resolved_at)")

    conn.commit()
    return conn

# ═══════════════════════════════════════════════════════════════
# MARKET DISCOVERY
# ═══════════════════════════════════════════════════════════════

# Global state
_markets = {}        # {mid: {"title", "typ", "sport", "tok_no", "tok_yes", "end_date", "vol24"}}
_tok_to_mid = {}     # {token_id: (mid, "NO"|"YES")}
_ws_cache = {}       # {token_id: (best_ask, ask_depth, best_bid, bid_depth, ts)}
_ws_running = True
_ws_new_tokens = []  # tokens to subscribe on the fly
_lock = threading.Lock()

_http_session = None

def get_session():
    global _http_session
    if not _http_session:
        _http_session = requests.Session()
        _http_session.headers.update({"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=0)
        _http_session.mount("https://", adapter)
    return _http_session

def api_get(url, timeout=15, retries=2):
    sess = get_session()
    for attempt in range(retries + 1):
        try:
            resp = sess.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                time.sleep(2 ** attempt * 2)
                continue
            if e.response and e.response.status_code >= 500:
                time.sleep(1)
                continue
            return None
        except Exception:
            time.sleep(0.5)
            continue
    return None

def discover_markets(db_conn):
    """Fetch sports markets from Gamma API and register new ones."""
    new_count = 0
    new_tokens = []

    for offset in range(0, 2000, 200):
        url = (f"{GAMMA}/markets?limit=200&offset={offset}"
               f"&active=true&closed=false"
               f"&order=volume24hr&ascending=false")

        data = api_get(url, timeout=15)
        if not data or not isinstance(data, list):
            if offset > 0: break
            continue

        if len(data) < 50 and offset > 0:
            # Process these but stop after
            pass

        for m in data:
            mid = m.get("id", "")
            if not mid: continue

            # Already tracking?
            with _lock:
                if mid in _markets:
                    # Update vol24
                    _markets[mid]["vol24"] = float(m.get("volume24hr") or 0)
                    continue

            title = m.get("question", "") or m.get("title", "")
            if not is_sports_market(title):
                continue

            # End date filter
            end_str = m.get("endDate") or m.get("end_date_iso") or ""
            if end_str:
                try:
                    end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    if end_dt < datetime.now(end_dt.tzinfo):
                        continue  # already ended
                    if end_dt > datetime.now(end_dt.tzinfo) + timedelta(days=MAX_DAYS_OUT):
                        continue  # too far out
                except (ValueError, TypeError):
                    pass

            # Get CLOB token IDs
            clob_ids = m.get("clobTokenIds", [])
            if isinstance(clob_ids, str):
                try: clob_ids = json.loads(clob_ids)
                except: clob_ids = []
            if not isinstance(clob_ids, list) or len(clob_ids) < 2:
                continue

            typ = classify(title)
            sport = detect_sport(title)
            vol24 = float(m.get("volume24hr") or 0)

            mkt_data = {
                "title": title, "typ": typ, "sport": sport,
                "tok_no": clob_ids[0], "tok_yes": clob_ids[1],
                "end_date": end_str, "vol24": vol24,
            }

            with _lock:
                _markets[mid] = mkt_data
                _tok_to_mid[clob_ids[0]] = (mid, "NO")
                _tok_to_mid[clob_ids[1]] = (mid, "YES")

            # Save to DB
            try:
                db_conn.execute(
                    "INSERT OR IGNORE INTO markets (mid, title, typ, sport, tok_no, tok_yes, vol24, end_date, first_seen) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (mid, title[:200], typ, sport, clob_ids[0], clob_ids[1], vol24, end_str, time.time()))
            except sqlite3.Error:
                pass

            new_tokens.extend(clob_ids)
            new_count += 1

        if len(data) < 50:
            break

    db_conn.commit()

    # Queue new tokens for WS subscription
    if new_tokens:
        with _lock:
            _ws_new_tokens.extend(new_tokens)

    return new_count

# ═══════════════════════════════════════════════════════════════
# WEBSOCKET PRICE STREAM
# ═══════════════════════════════════════════════════════════════

async def _process_ws_event(evt):
    """Process a single WS event."""
    event_type = evt.get("event_type", "book")
    aid = evt.get("asset_id", "")

    if event_type == "price_change":
        for pc in evt.get("price_changes", []):
            pc_aid = pc.get("asset_id", "")
            if not pc_aid: continue
            best_ask = float(pc["best_ask"]) if pc.get("best_ask") else None
            best_bid = float(pc["best_bid"]) if pc.get("best_bid") else None
            old = _ws_cache.get(pc_aid)
            old_ask_d = old[1] if old else 0
            old_bid_d = old[3] if old else 0
            _ws_cache[pc_aid] = (best_ask, old_ask_d, best_bid, old_bid_d, time.time())
        return

    if not aid: return

    if event_type == "last_trade_price":
        price = float(evt.get("price", 0))
        side = evt.get("side", "")
        old = _ws_cache.get(aid)
        if old:
            if side == "BUY":
                _ws_cache[aid] = (old[0], old[1], price, old[3], time.time())
            else:
                _ws_cache[aid] = (price, old[1], old[2], old[3], time.time())
        return

    # Book event
    bids = evt.get("bids", [])
    asks = evt.get("asks", [])
    best_ask = None; ask_depth = 0
    for a in asks:
        p, s = float(a.get("price", 0)), float(a.get("size", 0))
        if p > 0 and s > 0:
            if best_ask is None or p < best_ask: best_ask = p
            ask_depth += s * p
    best_bid = None; bid_depth = 0
    for b in bids:
        p, s = float(b.get("price", 0)), float(b.get("size", 0))
        if p > 0 and s > 0:
            if best_bid is None or p > best_bid: best_bid = p
            bid_depth += s * p
    _ws_cache[aid] = (best_ask, round(ask_depth, 2), best_bid, round(bid_depth, 2), time.time())


async def ws_connection(tokens_chunk, conn_id):
    """Single WS connection for up to 500 tokens."""
    my_count = len(tokens_chunk)
    events_processed = 0

    while _ws_running:
        try:
            async with websockets.connect(WS_URI, ping_interval=None, ping_timeout=None) as ws:
                sub_msg = json.dumps({"type": "market", "assets_ids": list(tokens_chunk)})
                await ws.send(sub_msg)
                log.info(f"  🔌 WS[{conn_id}] subscribed {my_count} tokens")

                async for raw in ws:
                    if not _ws_running:
                        break
                    if raw == "ping":
                        await ws.send("pong")
                        continue
                    if raw == "pong":
                        continue

                    try:
                        data = json.loads(raw)
                        if isinstance(data, list):
                            for evt in data:
                                await _process_ws_event(evt)
                        else:
                            await _process_ws_event(data)
                        events_processed += 1
                        if events_processed % 5000 == 0:
                            log.info(f"  📡 WS[{conn_id}] {events_processed} events")
                    except json.JSONDecodeError:
                        pass

                    # Subscribe new tokens (conn 0 only)
                    if conn_id == 0 and _ws_new_tokens and my_count < WS_BATCH_SIZE:
                        batch = []
                        with _lock:
                            while _ws_new_tokens and my_count < WS_BATCH_SIZE:
                                batch.append(_ws_new_tokens.pop(0))
                                my_count += 1
                        if batch:
                            add_msg = json.dumps({"type": "market", "assets_ids": batch})
                            await ws.send(add_msg)
                            log.info(f"  🔌 WS[{conn_id}] +{len(batch)} tokens (total: {my_count})")

        except Exception as e:
            log.info(f"  🔌 WS[{conn_id}] reconnecting ({type(e).__name__}: {e})")
            await asyncio.sleep(3)


async def ws_main():
    """Launch WS connections."""
    # Wait for initial market discovery
    while not _markets and _ws_running:
        await asyncio.sleep(1)

    with _lock:
        all_tokens = []
        for mid, mkt in _markets.items():
            all_tokens.append(mkt["tok_no"])
            all_tokens.append(mkt["tok_yes"])

    chunks = [all_tokens[i:i+WS_BATCH_SIZE] for i in range(0, len(all_tokens), WS_BATCH_SIZE)]
    log.info(f"  🔌 WS launching {len(chunks)} connection(s) for {len(all_tokens)} tokens")
    tasks = [asyncio.create_task(ws_connection(chunk, i)) for i, chunk in enumerate(chunks)]
    await asyncio.gather(*tasks)


def ws_thread_fn():
    """Run WS event loop in a background thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(ws_main())
    except Exception as e:
        log.error(f"WS thread error: {e}")


# ═══════════════════════════════════════════════════════════════
# SNAPSHOT & RESOLUTION
# ═══════════════════════════════════════════════════════════════

def take_snapshot(db_conn):
    """Record current prices for all tracked markets."""
    now = time.time()
    rows = []

    with _lock:
        for mid, mkt in _markets.items():
            no_data = _ws_cache.get(mkt["tok_no"], (None, 0, None, 0, 0))
            yes_data = _ws_cache.get(mkt["tok_yes"], (None, 0, None, 0, 0))

            # Skip if no data yet
            if no_data[0] is None and no_data[2] is None and yes_data[0] is None and yes_data[2] is None:
                continue

            rows.append((
                now, mid,
                no_data[2] or 0, no_data[0] or 0, no_data[1] or 0,  # no_bid, no_ask, no_depth
                yes_data[2] or 0, yes_data[0] or 0, yes_data[1] or 0,  # yes_bid, yes_ask, yes_depth
            ))

    if rows:
        db_conn.executemany(
            "INSERT INTO ticks (ts, mid, no_bid, no_ask, no_depth, yes_bid, yes_ask, yes_depth) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        db_conn.commit()

    return len(rows)


def check_resolutions(db_conn):
    """Detect markets that have resolved (one side near 0, other near 1)."""
    resolved = 0

    with _lock:
        for mid, mkt in list(_markets.items()):
            no_data = _ws_cache.get(mkt["tok_no"], (None, 0, None, 0, 0))
            yes_data = _ws_cache.get(mkt["tok_yes"], (None, 0, None, 0, 0))

            no_bid = no_data[2] or 0
            yes_bid = yes_data[2] or 0

            if no_bid <= 0 and yes_bid <= 0:
                continue

            winner = None
            if no_bid >= 0.95 and yes_bid <= 0.05:
                winner = "NO"
            elif yes_bid >= 0.95 and no_bid <= 0.05:
                winner = "YES"

            if winner:
                try:
                    db_conn.execute(
                        "UPDATE markets SET resolved_at=?, winner=?, no_bid_final=?, yes_bid_final=? "
                        "WHERE mid=? AND resolved_at=0",
                        (time.time(), winner, no_bid, yes_bid, mid))
                    db_conn.commit()
                    log.info(f"  ✅ RESOLVED: {mkt['title'][:50]} → {winner} won "
                            f"(NO={no_bid:.2f} YES={yes_bid:.2f})")
                    resolved += 1
                except sqlite3.Error:
                    pass

    return resolved


# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════

def main():
    global _ws_running

    print("=" * 70)
    print("  POLYMARKET SPORTS DATA COLLECTOR")
    print(f"  Database: {DB_FILE}")
    print(f"  Snapshot interval: {SNAPSHOT_INTERVAL}s")
    print("=" * 70)

    db_conn = init_db()

    # Graceful shutdown
    def shutdown(sig, frame):
        global _ws_running
        log.info("\n🛑 Shutting down gracefully...")
        _ws_running = False
        db_conn.close()
        sys.exit(0)
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Initial market discovery
    log.info("🔍 Discovering sports markets...")
    new = discover_markets(db_conn)
    log.info(f"  Found {new} new sports markets ({len(_markets)} total)")

    # Start WebSocket thread
    ws_thread = threading.Thread(target=ws_thread_fn, daemon=True)
    ws_thread.start()
    log.info("🔌 WebSocket thread started")

    # Wait for some WS data to arrive
    log.info("⏳ Waiting for price data...")
    time.sleep(5)

    # Main loop
    last_discovery = time.time()
    last_snapshot = 0
    last_resolution = 0
    cycle = 0
    total_ticks = 0
    total_resolutions = 0

    while _ws_running:
        try:
            now = time.time()

            # Market discovery
            if now - last_discovery >= DISCOVERY_INTERVAL:
                new = discover_markets(db_conn)
                if new > 0:
                    log.info(f"🔍 Discovered {new} new markets ({len(_markets)} total)")
                last_discovery = now

            # Price snapshot
            if now - last_snapshot >= SNAPSHOT_INTERVAL:
                n_ticks = take_snapshot(db_conn)
                total_ticks += n_ticks
                last_snapshot = now
                cycle += 1

                # Status update every 20 cycles (~10 min)
                if cycle % 20 == 0:
                    # Count resolved in DB
                    try:
                        res_count = db_conn.execute(
                            "SELECT COUNT(*) FROM markets WHERE resolved_at > 0").fetchone()[0]
                        tick_count = db_conn.execute(
                            "SELECT COUNT(*) FROM ticks").fetchone()[0]
                    except:
                        res_count = 0; tick_count = 0

                    log.info(f"📊 Cycle {cycle} | {len(_markets)} markets | "
                            f"{n_ticks} ticks/snap | {tick_count} total ticks | "
                            f"{res_count} resolved | cache={len(_ws_cache)} tokens")

            # Resolution check
            if now - last_resolution >= RESOLUTION_CHECK:
                n_res = check_resolutions(db_conn)
                total_resolutions += n_res
                last_resolution = now

            time.sleep(1)

        except KeyboardInterrupt:
            break
        except Exception as e:
            log.error(f"Main loop error: {e}")
            time.sleep(5)

    log.info("🛑 Collector stopped")
    db_conn.close()


if __name__ == "__main__":
    main()
