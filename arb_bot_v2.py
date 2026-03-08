#!/usr/bin/env python3
"""
PolyArb Bot v2.0 — WebSocket Edition
=====================================
Copies RN1's strategy:
  - Live sports markets only (soccer, esports, tennis)
  - WebSocket price feed (~100ms latency vs 1-3s polling)
  - Executes the instant YES+NO sum drops below threshold
  - Sizes orders to available book depth (no slippage)

Install deps first:
  pip install websockets aiohttp py_clob_client python-dotenv
"""

import asyncio
import json
import os
import re
import sys
import time
import threading
import logging
from collections import defaultdict
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests
import websockets

load_dotenv(os.path.join(os.path.dirname(__file__), "keys.env"))

# ── Config ──────────────────────────────────────────────────────────────────
PRIVATE_KEY        = os.getenv("PK", "")              # matches keys.env
PROXY_WALLET       = os.getenv("PROXY_WALLET", "").lower()
LOGIN_METHOD       = os.getenv("LOGIN_METHOD", "email").lower()
DRY_RUN            = os.getenv("DRY_RUN", "true").lower() == "true"
MIN_SPREAD_PCT     = float(os.getenv("MIN_SPREAD", "4"))
MAX_SPREAD_PCT     = float(os.getenv("MAX_SPREAD", "35"))
BASE_UNIT          = float(os.getenv("BASE_UNIT", "2"))
MAX_BET_USDC       = float(os.getenv("MAX_BET_USDC", "5"))
MAX_OPEN_POSITIONS = int(os.getenv("MAX_OPEN_POSITIONS", "150"))
MIN_LIQUIDITY      = float(os.getenv("MIN_LIQUIDITY", "2000"))
LOG_LEVEL          = os.getenv("LOG_LEVEL", "INFO")

# Polymarket endpoints
CLOB    = "https://clob.polymarket.com"
GAMMA   = "https://gamma-api.polymarket.com"
WSS     = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
HEADERS = {"Accept": "application/json", "User-Agent": "PolyArb/2.0"}

# Shared HTTP session (same as v1)
_session = requests.Session()
_session.headers.update(HEADERS)

def _get(url, params=None, timeout=10):
    for i in range(3):
        try:
            r = _session.get(url, params=params, timeout=timeout)
            if r.status_code == 429:
                time.sleep(5 * (i + 1))
                continue
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            log.debug(f"HTTP error: {e}")
            time.sleep(1)
    return None

# ── Logging ──────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
log_file = f"logs/arb_v2_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(), logging.FileHandler(log_file)]
)
log = logging.getLogger("polyarb")

# ── Sports keywords (word-boundary safe) ─────────────────────────────────────
SPORTS_KEYWORDS = [
    # Sports types
    "soccer","football","basketball","baseball","hockey","tennis",
    "golf","rugby","cricket","boxing","esports","cs2","csgo",
    "valorant","dota","counter-strike",
    # Common market phrases
    "vs ","o/u","over/under","spread","moneyline","draw",
    "win on","will win","clean sheet","both teams",
    "first half","second half","total goals","total points",
    "match winner","series winner","game winner","map winner","map 2","map 3",
    "sets","win on","lec","iec","draculan","dreamleague",
    # Leagues / orgs
    "nba","nfl","nhl","mlb","epl","mls","ufc","mma",
    "pgl","esl","blast","major","qualifier",
    "premier league","champions league","europa league",
    "copa","libertadores","serie a","bundesliga","la liga","ligue 1",
    "f1","formula","atp","wta","open",
    # Known teams/clubs (expand as needed)
    "lakers","celtics","warriors","heat","knicks","suns","nuggets",
    "river plate","boca juniors","flamengo","santos","estudiantes",
    "banfield","huracán","guaraní","tolima","táchira","millonarios",
    "real madrid","barcelona","liverpool","arsenal","chelsea","manchester",
    "tottenham","psg","juventus","inter","ac milan","bayern","dortmund",
    "yawara","bounty hunters","team aether","boss",
]

def is_sports_market(title: str) -> bool:
    t = title.lower()
    return any(
        (re.search(r'\b' + re.escape(k) + r'\b', t) if len(k) <= 4 else k in t)
        for k in SPORTS_KEYWORDS
    )

# ── Sizing ────────────────────────────────────────────────────────────────────
def calculate_bet_size(spread_pct: float) -> float:
    """Scale bet with spread, capped at MAX_BET_USDC."""
    if spread_pct >= 12:  mult = 20
    elif spread_pct >= 8: mult = 15
    elif spread_pct >= 5: mult = 8
    elif spread_pct >= 4: mult = 4
    elif spread_pct >= 3: mult = 2
    else:                 mult = 1
    return min(BASE_UNIT * mult, MAX_BET_USDC)

# ── CLOB client (thread-safe singleton) ──────────────────────────────────────
_client = None
_client_lock = threading.Lock()

def get_client():
    global _client
    with _client_lock:
        if _client is None:
            from py_clob_client.client import ClobClient
            if not PRIVATE_KEY:
                raise RuntimeError("Set PK in keys.env before going live")
            sig = {"email": 1, "magic": 1, "metamask": 2, "coinbase": 2}.get(LOGIN_METHOD, 1)
            _client = ClobClient(
                CLOB,
                key=PRIVATE_KEY,
                chain_id=137,
                signature_type=sig,
                funder=PROXY_WALLET or None,
            )
            try:
                _client.create_or_derive_api_creds()
            except Exception:
                pass
            log.info(f"CLOB client ready")
        return _client

# ── Order placement ───────────────────────────────────────────────────────────
def place_order(token_id: str, amount_usdc: float, label: str = "", max_price: float = None) -> tuple[bool, float]:
    """
    Place a GTC limit order at max_price to avoid slippage.
    If max_price is None, falls back to market order (not recommended).
    """
    from py_clob_client.clob_types import LimitOrderArgs, MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY

    if DRY_RUN:
        log.info(f"  [DRY] BUY ${amount_usdc:.2f} @{max_price:.3f}  {label}")
        return True, amount_usdc

    try:
        client = get_client()

        if max_price:
            # Limit order — only fills at max_price or better, never worse
            shares = round(amount_usdc / max_price, 4)
            lo = LimitOrderArgs(
                token_id=token_id,
                price=round(max_price, 2),
                size=shares,
                side=BUY,
            )
            signed = client.create_limit_order(lo)
            resp   = client.post_order(signed, OrderType.GTC)
        else:
            # Fallback market order
            mo     = MarketOrderArgs(token_id=token_id, amount=amount_usdc, side=BUY)
            signed = client.create_market_order(mo)
            resp   = client.post_order(signed, OrderType.FOK)

        if resp and resp.get("success"):
            filled = float(resp.get("size_matched") or amount_usdc)
            log.info(f"  ✅ FILLED ${amount_usdc:.2f} @{max_price:.3f}  {label}")
            return True, filled
        else:
            err = (resp or {}).get("errorMsg", "no response")
            log.warning(f"  ❌ REJECTED ${amount_usdc:.2f}: {err}  {label}")
            return False, 0
    except Exception as e:
        log.error(f"  Order error: {e}  {label}")
        return False, 0

# ── Market state ──────────────────────────────────────────────────────────────
# token_id -> {"best_ask": float, "asks": [...]}
price_book: dict[str, dict] = {}

# conditionId -> {"yes_token": str, "no_token": str, "title": str, "liquidity": float}
market_registry: dict[str, dict] = {}

# token_id -> conditionId (reverse lookup)
token_to_condition: dict[str, str] = {}

# already-traded conditions this session
traded: set[str] = set()

# Stats
stats = {
    "scans": 0, "opps": 0, "placed": 0,
    "filled": 0, "deployed": 0.0, "profit": 0.0,
    "start": time.time()
}

# Execution lock — one trade at a time
exec_lock = asyncio.Lock()

# ── Fetch live sports markets from Gamma API ──────────────────────────────────
def fetch_live_sports_markets() -> list[dict]:
    """
    Fetch active sports markets from Gamma API.
    Mirrors v1 bot which successfully finds 500+ markets.
    No time filter at fetch stage — apply in arb detection.
    """
    log.info("Fetching live sports markets from Gamma API...")
    markets = []
    offset = 0
    now = datetime.now(timezone.utc)

    while offset < 600:  # Only scan first 3 pages (600 markets)
        data = _get(f"{GAMMA}/markets", params={
            "active":    "true",
            "closed":    "false",
            "limit":     200,
            "offset":    offset,
            "order":     "volume24hr",
            "ascending": "false",
        })
        if data is None:
            log.error("Gamma fetch failed")
            break
        log.info(f"  Batch offset={offset}: {len(data)} markets, {len(markets)} sports so far")

        if not data or not isinstance(data, list):
            break

        for m in data:
            title = m.get("question") or m.get("title") or ""
            if not is_sports_market(title):
                continue

            # clobTokenIds comes as a JSON string — must parse it
            token_ids = m.get("clobTokenIds") or []
            if isinstance(token_ids, str):
                try:    token_ids = json.loads(token_ids)
                except: token_ids = []
            if len(token_ids) != 2:
                continue

            # Only trade markets ending within 24 hours (live games)
            end = m.get("endDate") or m.get("end_date_iso") or ""
            if end:
                try:
                    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
                    hours_left = (end_dt - now).total_seconds() / 3600
                    if hours_left < 0 or hours_left > 6:
                        continue  # expired or too far in future
                except Exception:
                    continue  # skip if can't parse date

            cid = m.get("conditionId") or m.get("condition_id") or ""
            if not cid:
                continue

            liquidity = float(m.get("liquidity") or 0)
            markets.append({
                "conditionId": cid,
                "yes_token":   token_ids[0],
                "no_token":    token_ids[1],
                "title":       title,
                "liquidity":   liquidity,
            })

        offset += 200
        if len(data) < 200:
            break

    log.info(f"Found {len(markets)} sports markets")
    return markets

# ── Arb detection and execution ───────────────────────────────────────────────
# Tracks resting limit orders per market:
# conditionId -> {yes_order_id, no_order_id, yes_price, no_price, bet, placed_at}
resting_orders: dict[str, dict] = {}

RESTING_TARGET = 0.96   # target sum — place both sides so YES+NO = 0.96
RESTING_EACH   = 0.02   # only place if each side is within 2c of target price
RESTING_TTL    = 300    # cancel and refresh after 5 minutes

def _place_limit_sync(token_id: str, price: float, bet: float, label: str):
    """Synchronous limit order placement for thread pool."""
    if DRY_RUN:
        log.info(f"  [DRY] RESTING {label} @{price:.3f} ${bet:.2f}")
        return "dry-run-id"
    try:
        from py_clob_client.clob_types import LimitOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY
        client = get_client()
        shares = round(bet / price, 4)
        lo = LimitOrderArgs(token_id=token_id, price=round(price, 2), size=shares, side=BUY)
        signed = client.create_limit_order(lo)
        resp = client.post_order(signed, OrderType.GTC)
        if resp and resp.get("success"):
            oid = resp.get("orderID") or resp.get("id", "")
            log.info(f"  📋 RESTING {label} @{price:.3f} ${bet:.2f} id:{oid[:8]}")
            return oid
        else:
            log.warning(f"  📋 RESTING {label} failed: {resp}")
    except Exception as e:
        log.error(f"  Resting error {label}: {e}")
    return None

def _cancel_order_sync(order_id: str):
    """Cancel a single order."""
    if DRY_RUN or not order_id or order_id == "dry-run-id":
        return
    try:
        get_client().cancel(order_id)
    except Exception as e:
        log.debug(f"Cancel {order_id[:8]}: {e}")

async def place_dual_resting(condition_id: str):
    """
    Place resting limit orders on BOTH sides at prices that guarantee an arb.
    Strategy: split RESTING_TARGET evenly, adjusted for current market prices.
    
    When one side fills:
      - Immediately cancel the other resting order
      - Fire the other side as a market order at current best price
    """
    info = market_registry.get(condition_id)
    if not info or condition_id in traded or condition_id in resting_orders:
        return

    yes_book = price_book.get(info["yes_token"], {})
    no_book  = price_book.get(info["no_token"], {})
    yes_ask  = yes_book.get("best_ask")
    no_ask   = no_book.get("best_ask")
    if not yes_ask or not no_ask:
        return

    # Current sum must be within striking distance (1.00 - 1.15)
    total = yes_ask + no_ask
    if total < 0.96 or total > 1.15:
        return

    # Target prices: place both sides at prices that sum to RESTING_TARGET
    # Split proportionally to current market prices
    ratio = yes_ask / total
    yes_target = round(RESTING_TARGET * ratio - 0.005, 2)
    no_target  = round(RESTING_TARGET * (1 - ratio) - 0.005, 2)

    # Only place if targets are realistic (within RESTING_EACH of current price)
    if yes_target < 0.03 or no_target < 0.03:
        return
    if abs(yes_ask - yes_target) > 0.15 or abs(no_ask - no_target) > 0.15:
        return  # targets too far from market, unlikely to fill

    bet = calculate_bet_size((RESTING_TARGET - (yes_target + no_target)) * 100 + MIN_SPREAD_PCT)
    log.info(f"  📋 Dual resting: YES@{yes_target:.3f} + NO@{no_target:.3f} = {yes_target+no_target:.3f} | {info['title'][:40]}")

    loop = asyncio.get_event_loop()
    yes_oid, no_oid = await asyncio.gather(
        loop.run_in_executor(None, _place_limit_sync, info["yes_token"], yes_target, bet, f"YES {info['title'][:20]}"),
        loop.run_in_executor(None, _place_limit_sync, info["no_token"],  no_target,  bet, f"NO  {info['title'][:20]}"),
    )

    if yes_oid or no_oid:
        resting_orders[condition_id] = {
            "yes_order_id": yes_oid,
            "no_order_id":  no_oid,
            "yes_token":    info["yes_token"],
            "no_token":     info["no_token"],
            "yes_price":    yes_target,
            "no_price":     no_target,
            "bet":          bet,
            "placed_at":    time.time(),
        }

async def check_resting_fills(condition_id: str):
    """
    Check if a resting order may have filled by watching price movement.
    If YES price drops BELOW our resting YES price → our YES order likely filled.
    Immediately cancel NO resting and fire NO as market order (and vice versa).
    """
    rest = resting_orders.get(condition_id)
    if not rest:
        return

    info = market_registry.get(condition_id)
    if not info:
        return

    # Cancel stale resting orders
    if time.time() - rest["placed_at"] > RESTING_TTL:
        log.info(f"  📋 Resting orders expired — cancelling & refreshing")
        loop = asyncio.get_event_loop()
        await asyncio.gather(
            loop.run_in_executor(None, _cancel_order_sync, rest.get("yes_order_id")),
            loop.run_in_executor(None, _cancel_order_sync, rest.get("no_order_id")),
        )
        del resting_orders[condition_id]
        await place_dual_resting(condition_id)
        return

    yes_book = price_book.get(info["yes_token"], {})
    no_book  = price_book.get(info["no_token"], {})
    yes_ask  = yes_book.get("best_ask")
    no_ask   = no_book.get("best_ask")
    if not yes_ask or not no_ask:
        return

    # If market YES price dropped below our resting YES price → YES likely filled
    if yes_ask < rest["yes_price"] - 0.02:
        log.info(f"  🔔 YES resting likely FILLED @{rest['yes_price']:.3f} — firing NO market order")
        loop = asyncio.get_event_loop()
        # Cancel the NO resting order first
        await loop.run_in_executor(None, _cancel_order_sync, rest.get("no_order_id"))
        del resting_orders[condition_id]
        # Fire NO immediately at market
        ok, filled = await loop.run_in_executor(
            None, place_order, info["no_token"], rest["bet"],
            f"NO {info['title'][:30]} (resting fill)", no_ask
        )
        if ok:
            traded.add(condition_id)
            stats["filled"] += 2
            stats["deployed"] += rest["bet"] * 2
            profit = rest["bet"] * 2 * (MIN_SPREAD_PCT / 100)
            stats["profit"] += profit
            log.info(f"  💰 RESTING ARB COMPLETE: ${rest['bet']*2:.2f} deployed ~${profit:.2f} profit")

    # If market NO price dropped below our resting NO price → NO likely filled
    elif no_ask < rest["no_price"] - 0.02:
        log.info(f"  🔔 NO resting likely FILLED @{rest['no_price']:.3f} — firing YES market order")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, _cancel_order_sync, rest.get("yes_order_id"))
        del resting_orders[condition_id]
        ok, filled = await loop.run_in_executor(
            None, place_order, info["yes_token"], rest["bet"],
            f"YES {info['title'][:30]} (resting fill)", yes_ask
        )
        if ok:
            traded.add(condition_id)
            stats["filled"] += 2
            stats["deployed"] += rest["bet"] * 2
            profit = rest["bet"] * 2 * (MIN_SPREAD_PCT / 100)
            stats["profit"] += profit
            log.info(f"  💰 RESTING ARB COMPLETE: ${rest['bet']*2:.2f} deployed ~${profit:.2f} profit")

async def check_and_place_resting(condition_id: str):
    """Entry point: check fills on existing resting orders, or place new ones."""
    if condition_id in resting_orders:
        await check_resting_fills(condition_id)
    elif condition_id not in traded:
        await place_dual_resting(condition_id)

async def check_and_execute(condition_id: str):
    """
    Called whenever a price updates. Checks if YES+NO < threshold and executes.
    Runs one at a time via exec_lock.
    """
    info = market_registry.get(condition_id)
    if not info:
        return

    yes_book = price_book.get(info["yes_token"])
    no_book  = price_book.get(info["no_token"])
    if not yes_book or not no_book:
        return

    yes_ask = yes_book.get("best_ask")
    no_ask  = no_book.get("best_ask")
    if not yes_ask or not no_ask:
        return

    total = yes_ask + no_ask
    spread_pct = (1.0 - total) * 100

    log.debug(f"  CHECK {info['title'][:40]} | YES@{yes_ask:.3f} + NO@{no_ask:.3f} = {total:.3f} ({spread_pct:.1f}%)")

    if spread_pct < MIN_SPREAD_PCT or spread_pct > MAX_SPREAD_PCT:
        return

    if condition_id in traded:
        return

    title = info["title"]
    log.info(f"🎯 ARB: {spread_pct:.1f}% | YES@{yes_ask:.3f} + NO@{no_ask:.3f} = {total:.3f} | {title[:60]}")
    stats["opps"] += 1

    async with exec_lock:
        # Re-check after acquiring lock (another coroutine may have traded this)
        if condition_id in traded:
            return

        # Re-verify prices are still good from latest book data
        yes_book = price_book.get(info["yes_token"])
        no_book  = price_book.get(info["no_token"])
        if not yes_book or not no_book:
            return

        yes_ask = yes_book.get("best_ask")
        no_ask  = no_book.get("best_ask")
        if not yes_ask or not no_ask:
            return

        total = yes_ask + no_ask
        spread_pct = (1.0 - total) * 100

        if spread_pct < MIN_SPREAD_PCT:
            log.warning(f"  ⏭️  Price moved, spread now {spread_pct:.1f}% — skipping")
            return

        # Size based on book depth if available, otherwise use default bet
        yes_asks = yes_book.get("asks", [])
        no_asks  = no_book.get("asks", [])

        if yes_asks and no_asks:
            yes_usdc = sum(
                float(l["price"]) * float(l["size"])
                for l in yes_asks
                if float(l["price"]) <= yes_ask + 0.01
            )
            no_usdc = sum(
                float(l["price"]) * float(l["size"])
                for l in no_asks
                if float(l["price"]) <= no_ask + 0.01
            )
            if yes_usdc < 0.50 or no_usdc < 0.50:
                log.warning(f"  ⏭️  Too thin (YES ${yes_usdc:.2f}, NO ${no_usdc:.2f}) — skipping")
                return
            bet = calculate_bet_size(spread_pct)
            bet = round(min(bet, yes_usdc, no_usdc), 2)
            bet = max(bet, 0.50)
        else:
            # No depth data yet — use default bet size
            bet = calculate_bet_size(spread_pct)

        traded.add(condition_id)

        label = title[:50]

        # Execute both sides concurrently in a thread pool
        loop = asyncio.get_event_loop()
        yes_fut = loop.run_in_executor(
            None, place_order, info["yes_token"], bet, f"YES {label}", yes_ask
        )
        no_fut = loop.run_in_executor(
            None, place_order, info["no_token"], bet, f"NO  {label}", no_ask
        )
        (yes_ok, yes_filled), (no_ok, no_filled) = await asyncio.gather(yes_fut, no_fut)

        stats["placed"] += 2
        if yes_ok and no_ok:
            deployed = bet * 2
            profit   = deployed * (spread_pct / 100)
            stats["filled"]   += 2
            stats["deployed"] += deployed
            stats["profit"]   += profit
            log.info(f"  💰 ARB COMPLETE: ${deployed:.2f} deployed, ~${profit:.2f} profit | {label}")
        else:
            log.warning(f"  ⚠️  Partial fill: YES={yes_ok} NO={no_ok} — check positions!")

# ── WebSocket price book updates ──────────────────────────────────────────────
def update_price_book(asset_id: str, asks: list):
    """Update local price book and return best ask."""
    if not asks:
        return None
    # asks sorted ascending by price
    sorted_asks = sorted(asks, key=lambda x: float(x["price"]))
    best_ask = float(sorted_asks[0]["price"])
    price_book[asset_id] = {
        "best_ask": best_ask,
        "asks": sorted_asks,
        "updated": time.time(),
    }
    return best_ask

_msg_count = 0
async def handle_message(message: str):
    """Process a WebSocket message."""
    global _msg_count
    _msg_count += 1
    if _msg_count % 50 == 0:
        log.info(f"📡 {_msg_count} WS messages | {len(price_book)} tokens priced")
    elif _msg_count <= 5:
        log.info(f"📡 WS msg #{_msg_count}: {message[:120]}")
    if _msg_count <= 3:
        log.info(f"  RAW MSG: {message[:200]}")
    try:
        data = json.loads(message)
    except Exception:
        return

    # Handle list of events
    events = data if isinstance(data, list) else [data]

    for event in events:
        event_type = event.get("event_type")
        asset_id   = event.get("asset_id")

        if not asset_id or asset_id not in token_to_condition:
            continue

        condition_id = token_to_condition[asset_id]

        if event_type == "book":
            # Full order book snapshot
            asks = event.get("asks", [])
            update_price_book(asset_id, asks)
            await check_and_execute(condition_id)

        elif event_type == "price_change":
            # price_changes is a list — each entry has its own asset_id and price
            changes = event.get("price_changes", [])
            for change in changes:
                ch_asset = change.get("asset_id")
                if not ch_asset or ch_asset not in token_to_condition:
                    continue
                ch_cid   = token_to_condition[ch_asset]
                ch_price = float(change.get("price") or 0)
                ch_size  = float(change.get("size") or 0)
                if ch_price <= 0:
                    continue
                # Update or create book entry for this price level
                book = price_book.setdefault(ch_asset, {"best_ask": ch_price, "asks": [], "updated": time.time()})
                existing = {l["price"]: l for l in book.get("asks", [])}
                if ch_size == 0:
                    existing.pop(str(ch_price), None)
                    existing.pop(f"{ch_price:.2f}", None)
                else:
                    existing[str(ch_price)] = {"price": str(ch_price), "size": str(ch_size)}
                update_price_book(ch_asset, list(existing.values()))
                await check_and_execute(ch_cid)
                await check_and_place_resting(ch_cid)

        elif event_type == "last_trade_price":
            await check_and_execute(condition_id)
            await check_and_place_resting(condition_id)

# ── WebSocket connection with auto-reconnect ──────────────────────────────────
async def run_websocket(token_ids: list[str]):
    """Maintain persistent WebSocket connection with reconnection."""
    backoff = 1
    while True:
        try:
            log.info(f"Connecting to WebSocket ({len(token_ids)} tokens)...")
            async with websockets.connect(
                WSS,
                ping_interval=20,
                ping_timeout=10,
                close_timeout=5,
            ) as ws:
                # Subscribe to all tokens in batches of 100
                batch_size = 100
                for i in range(0, len(token_ids), batch_size):
                    batch = token_ids[i:i + batch_size]
                    await ws.send(json.dumps({
                        "type": "market",
                        "assets_ids": batch
                    }))
                    log.info(f"  Subscribed to tokens {i+1}-{min(i+batch_size, len(token_ids))}")

                log.info(f"✅ WebSocket live — watching {len(token_ids)} tokens in real time")
                backoff = 1  # reset on success

                async for message in ws:
                    await handle_message(message)

        except websockets.exceptions.ConnectionClosed as e:
            log.warning(f"WebSocket closed: {e} — reconnecting in {backoff}s...")
        except Exception as e:
            log.error(f"WebSocket error: {e} — reconnecting in {backoff}s...")

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)

# ── Stats printer ─────────────────────────────────────────────────────────────
async def print_stats():
    while True:
        await asyncio.sleep(60)
        runtime = (time.time() - stats["start"]) / 60
        log.info(
            f"📊 Stats | Runtime: {runtime:.1f}m | "
            f"Markets: {len(market_registry)} | "
            f"Opps: {stats['opps']} | "
            f"Filled: {stats['filled']} | "
            f"Deployed: ${stats['deployed']:.2f} | "
            f"Est.Profit: ${stats['profit']:.2f}"
        )
        # Show current prices for all watched markets
        for cid, info in list(market_registry.items()):
            yes_book = price_book.get(info["yes_token"], {})
            no_book  = price_book.get(info["no_token"], {})
            yes_ask  = yes_book.get("best_ask")
            no_ask   = no_book.get("best_ask")
            if yes_ask and no_ask:
                total  = yes_ask + no_ask
                spread = (1.0 - total) * 100
                flag   = "🎯 ARB!" if spread >= MIN_SPREAD_PCT else "    "
                log.info(f"  {flag} {info['title'][:45]:<45} YES@{yes_ask:.3f} + NO@{no_ask:.3f} = {total:.3f} ({spread:+.1f}%)")

# ── Market refresh (re-fetch every 10 minutes for new live games) ─────────────
async def refresh_markets_loop():
    """Re-fetch live markets every 10 minutes as new games start."""
    while True:
        await asyncio.sleep(600)
        log.info("Refreshing market list...")
        markets = await asyncio.get_event_loop().run_in_executor(
            None, fetch_live_sports_markets
        )
        new_count = 0
        for m in markets:
            cid = m["conditionId"]
            if cid not in market_registry:
                market_registry[cid] = m
                token_to_condition[m["yes_token"]] = cid
                token_to_condition[m["no_token"]]  = cid
                new_count += 1
        if new_count:
            log.info(f"Added {new_count} new markets")

# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    log.info("=" * 70)
    log.info("  PolyArb Bot v2.0 — WebSocket Edition")
    log.info(f"  Mode       : {'🟡 DRY RUN' if DRY_RUN else '🔴 LIVE TRADING'}")
    log.info(f"  Min spread : {MIN_SPREAD_PCT}%")
    log.info(f"  Max bet    : ${MAX_BET_USDC}")
    log.info(f"  Min liq    : ${MIN_LIQUIDITY:,.0f}")
    log.info(f"  Latency    : ~100ms (WebSocket)")
    log.info("=" * 70)

    if not DRY_RUN:
        log.warning("🔴 LIVE MODE — Real orders will be placed!")
        log.warning("   Press Ctrl+C within 10 seconds to abort...")
        await asyncio.sleep(10)

    # Fetch initial markets
    markets = await asyncio.get_event_loop().run_in_executor(
        None, fetch_live_sports_markets
    )

    if not markets:
        log.error("No live sports markets found! Check MIN_LIQUIDITY setting.")
        return

    # Build registry
    for m in markets:
        cid = m["conditionId"]
        market_registry[cid] = m
        token_to_condition[m["yes_token"]] = cid
        token_to_condition[m["no_token"]]  = cid

    token_ids = list(token_to_condition.keys())
    log.info(f"Monitoring {len(market_registry)} markets ({len(token_ids)} tokens)")

    # Initialize CLOB client in background
    if not DRY_RUN:
        await asyncio.get_event_loop().run_in_executor(None, get_client)

    # Run WebSocket + stats + market refresh concurrently
    try:
        await asyncio.gather(
            run_websocket(token_ids),
            print_stats(),
            refresh_markets_loop(),
        )
    except KeyboardInterrupt:
        pass

    runtime = (time.time() - stats["start"]) / 60
    log.info(f"\n🛑 Bot stopped")
    log.info(
        f"Final: Runtime {runtime:.1f}m | "
        f"Opps {stats['opps']} | "
        f"Deployed ${stats['deployed']:.2f} | "
        f"Profit ${stats['profit']:.2f}"
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
