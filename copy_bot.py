"""
PolyCopy Bot — Auto Copy Trader
================================
Monitors a target wallet on Polymarket and automatically mirrors
every trade they make using your own wallet.

Setup:
  1. pip install py-clob-client web3==6.14.0 python-dotenv requests
  2. Fill in keys.env with your details
  3. Set DRY_RUN=true first to test without real money
  4. Run: python copy_bot.py

How it works:
  - Polls data-api.polymarket.com/trades every POLL_INTERVAL seconds
  - When a new trade from the target wallet is detected, it:
      1. Looks up the market and token ID
      2. Places a matching market order for BET_SIZE_USDC
  - All activity is logged to copy_bot.log and printed to console
"""

import os
import sys
import time
import logging
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

# ── Load config ───────────────────────────────────────────────────────────────
load_dotenv("keys.env")

PRIVATE_KEY       = os.getenv("PK", "")
PROXY_WALLET      = os.getenv("PROXY_WALLET", "")
LOGIN_METHOD      = os.getenv("LOGIN_METHOD", "email").lower()
TARGET_WALLET     = os.getenv("TARGET_WALLET", "0x2005d16a84ceefa912d4e380cd32e7ff827875ea")
BET_SIZE_USDC     = float(os.getenv("BET_SIZE_USDC", "10"))
MAX_BET_USDC      = float(os.getenv("MAX_BET_USDC", "100"))
MIN_COPY_USDC     = float(os.getenv("MIN_COPY_USDC", "5"))
POLL_INTERVAL     = float(os.getenv("POLL_INTERVAL", "2"))
MAX_OPEN_POSITIONS= int(os.getenv("MAX_OPEN_POSITIONS", "20"))
DRY_RUN           = os.getenv("DRY_RUN", "true").lower() == "true"

HOST     = "https://clob.polymarket.com"
DATA_API = "https://data-api.polymarket.com"
CHAIN_ID = 137  # Polygon mainnet

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("copy_bot.log"),
    ]
)
log = logging.getLogger("PolyCopy")

# ── Validate config ───────────────────────────────────────────────────────────
def validate_config():
    errors = []
    if not PRIVATE_KEY or PRIVATE_KEY == "your_private_key_here_without_0x_prefix":
        errors.append("PK not set in keys.env")
    if not PROXY_WALLET or PROXY_WALLET.startswith("0x_your"):
        errors.append("PROXY_WALLET not set in keys.env")
    if not TARGET_WALLET.startswith("0x"):
        errors.append("TARGET_WALLET must be a 0x address")
    if BET_SIZE_USDC < 1:
        errors.append("BET_SIZE_USDC must be at least $1")
    if errors:
        for e in errors:
            log.error(f"Config error: {e}")
        sys.exit(1)

# ── Init Polymarket CLOB client ───────────────────────────────────────────────
def init_client():
    try:
        from py_clob_client.client import ClobClient
    except ImportError:
        log.error("py-clob-client not installed. Run: pip install py-clob-client web3==6.14.0")
        sys.exit(1)

    pk = PRIVATE_KEY if PRIVATE_KEY.startswith("0x") else f"0x{PRIVATE_KEY}"

    try:
        if LOGIN_METHOD == "email":
            # Email / Magic wallet login
            client = ClobClient(
                HOST, key=pk, chain_id=CHAIN_ID,
                signature_type=1, funder=PROXY_WALLET
            )
        elif LOGIN_METHOD == "metamask" or LOGIN_METHOD == "coinbase":
            # Browser wallet / MetaMask
            client = ClobClient(
                HOST, key=pk, chain_id=CHAIN_ID,
                signature_type=2, funder=PROXY_WALLET
            )
        else:
            # EOA — direct wallet, no proxy
            client = ClobClient(HOST, key=pk, chain_id=CHAIN_ID, signature_type=0)

        client.set_api_creds(client.create_or_derive_api_creds())
        log.info(f"✓ CLOB client initialized ({LOGIN_METHOD} wallet)")
        return client

    except Exception as e:
        log.error(f"Failed to init CLOB client: {e}")
        log.error("Check your private key and proxy wallet address in keys.env")
        sys.exit(1)

# ── API helpers ───────────────────────────────────────────────────────────────
SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json", "User-Agent": "PolyCopy/1.0"})

def api_get(url, params=None, retries=3):
    for i in range(retries):
        try:
            r = SESSION.get(url, params=params, timeout=10)
            if r.status_code == 429:
                wait = 5 * (i + 1)
                log.warning(f"Rate limited — waiting {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if i < retries - 1:
                time.sleep(1)
            else:
                log.warning(f"API error {url}: {e}")
    return None

def get_recent_trades(wallet, limit=20):
    """Fetch the most recent trades for a wallet."""
    data = api_get(f"{DATA_API}/trades", {
        "user":   wallet,
        "limit":  limit,
    })
    if not data:
        return []
    return data if isinstance(data, list) else []

def get_token_id_for_market(condition_id, outcome):
    """
    Look up the token ID for a specific outcome in a market.
    Token IDs are needed to place orders on the CLOB.
    """
    data = api_get("https://gamma-api.polymarket.com/markets", {
        "condition_ids": condition_id,
        "limit": 1,
    })
    if not data:
        return None
    markets = data if isinstance(data, list) else data.get("markets", [])
    if not markets:
        return None

    market = markets[0]
    tokens = market.get("tokens", []) or []
    outcome_upper = outcome.upper()

    for tok in tokens:
        if not isinstance(tok, dict):
            continue
        tok_outcome = (tok.get("outcome") or "").upper()
        if tok_outcome == outcome_upper:
            return tok.get("token_id") or tok.get("tokenId")

    # Fallback: try clobTokenIds from market
    clob_ids = market.get("clobTokenIds") or []
    if isinstance(clob_ids, str):
        import json
        try:
            clob_ids = json.loads(clob_ids)
        except Exception:
            clob_ids = []

    # YES = index 0, NO = index 1
    if "YES" in outcome_upper and len(clob_ids) > 0:
        return clob_ids[0]
    if "NO" in outcome_upper and len(clob_ids) > 1:
        return clob_ids[1]

    return None

def get_my_open_position_count():
    """Count our current open positions to respect MAX_OPEN_POSITIONS."""
    data = api_get(f"{DATA_API}/positions", {
        "user":          PROXY_WALLET,
        "sizeThreshold": 1,
        "limit":         500,
    })
    if not data:
        return 0
    positions = data if isinstance(data, list) else []
    active = [p for p in positions
              if not p.get("redeemable")
              and 0.04 < float(p.get("curPrice") or 0) < 0.96]
    return len(active)

# ── Place copy order ──────────────────────────────────────────────────────────
def place_copy_order(client, trade, token_id):
    """
    Place a market order matching the target's trade.
    Uses FOK (Fill or Kill) so it either fills immediately or cancels.
    """
    from py_clob_client.clob_types import MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL

    side_str = (trade.get("side") or "BUY").upper()
    side     = BUY if side_str == "BUY" else SELL

    # Cap bet size
    bet = min(BET_SIZE_USDC, MAX_BET_USDC)

    if DRY_RUN:
        log.info(
            f"[DRY RUN] Would place {side_str} ${bet:.2f} on token {token_id[:16]}... "
            f"| Market: {trade.get('title', '?')[:50]} | Outcome: {trade.get('outcome','?')}"
        )
        return {"status": "dry_run", "bet": bet}

    try:
        mo = MarketOrderArgs(
            token_id=token_id,
            amount=bet,
            side=side,
        )
        signed = client.create_market_order(mo)
        resp   = client.post_order(signed, OrderType.FOK)

        if resp and resp.get("success"):
            log.info(
                f"✓ ORDER PLACED  {side_str} ${bet:.2f} "
                f"| {trade.get('title','?')[:40]} | {trade.get('outcome','?')}"
            )
        else:
            log.warning(f"Order rejected: {resp}")

        return resp

    except Exception as e:
        log.error(f"Order failed: {e}")
        return None

# ── Main loop ─────────────────────────────────────────────────────────────────
def run():
    log.info("=" * 60)
    log.info("  PolyCopy Bot starting up")
    log.info("=" * 60)
    log.info(f"  Target wallet : {TARGET_WALLET}")
    log.info(f"  My wallet     : {PROXY_WALLET}")
    log.info(f"  Bet size      : ${BET_SIZE_USDC} USDC per trade")
    log.info(f"  Max bet       : ${MAX_BET_USDC} USDC")
    log.info(f"  Min copy size : ${MIN_COPY_USDC} USDC")
    log.info(f"  Poll interval : {POLL_INTERVAL}s")
    log.info(f"  DRY RUN       : {DRY_RUN}")
    log.info("=" * 60)

    if DRY_RUN:
        log.info("⚠  DRY RUN MODE — no real orders will be placed")
        log.info("   Change DRY_RUN=false in keys.env when ready to trade live")
        log.info("=" * 60)

    validate_config()
    client = init_client()

    # Seed with the current trade history so we don't copy old trades on startup
    log.info("Seeding known trade history (ignoring trades before now)...")
    initial_trades = get_recent_trades(TARGET_WALLET, limit=50)
    seen_tx_hashes = {t.get("transactionHash") or t.get("id") or str(i)
                      for i, t in enumerate(initial_trades)}
    log.info(f"✓ Seeded {len(seen_tx_hashes)} existing trades — now watching for new ones")

    open_positions = 0
    total_copied   = 0
    total_skipped  = 0
    start_time     = time.time()

    while True:
        try:
            trades = get_recent_trades(TARGET_WALLET, limit=30)

            new_trades = []
            for t in trades:
                tx_id = t.get("transactionHash") or t.get("id")
                if tx_id and tx_id not in seen_tx_hashes:
                    new_trades.append(t)
                    seen_tx_hashes.add(tx_id)

            if new_trades:
                log.info(f"⚡ {len(new_trades)} new trade(s) detected from target!")

                # Check our position count
                if MAX_OPEN_POSITIONS > 0:
                    open_positions = get_my_open_position_count()

                for trade in new_trades:
                    side        = (trade.get("side") or "").upper()
                    outcome     = trade.get("outcome") or ""
                    cond_id     = trade.get("conditionId") or ""
                    title       = (trade.get("title") or "?")[:60]
                    trade_size  = float(trade.get("usdcSize") or trade.get("size") or 0)
                    price       = float(trade.get("price") or 0)
                    timestamp   = trade.get("timestamp") or 0

                    log.info(
                        f"  → {side} {outcome} | {title} | "
                        f"${trade_size:.2f} @ {price:.3f}"
                    )

                    # ── Skip checks ──────────────────────────────────────────
                    if side not in ("BUY", "SELL"):
                        log.info(f"    Skipping — unknown side: {side}")
                        total_skipped += 1
                        continue

                    if trade_size < MIN_COPY_USDC:
                        log.info(f"    Skipping — trade too small (${trade_size:.2f} < ${MIN_COPY_USDC})")
                        total_skipped += 1
                        continue

                    if MAX_OPEN_POSITIONS > 0 and open_positions >= MAX_OPEN_POSITIONS:
                        log.warning(f"    Skipping — at max open positions ({MAX_OPEN_POSITIONS})")
                        total_skipped += 1
                        continue

                    if not cond_id:
                        log.warning(f"    Skipping — no conditionId in trade data")
                        total_skipped += 1
                        continue

                    # ── Get token ID ─────────────────────────────────────────
                    token_id = get_token_id_for_market(cond_id, outcome)
                    if not token_id:
                        log.warning(f"    Skipping — could not find token ID for {outcome} in {cond_id[:12]}...")
                        total_skipped += 1
                        continue

                    # ── Place the order ──────────────────────────────────────
                    result = place_copy_order(client, trade, token_id)
                    if result:
                        total_copied += 1
                        if not DRY_RUN:
                            open_positions += 1

                    # Small delay between orders to avoid rate limits
                    time.sleep(0.3)

            else:
                # Heartbeat every 30 seconds
                elapsed = time.time() - start_time
                if int(elapsed) % 30 == 0:
                    log.info(
                        f"  Watching... | Copied: {total_copied} | "
                        f"Skipped: {total_skipped} | "
                        f"Runtime: {int(elapsed//60)}m {int(elapsed%60)}s"
                    )

        except KeyboardInterrupt:
            log.info("\n⛔ Bot stopped by user")
            log.info(f"   Total copied : {total_copied}")
            log.info(f"   Total skipped: {total_skipped}")
            log.info(f"   Runtime      : {int((time.time()-start_time)//60)}m")
            break

        except Exception as e:
            log.error(f"Unexpected error: {e}")
            log.info("Continuing after 5 seconds...")
            time.sleep(5)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
