"""
Polymarket Position Manager
View all open positions and selectively close them out.
"""
import os, sys, json, time, requests
from dotenv import load_dotenv

load_dotenv("keys.env")

PRIVATE_KEY   = os.getenv("PK", "")
PROXY_WALLET  = os.getenv("PROXY_WALLET", "").lower()
LOGIN_METHOD  = os.getenv("LOGIN_METHOD", "email")
CLOB          = "https://clob.polymarket.com"
GAMMA         = "https://gamma-api.polymarket.com"
DATA          = "https://data-api.polymarket.com"
HEADERS       = {"Accept": "application/json", "User-Agent": "PolyArb/2.0"}

_session = requests.Session()
_session.headers.update(HEADERS)
_client  = None

def get_client():
    global _client
    if _client:
        return _client
    from py_clob_client.client import ClobClient
    sig = {"email": 1, "magic": 1, "metamask": 2, "coinbase": 2}.get(LOGIN_METHOD, 1)
    _client = ClobClient(CLOB, key=PRIVATE_KEY, chain_id=137,
                         signature_type=sig, funder=PROXY_WALLET)
    _client.set_api_creds(_client.create_or_derive_api_creds())
    return _client

def get_positions():
    """Fetch all open positions from the data API."""
    url = f"{DATA}/positions?user={PROXY_WALLET}&sizeThreshold=0.01"
    try:
        r = _session.get(url, timeout=15)
        return r.json()
    except Exception as e:
        print(f"Error fetching positions: {e}")
        return []

def get_best_ask(token_id):
    """Get current best ask price for a token."""
    try:
        r = _session.get(f"{CLOB}/price?token_id={token_id}&side=SELL", timeout=10)
        data = r.json()
        return float(data.get("price", 0))
    except:
        return 0.0

def get_best_bid(token_id):
    """Get current best bid (what you'd sell at)."""
    try:
        r = _session.get(f"{CLOB}/price?token_id={token_id}&side=BUY", timeout=10)
        data = r.json()
        return float(data.get("price", 0))
    except:
        return 0.0

def get_market_title(condition_id):
    """Get market title from Gamma."""
    try:
        r = _session.get(f"{GAMMA}/markets", params={"conditionId": condition_id}, timeout=10)
        data = r.json()
        if data and isinstance(data, list) and len(data) > 0:
            m = data[0]
            return m.get("question") or m.get("title") or condition_id[:20]
    except:
        pass
    return condition_id[:16] + "..."

def sell_position(token_id, shares, label=""):
    """Sell (close) a position by selling shares."""
    from py_clob_client.clob_types import MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import SELL
    try:
        client = get_client()
        # Sell by share count
        mo     = MarketOrderArgs(token_id=token_id, amount=shares, side=SELL)
        signed = client.create_market_order(mo)
        resp   = client.post_order(signed, OrderType.FOK)
        if resp and resp.get("success"):
            print(f"  ✅ SOLD {label}")
            return True
        else:
            print(f"  ❌ Failed: {resp}")
            return False
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return False

def main():
    if not PROXY_WALLET:
        print("ERROR: Set PROXY_WALLET in keys.env")
        sys.exit(1)

    print(f"\n{'='*70}")
    print(f"  Position Manager — Wallet: {PROXY_WALLET[:10]}...")
    print(f"{'='*70}\n")
    print("Fetching positions...")

    positions = get_positions()
    if not positions:
        print("No open positions found.")
        return

    # Enrich with current prices
    print(f"Found {len(positions)} positions. Fetching current prices...\n")

    enriched = []
    for p in positions:
        token_id   = p.get("asset") or p.get("token_id") or p.get("tokenId", "")
        size       = float(p.get("size") or p.get("currentSize") or 0)
        avg_price  = float(p.get("avgPrice") or p.get("buyPrice") or 0)
        outcome    = p.get("outcome") or p.get("side") or "?"
        cid        = p.get("conditionId") or p.get("market") or ""

        if size < 0.01:
            continue

        # Get current bid (sell price)
        bid = get_best_bid(token_id)
        cost   = avg_price * size
        value  = bid * size
        pnl    = value - cost
        pnl_pct = (pnl / cost * 100) if cost > 0 else 0

        title = get_market_title(cid) if cid else "Unknown"

        enriched.append({
            "token_id": token_id,
            "size":     size,
            "avg_price": avg_price,
            "bid":      bid,
            "cost":     cost,
            "value":    value,
            "pnl":      pnl,
            "pnl_pct":  pnl_pct,
            "outcome":  outcome,
            "title":    title,
            "cid":      cid,
        })
        time.sleep(0.1)  # rate limit

    if not enriched:
        print("No positions with meaningful size found.")
        return

    # Sort by P&L
    enriched.sort(key=lambda x: x["pnl"])

    # Display
    print(f"{'#':<3} {'Market':<40} {'Out':<4} {'Size':>6} {'Avg':>6} {'Bid':>6} {'Cost':>7} {'Value':>7} {'P&L':>8}  Action")
    print("-" * 110)

    for i, p in enumerate(enriched):
        # Determine recommended action
        if p["bid"] >= 0.69:
            action = "SELL (likely winner - lock profit)"
        elif p["avg_price"] < 0.30 and p["bid"] > p["avg_price"] * 1.2:
            action = "SELL (up from entry)"
        elif p["bid"] < 0.10:
            action = "SELL (near worthless)"
        elif p["pnl_pct"] < -20:
            action = "SELL (cut loss)"
        else:
            action = "hold"

        pnl_str = f"${p['pnl']:+.2f}"
        flag = "🔴" if p["pnl"] < -0.50 else "🟡" if p["pnl"] < 0 else "🟢"

        print(f"{i+1:<3} {p['title'][:40]:<40} {p['outcome']:<4} {p['size']:>6.1f} "
              f"{p['avg_price']:>6.3f} {p['bid']:>6.3f} ${p['cost']:>6.2f} ${p['value']:>6.2f} "
              f"{flag}{pnl_str:>7}  {action}")

    # Summary
    total_cost  = sum(p["cost"]  for p in enriched)
    total_value = sum(p["value"] for p in enriched)
    total_pnl   = total_value - total_cost
    print("-" * 110)
    print(f"{'TOTAL':<48} ${total_cost:>6.2f} ${total_value:>6.2f} {'$'+f'{total_pnl:+.2f}':>8}")

    # Interactive close
    print(f"\nOptions:")
    print(f"  Enter position number to sell that position")
    print(f"  'all-losses'  — sell everything with P&L < -$0.10")
    print(f"  'all-winners' — sell everything with bid >= 0.69")
    print(f"  'recommended' — sell all positions marked SELL above")
    print(f"  'quit'        — exit without trading\n")

    while True:
        cmd = input("Command: ").strip().lower()

        if cmd == "quit" or cmd == "q":
            print("Exiting.")
            break

        elif cmd == "cleanup":
            # Sell: near-worthless, big losses, low entries that went up
            # KEEP: anything with bid >= 0.69 (let them resolve at 1.0)
            targets = [p for p in enriched if
                       p["bid"] < 0.75 and (          # don't touch likely winners
                           p["bid"] < 0.15 or         # near worthless
                           p["pnl_pct"] < -25 or      # big loser
                           (p["avg_price"] < 0.35 and p["bid"] > p["avg_price"] * 1.15)  # low entry, went up
                       )]
            keepers = [p for p in enriched if p["bid"] >= 0.69]
            print(f"\nWill SELL {len(targets)} positions, KEEP {len(keepers)} winners (bid >= 0.69)")
            print(f"Estimated recovery: ${sum(p['value'] for p in targets):.2f}")
            print(f"Estimated losses crystallized: ${sum(p['pnl'] for p in targets):.2f}")
            confirm = input("Confirm? (y/n): ")
            if confirm.lower() == "y":
                for p in targets:
                    sell_position(p["token_id"], p["size"],
                                  f"{p['title'][:30]} {p['outcome']} @ {p['bid']:.3f}")
                    time.sleep(0.3)
                print(f"\nDone. Keeping {len(keepers)} positions with bid >= 0.69:")
                for p in keepers:
                    print(f"  🏆 {p['title'][:40]} {p['outcome']} @ {p['bid']:.3f} (${p['value']:.2f})")

        elif cmd == "all-losses":
            targets = [p for p in enriched if p["pnl"] < -0.10]
            print(f"Will sell {len(targets)} losing positions...")
            for p in targets:
                sell_position(p["token_id"], p["size"],
                              f"{p['title'][:30]} {p['outcome']} @ {p['bid']:.3f}")
                time.sleep(0.3)

        elif cmd == "all-winners":
            targets = [p for p in enriched if p["bid"] >= 0.69]
            print(f"Will sell {len(targets)} winning positions...")
            for p in targets:
                sell_position(p["token_id"], p["size"],
                              f"{p['title'][:30]} {p['outcome']} @ {p['bid']:.3f}")
                time.sleep(0.3)

        elif cmd == "recommended":
            targets = [p for p in enriched if "SELL" in p.get("action_label", "")]
            # Re-compute
            targets = [p for p in enriched if
                       p["bid"] >= 0.69 or
                       (p["avg_price"] < 0.30 and p["bid"] > p["avg_price"] * 1.2) or
                       p["bid"] < 0.10 or
                       p["pnl_pct"] < -20]
            print(f"Will sell {len(targets)} positions...")
            for p in targets:
                sell_position(p["token_id"], p["size"],
                              f"{p['title'][:30]} {p['outcome']} @ {p['bid']:.3f}")
                time.sleep(0.3)

        elif cmd.isdigit():
            idx = int(cmd) - 1
            if 0 <= idx < len(enriched):
                p = enriched[idx]
                confirm = input(f"Sell {p['size']:.1f} shares of {p['title'][:40]} {p['outcome']} @ {p['bid']:.3f}? (y/n): ")
                if confirm.lower() == "y":
                    sell_position(p["token_id"], p["size"],
                                  f"{p['title'][:30]} {p['outcome']}")
            else:
                print("Invalid number.")
        else:
            print("Unknown command.")

if __name__ == "__main__":
    main()
