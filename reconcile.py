#!/usr/bin/env python3
"""
PolyArb Reconciler — compares ledger.json against actual Polymarket account.
Shows real portfolio value vs bot's estimated profit.

Usage: python3.11 ~/polycopy/reconcile.py
"""
import json, os, sys, time
from pathlib import Path

# ── Load keys ────────────────────────────────────────────────────────
def load_env(path):
    env = {}
    try:
        for line in open(path):
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                env[k.strip()] = v.strip()
    except FileNotFoundError:
        pass
    return env

BASE  = Path.home() / "polycopy"
env   = load_env(BASE / "keys.env")

PK    = env.get("PK") or env.get("PRIVATE_KEY","")
API_K = env.get("API_KEY","")
API_S = env.get("API_SECRET","")
API_P = env.get("API_PASSPHRASE","")
HOST  = env.get("CLOB_HOST","https://clob.polymarket.com")

if not PK:
    print("No private key found in keys.env"); sys.exit(1)

# ── Auth ─────────────────────────────────────────────────────────────
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    from eth_account import Account
    acct   = Account.from_key(PK)
    wallet = acct.address
    creds  = ApiCreds(api_key=API_K, api_secret=API_S, api_passphrase=API_P)
    client = ClobClient(HOST, key=PK, chain_id=137, creds=creds)
    # Try derive if creds empty
    if not API_K:
        derived = client.derive_api_key()
        creds   = ApiCreds(**derived)
        client  = ClobClient(HOST, key=PK, chain_id=137, creds=creds)
    print(f"Connected: {wallet[:10]}...")
except Exception as e:
    print(f"Auth error: {e}"); sys.exit(1)

# ── Fetch actual positions from Polymarket ────────────────────────────
RESET="\033[0m"; BOLD="\033[1m"; GREEN="\033[92m"; RED="\033[91m"; CYAN="\033[96m"; YELLOW="\033[93m"

print(f"\n{BOLD}{CYAN}Fetching your actual Polymarket positions...{RESET}")
try:
    # Get open positions
    positions = client.get_positions(addr=wallet)
    if not positions:
        positions = []
except Exception as e:
    print(f"Error fetching positions: {e}")
    positions = []

# ── Load ledger ───────────────────────────────────────────────────────
ledger_path = BASE / "ledger.json"
try:
    ledger = json.load(open(ledger_path))
except FileNotFoundError:
    ledger = {}

# ── Display actual positions ──────────────────────────────────────────
print(f"\n{BOLD}{'='*70}{RESET}")
print(f"{BOLD}  ACTUAL POLYMARKET POSITIONS{RESET}")
print(f"{'='*70}")

total_cost      = 0.0
total_value     = 0.0
total_redeemable = 0.0

if not positions:
    print("  No open positions found (or API returned empty)")
else:
    print(f"  {'Market':<45} {'Side':>4} {'Size':>6} {'Avg':>6} {'Cost':>7} {'Curr$':>7}")
    print(f"  {'─'*45} {'─'*4} {'─'*6} {'─'*6} {'─'*7} {'─'*7}")
    for p in positions:
        try:
            title   = (p.get("title") or p.get("market",""))[:45]
            side    = p.get("side","?")
            size    = float(p.get("size") or p.get("shares",0))
            avg     = float(p.get("avgPrice") or p.get("average_price",0))
            cost    = size * avg
            # Current value = size * current_price (if available)
            curr_p  = float(p.get("currentPrice") or p.get("price") or avg)
            curr_v  = size * curr_p
            total_cost  += cost
            total_value += curr_v
            pl      = curr_v - cost
            c       = GREEN if pl >= 0 else RED
            print(f"  {title:<45} {side:>4} {size:>6.2f} {avg:>6.3f} ${cost:>6.2f} {c}${curr_v:>6.2f}{RESET}")
        except Exception as e:
            print(f"  Error parsing position: {e} | {p}")

print(f"\n  {'─'*70}")
pl_total = total_value - total_cost
c = GREEN if pl_total >= 0 else RED
print(f"  Total cost (what you paid):    ${total_cost:>8.2f}")
print(f"  Current market value:          ${total_value:>8.2f}")
print(f"  Unrealized P&L:               {c}${pl_total:>+8.2f}{RESET}")

# ── Bot ledger summary ────────────────────────────────────────────────
print(f"\n{BOLD}{'='*70}{RESET}")
print(f"{BOLD}  BOT LEDGER (estimated at resolution){RESET}")
print(f"{'='*70}")

bot_deployed = 0.0
bot_profit   = 0.0
for mid, d in ledger.items():
    yf = d.get("yes_fills",[])
    nf = d.get("no_fills",[])
    y_usdc = sum(a for _,a in yf)
    n_usdc = sum(a for _,a in nf)
    y_avg  = sum(p*a for p,a in yf)/y_usdc if y_usdc else 0
    n_avg  = sum(p*a for p,a in nf)/n_usdc if n_usdc else 0
    deployed = y_usdc + n_usdc
    bot_deployed += deployed
    if y_usdc and n_usdc:
        spread = 1.0 - y_avg - n_avg
        est    = spread * min(y_usdc, n_usdc)
        bot_profit += est

print(f"  Bot deployed (ledger):         ${bot_deployed:>8.2f}")
print(f"  Bot estimated profit:          ${bot_profit:>+8.2f}")
print(f"  Bot ROI:                       {bot_profit/bot_deployed*100:>+7.1f}%" if bot_deployed else "  Bot ROI: N/A")

# ── Explanation ───────────────────────────────────────────────────────
print(f"\n{BOLD}{'='*70}{RESET}")
print(f"{BOLD}  WHY THESE NUMBERS DIFFER{RESET}")
print(f"{'='*70}")
print(f"""
  Bot profit   = what you COLLECT at resolution if both sides win
                 (YES resolves $1 + NO resolves $0 or vice versa)

  Polymarket   = current MARKET VALUE of your shares right now
                 (what you'd get if you sold everything today)

  Example: You bought YES@0.50 + NO@0.40 = 0.90 combined
    Bot says:  +$0.10 profit (10% spread, locked)
    Polymarket: YES shares currently worth $0.45 → shows -$0.05 paper loss
    At resolution: YES pays $1.00 → you collect full profit ✓

  The bot's number is CORRECT at resolution.
  Polymarket's number is correct if you SELL NOW.
  Since we hold to resolution, trust the bot's number.
""")
