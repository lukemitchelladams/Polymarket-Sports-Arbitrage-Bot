#!/usr/bin/env python3
"""Check where funds are."""
import os, requests

with open("keys.env") as f:
    for line in f:
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

pk = os.getenv("PK")
pw = os.getenv("PROXY_WALLET")

# Derive EOA address from private key
from eth_account import Account
acct = Account.from_key(pk)
eoa = acct.address
print(f"EOA address (from PK): {eoa}")
print(f"PROXY_WALLET:          {pw}")
print(f"Same address? {eoa.lower() == pw.lower()}")
print()

# Check USDC balance on both via Polygon RPC
USDC = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # USDC on Polygon
USDC_E = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # USDC native

for label, addr in [("EOA", eoa), ("PROXY", pw)]:
    for token_label, token in [("USDC.e", USDC), ("USDC", USDC_E)]:
        # balanceOf(address) = 0x70a08231
        data = "0x70a08231" + addr.lower().replace("0x", "").zfill(64)
        payload = {
            "jsonrpc": "2.0", "method": "eth_call",
            "params": [{"to": token, "data": data}, "latest"],
            "id": 1
        }
        try:
            r = requests.post("https://polygon-rpc.com", json=payload, timeout=10)
            result = r.json().get("result", "0x0")
            bal = int(result, 16) / 1e6  # USDC has 6 decimals
            if bal > 0:
                print(f"  {label} {token_label}: ${bal:,.2f}")
        except:
            pass

print()
print("If funds are in PROXY but bot uses EOA (sig_type=0),")
print("the bot can't access them. We'd need sig_type=2 to work,")
print("or move funds to EOA.")
