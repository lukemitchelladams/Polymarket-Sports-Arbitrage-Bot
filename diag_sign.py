#!/usr/bin/env python3
"""Detailed signing diagnostic."""
import os

with open("keys.env") as f:
    for line in f:
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

pk = os.getenv("PK")
pw = os.getenv("PROXY_WALLET")
print(f"PK: {pk[:6]}...{pk[-4:]}")
print(f"PROXY_WALLET: {pw}")
print()

from py_clob_client import ClobClient, OrderArgs

for sig_type in [0, 1, 2]:
    print(f"--- sig_type={sig_type} ---")
    try:
        kw = {"key": pk, "chain_id": 137, "signature_type": sig_type}
        if sig_type == 2:
            kw["funder"] = pw
        c = ClobClient("https://clob.polymarket.com", **kw)
        creds = c.create_or_derive_api_creds()
        c.set_api_creds(creds)
        print(f"  Creds: OK")
        
        # Try signing
        test_args = OrderArgs(
            token_id="0" * 64,
            price=0.50, size=1.0, side="BUY"
        )
        signed = c.create_order(test_args)
        print(f"  Signing: OK (type={type(signed).__name__})")
    except Exception as e:
        print(f"  ERROR: {e}")
    print()

# Also check if we can get tick size info
print("--- Checking API access ---")
try:
    import requests
    r = requests.get("https://clob.polymarket.com/tick-size?token_id=" + "0" * 64)
    print(f"  Tick size API: {r.status_code} {r.text[:100]}")
except Exception as e:
    print(f"  API error: {e}")
