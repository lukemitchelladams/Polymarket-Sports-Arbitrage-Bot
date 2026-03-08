#!/usr/bin/env python3
"""Test actual order posting with each signature type."""
import os, json, requests

with open("keys.env") as f:
    for line in f:
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ[k.strip()] = v.strip()

pk = os.getenv("PK")
pw = os.getenv("PROXY_WALLET")

# Find a real token to test with
print("Finding a real market token...")
r = requests.get("https://gamma-api.polymarket.com/markets?limit=1&active=true&closed=false&order=volume24hr&ascending=false")
m = r.json()[0]
clob_ids = m.get("clobTokenIds", [])
if isinstance(clob_ids, str):
    clob_ids = json.loads(clob_ids)
token_id = str(clob_ids[1])  # NO token
title = m.get("question", "")[:50]
print(f"  Market: {title}")
print(f"  Token: {token_id[:20]}...")
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
        
        # Try to create and post a tiny GTC order at $0.01 (won't fill)
        args = OrderArgs(
            token_id=token_id,
            price=0.01, size=1.0, side="BUY"
        )
        signed = c.create_order(args)
        print(f"  Signing: OK")
        
        resp = c.post_order(signed, orderType="GTC")
        print(f"  Post order: OK! Response: {resp}")
        
        # Cancel it immediately
        if resp and resp.get("orderID"):
            try:
                c.cancel(resp["orderID"])
                print(f"  Cancelled test order")
            except:
                print(f"  (cancel failed, order at $0.01 won't fill anyway)")
    except Exception as e:
        print(f"  ERROR: {e}")
    print()
