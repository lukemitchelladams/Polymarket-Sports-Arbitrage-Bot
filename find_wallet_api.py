#!/usr/bin/env python3
"""Find the correct Polymarket API endpoint for wallet positions."""
import json
from urllib.request import urlopen, Request

W = "0xEa5Dd6558179b0af0089EBda038603725f3aD375"
WL = W.lower()

endpoints = [
    # Gamma API variations
    f"https://gamma-api.polymarket.com/positions?user={WL}&limit=10",
    f"https://gamma-api.polymarket.com/positions?address={WL}&limit=10",
    f"https://gamma-api.polymarket.com/positions?wallet={WL}&limit=10",
    f"https://gamma-api.polymarket.com/users/{WL}/positions",
    f"https://gamma-api.polymarket.com/activity?user={WL}&limit=5",
    f"https://gamma-api.polymarket.com/trade-history?user={WL}&limit=5",
    # Data API
    f"https://data-api.polymarket.com/positions?user={WL}",
    f"https://data-api.polymarket.com/positions?address={WL}",
    f"https://data-api.polymarket.com/trade-history?user={WL}&limit=5",
    # CLOB
    f"https://clob.polymarket.com/data/positions?user={WL}",
    f"https://clob.polymarket.com/positions?user={WL}",
    # Profile
    f"https://gamma-api.polymarket.com/users/{WL}",
    f"https://polymarket.com/api/positions?user={WL}",
    f"https://lb-api.polymarket.com/positions?user={WL}",
    # Subgraph / other
    f"https://strapi-matic.polymarket.com/positions?user={WL}",
]

print(f"Testing endpoints for wallet: {W[:12]}...\n")

for url in endpoints:
    short = url.replace("https://","").replace(WL, "WALLET")[:70]
    try:
        req = Request(url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
        resp = urlopen(req, timeout=10)
        code = resp.getcode()
        raw = resp.read().decode("utf-8")
        data = json.loads(raw)
        
        if isinstance(data, list):
            n = len(data)
            if n > 0:
                keys = list(data[0].keys())[:8]
                print(f"✅ [{code}] {short}")
                print(f"   {n} items | keys: {keys}")
                print(f"   sample: {json.dumps(data[0], default=str)[:150]}")
                print()
            else:
                print(f"⚪ [{code}] {short} → empty list")
        elif isinstance(data, dict):
            keys = list(data.keys())[:8]
            # Check nested
            found = False
            for k in ['data','positions','results','items','history']:
                if k in data and isinstance(data[k], list) and len(data[k]) > 0:
                    print(f"✅ [{code}] {short}")
                    print(f"   {k}: {len(data[k])} items | keys: {list(data[k][0].keys())[:8]}")
                    print(f"   sample: {json.dumps(data[k][0], default=str)[:150]}")
                    print()
                    found = True
                    break
            if not found:
                print(f"⚪ [{code}] {short} → {keys}")
        else:
            print(f"⚪ [{code}] {short} → {type(data).__name__}")
    except Exception as e:
        err = str(e)[:60]
        print(f"❌ {short} → {err}")

print("\nDone! Copy any ✅ lines and send to Claude.")
