#!/usr/bin/env python3
"""Dump live market titles to understand current format."""
import json, time
from urllib.request import urlopen, Request
from urllib.error import HTTPError

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

def api_get(url, timeout=15):
    for attempt in range(3):
        try:
            req = Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Accept": "application/json",
            })
            resp = urlopen(req, timeout=timeout)
            return json.loads(resp.read().decode("utf-8"))
        except HTTPError as e:
            print(f"  HTTP {e.code} on attempt {attempt+1}: {url[:80]}")
            if e.code == 429:
                time.sleep(3)
            elif e.code == 403:
                time.sleep(1)
            else:
                return None
        except Exception as e:
            print(f"  Error: {e}")
            time.sleep(1)
    return None

# Try Gamma API
print("=== TRYING GAMMA API ===")
data = api_get(f"{GAMMA}/markets?limit=50&offset=0&active=true&closed=false")
if data:
    print(f"Got {len(data)} markets from Gamma")
    for m in data[:20]:
        t = m.get("question", m.get("title", ""))
        cat = m.get("category", "")
        print(f"  [{cat:15s}] {t[:90]}")
else:
    print("Gamma API failed, trying CLOB...")

# Try CLOB markets endpoint
print("\n=== TRYING CLOB API ===")
data2 = api_get(f"{CLOB}/markets?next_cursor=MA==")
if data2:
    markets = data2 if isinstance(data2, list) else data2.get("data", data2.get("markets", []))
    if isinstance(markets, list):
        print(f"Got {len(markets)} markets from CLOB")
        for m in markets[:30]:
            t = m.get("question", m.get("title", ""))
            cid = m.get("condition_id", "")[:16]
            cat = m.get("category", m.get("market_type", ""))
            print(f"  [{cat:15s}] [{cid}] {t[:85]}")
    else:
        print(f"Unexpected format: {type(markets)}")
        print(json.dumps(data2, indent=2)[:500])
else:
    print("CLOB API also failed")

# Try events endpoint
print("\n=== TRYING GAMMA EVENTS ===")
data3 = api_get(f"{GAMMA}/events?limit=30&active=true&closed=false")
if data3:
    events = data3 if isinstance(data3, list) else data3.get("data", [])
    print(f"Got {len(events)} events")
    for ev in events[:20]:
        title = ev.get("title", ev.get("question", ""))
        cat = ev.get("category", "")
        markets = ev.get("markets", [])
        print(f"\n  [{cat:12s}] EVENT: {title[:70]}")
        if isinstance(markets, list):
            for mk in markets[:5]:
                mt = mk.get("question", mk.get("title", ""))
                mid = mk.get("id", "")[:12]
                clob_ids = mk.get("clobTokenIds", [])
                print(f"    [{mid}] {mt[:75]}")
            if len(markets) > 5:
                print(f"    ... and {len(markets)-5} more markets")
else:
    print("Events API failed")

# Also check: what does the bot's existing market registry look like?
print("\n=== CHECKING WHAT arb_v5 FINDS ===")
# The bot uses WebSocket and REST polling, let's check the REST endpoint it uses
data4 = api_get(f"{CLOB}/sampling-markets?next_cursor=MA==")
if data4:
    markets = data4 if isinstance(data4, list) else data4.get("data", [])
    if isinstance(markets, list):
        print(f"Got {len(markets)} sampling markets")
        for m in markets[:15]:
            t = m.get("question", m.get("title", ""))
            tokens = m.get("tokens", [])
            print(f"  {t[:80]}")
            if isinstance(tokens, list):
                for tok in tokens[:3]:
                    tid = tok.get("token_id", "")[-12:]
                    outcome = tok.get("outcome", "")
                    print(f"    [{outcome}] ...{tid}")
    else:
        print(f"Format: {json.dumps(data4, indent=2)[:300]}")
else:
    print("Sampling markets failed")
