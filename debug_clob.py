#!/usr/bin/env python3
"""Diagnostic: try CLOB API and book checks for known-closed markets."""
import json, os, sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LEDGER_FILE = os.path.join(SCRIPT_DIR, "ledger.json")
CLOB = "https://clob.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

for fname in ["keys.env", ".env"]:
    fpath = os.path.join(SCRIPT_DIR, fname)
    if not os.path.exists(fpath): continue
    with open(fpath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            if line.startswith("export "): line = line[7:]
            if "=" not in line: continue
            k, v = line.split("=", 1)
            os.environ[k] = v.strip().strip('"').strip("'")

PROXY_WALLET = os.getenv("PROXY_WALLET", "")

try:
    import httpx
    _s = httpx.Client(timeout=15, follow_redirects=True)
    def http_get(url, params=None):
        r = _s.get(url, params=params or {})
        return r.status_code, (r.json() if r.status_code == 200 else r.text[:200])
except ImportError:
    import urllib.request, urllib.parse
    def http_get(url, params=None):
        if params: url += "?" + urllib.parse.urlencode(params)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "PolyArb/1.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                return 200, json.loads(r.read())
        except Exception as e:
            return 0, str(e)[:200]

with open(LEDGER_FILE) as f:
    ledger = json.load(f)

# Target known-closed markets
targets = []
for mid, pos in ledger.items():
    if pos.get("resolved"): continue
    title = pos.get("title", "")
    if any(kw in title.lower() for kw in ["iran", "bitcoin up or down", "nuggets vs. thunder", "cavaliers vs. pistons"]):
        targets.append((mid, pos))

if not targets:
    print("No target markets found")
    sys.exit(1)

for mid, pos in targets[:4]:
    title = pos.get("title", "")
    yes_tok = pos.get("yes_token", "")
    no_tok = pos.get("no_token", "")
    
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"  condition_id: {mid}")
    print(f"  yes_token: {yes_tok[:40]}...")
    print(f"  no_token:  {no_tok[:40]}...")

    # Test 1: CLOB /markets/{condition_id}
    print(f"\n  [1] CLOB /markets/{{cid}}")
    code, data = http_get(f"{CLOB}/markets/{mid}")
    print(f"      HTTP {code}")
    if code == 200 and isinstance(data, dict):
        for k in ["condition_id", "closed", "active", "end_date_iso", 
                   "accepting_orders", "minimum_tick_size", "question"]:
            if k in data:
                print(f"      {k}: {data[k]}")
    else:
        print(f"      Response: {str(data)[:150]}")

    # Test 2: CLOB book for YES token
    if yes_tok:
        print(f"\n  [2] CLOB /book YES token")
        code, data = http_get(f"{CLOB}/book", params={"token_id": yes_tok})
        print(f"      HTTP {code}")
        if code == 200 and isinstance(data, dict):
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            print(f"      bids: {len(bids)}, asks: {len(asks)}")
            if bids: print(f"      top bid: {bids[0]}")
            if asks: print(f"      top ask: {asks[0]}")
            if not bids and not asks:
                print(f"      *** EMPTY BOOK — market likely closed ***")

    # Test 3: CLOB book for NO token
    if no_tok:
        print(f"\n  [3] CLOB /book NO token")
        code, data = http_get(f"{CLOB}/book", params={"token_id": no_tok})
        print(f"      HTTP {code}")
        if code == 200 and isinstance(data, dict):
            bids = data.get("bids", [])
            asks = data.get("asks", [])
            print(f"      bids: {len(bids)}, asks: {len(asks)}")
            if bids: print(f"      top bid: {bids[0]}")
            if asks: print(f"      top ask: {asks[0]}")
            if not bids and not asks:
                print(f"      *** EMPTY BOOK — market likely closed ***")

    # Test 4: data-api position
    if PROXY_WALLET:
        print(f"\n  [4] data-api position value")
        code, data = http_get(f"{DATA_API}/positions", params={
            "user": PROXY_WALLET.lower(),
            "market": mid,
        })
        print(f"      HTTP {code}")
        if code == 200 and isinstance(data, list):
            print(f"      {len(data)} positions found")
            for p in data[:3]:
                print(f"      outcome={p.get('outcome','')} size={p.get('size','')} "
                      f"value={p.get('currentValue', p.get('value',''))} "
                      f"price={p.get('avgPrice','')}")
        else:
            print(f"      Response: {str(data)[:150]}")

    # Test 5: CLOB /neg-risk/markets endpoint
    print(f"\n  [5] CLOB /neg-risk/markets/{{cid}}")
    code, data = http_get(f"{CLOB}/neg-risk/markets/{mid}")
    print(f"      HTTP {code}")
    if code == 200 and isinstance(data, dict):
        for k in list(data.keys())[:10]:
            v = data[k]
            if isinstance(v, str) and len(v) > 60: v = v[:60] + "..."
            print(f"      {k}: {v}")
    else:
        print(f"      Response: {str(data)[:150]}")
