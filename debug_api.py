#!/usr/bin/env python3
"""Quick diagnostic: what does the API actually return for known-closed markets?"""
import json, os, sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LEDGER_FILE = os.path.join(SCRIPT_DIR, "ledger.json")
GAMMA = "https://gamma-api.polymarket.com"

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

try:
    import httpx
    _s = httpx.Client(timeout=15, follow_redirects=True)
    def http_get(url, params=None):
        r = _s.get(url, params=params or {})
        print(f"    HTTP {r.status_code} for {url[:80]}")
        return r.json() if r.status_code == 200 else None
except ImportError:
    import urllib.request, urllib.parse
    def http_get(url, params=None):
        if params: url += "?" + urllib.parse.urlencode(params)
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "PolyArb/1.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
                print(f"    HTTP 200 for {url[:80]}")
                return data
        except Exception as e:
            print(f"    HTTP FAIL for {url[:80]}: {e}")
            return None

with open(LEDGER_FILE) as f:
    ledger = json.load(f)

# Find known-closed markets
targets = []
for mid, pos in ledger.items():
    if pos.get("resolved"): continue
    title = pos.get("title", "")
    if any(kw in title.lower() for kw in ["iran", "bitcoin up or down", "nuggets vs. thunder", "cavaliers vs. pistons"]):
        targets.append((mid, title))

if not targets:
    print("No target markets found in ledger")
    sys.exit(1)

for mid, title in targets[:5]:
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"  condition_id: {mid}")
    print(f"  (length: {len(mid)})")
    
    # Method 1: direct lookup
    print(f"\n  Method 1: GET /markets/{mid}")
    data = http_get(f"{GAMMA}/markets/{mid}")
    if data:
        if isinstance(data, dict):
            for key in ["closed", "resolved", "active", "acceptingOrders", "endDate", 
                        "resolution", "outcomePrices", "condition_id", "question",
                        "winner", "winningOutcome"]:
                if key in data:
                    val = data[key]
                    if isinstance(val, str) and len(val) > 80:
                        val = val[:80] + "..."
                    print(f"    {key}: {val}")
            if not data.get("condition_id") and not data.get("question"):
                print(f"    *** Returned data has no condition_id or question! Keys: {list(data.keys())[:10]}")
        elif isinstance(data, list):
            print(f"    Returned a LIST with {len(data)} items")
            if data:
                print(f"    First item keys: {list(data[0].keys())[:10]}")
    else:
        print(f"    Returned None")

    # Method 2: query by condition_id
    print(f"\n  Method 2: GET /markets?condition_id={mid[:40]}...")
    data2 = http_get(f"{GAMMA}/markets", params={"condition_id": mid})
    if data2:
        if isinstance(data2, list):
            print(f"    Returned list with {len(data2)} items")
            for item in data2[:2]:
                for key in ["closed", "resolved", "active", "endDate", "question", "condition_id"]:
                    if key in item:
                        val = item[key]
                        if isinstance(val, str) and len(val) > 60:
                            val = val[:60] + "..."
                        print(f"      {key}: {val}")
        elif isinstance(data2, dict):
            print(f"    Returned dict with keys: {list(data2.keys())[:10]}")
    else:
        print(f"    Returned None")

    # Method 3: try slug-based lookup
    print(f"\n  Method 3: GET /markets?slug=... (skip)")
    
    # Show raw condition_id from ledger
    print(f"\n  Raw mid repr: {repr(mid)}")
