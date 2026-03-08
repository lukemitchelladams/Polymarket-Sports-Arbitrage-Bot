#!/usr/bin/env python3
"""
Quick diagnostic — tests the Gamma API and market filter.
Run this standalone to see what's going on.
"""
import json, re, time
from urllib.request import urlopen, Request
from datetime import datetime, timezone

GAMMA = "https://gamma-api.polymarket.com"

def api_get(url, timeout=15):
    try:
        req = Request(url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
        return json.loads(urlopen(req, timeout=timeout).read().decode("utf-8"))
    except Exception as e:
        print(f"  ERROR: {e}")
        return None

MAX_DAYS_OUT = 7

def is_sports_market(title, end_date_str=""):
    if not title: return False, "empty"
    tl = title.lower()
    has_vs = bool(re.search(r'\bvs\.?\b', tl))
    has_win_date = bool(re.match(r'will .+ win on \d{4}-\d{2}-\d{2}', tl))
    has_ou = bool(re.search(r'o/u\s+\d', tl))
    has_draw = 'end in a draw' in tl
    has_btts = 'both teams to score' in tl
    has_spread = bool(re.search(r'spread\s+[+-]?\d', tl))
    has_esports = bool(re.search(r'(game \d+ winner|map \d+ winner|set \d+ winner)', tl))
    
    if not (has_vs or has_win_date or has_ou or has_draw or has_btts or has_spread or has_esports):
        return False, "no_pattern"
    if re.search(r'\d{4}[–-]\d{2}(?!\s*-\d{2})\b', tl):
        return False, "season_futures"
    if re.search(r'by (january|february|march|april|may|june|july|august|september|october|november|december) \d{4}', tl):
        return False, "by_month"
    if re.search(r'before \d{4}', tl):
        return False, "before_year"
    REJECT_KW = ['bitcoin','crypto','xrp','ethereum','solana','token','dip to',
        'all time high','acquired','federal','embassy','democrat','republican',
        'election','senate','governor','congress','president','openai','google',
        'apple','microsoft','amazon','tesla','stock','reserve','price by',
        'market cap','ceo ','ipo ','sec ','fed ','nato ','ukraine','russia',
        'china ','israel','palestine','war ','nuclear','ai ','gpt','chatgpt',
        'supreme court','legislation','bill ','act ','tariff','immigration']
    for kw in REJECT_KW:
        if kw in tl:
            return False, f"keyword:{kw}"
    if has_win_date:
        m = re.search(r'(\d{4}-\d{2}-\d{2})', title)
        if m:
            try:
                from datetime import timedelta
                mkt_date = datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                days_out = (mkt_date - now).days
                if days_out > MAX_DAYS_OUT or days_out < -1:
                    return False, f"date_too_far:{days_out}d"
            except: pass
    if end_date_str:
        try:
            end = datetime.fromisoformat(end_date_str.replace("Z","+00:00"))
            now = datetime.now(timezone.utc)
            days_out = (end - now).days
            if days_out > MAX_DAYS_OUT or days_out < -1:
                return False, f"enddate_too_far:{days_out}d"
        except: pass
    return True, "accepted"

print("="*70)
print("POLYMARKET API DIAGNOSTIC")
print("="*70)

# Test 1: Basic API connectivity
print("\n1. TESTING API CONNECTIVITY...")
data = api_get(f"{GAMMA}/markets?limit=5&active=true")
if data:
    print(f"   ✅ API works! Got {len(data)} markets")
    if data:
        print(f"   Sample: {data[0].get('question','')[:60]}")
else:
    print("   ❌ API FAILED — check internet connection")
    exit(1)

# Test 2: What tags return data?
print("\n2. TESTING TAGS...")
tags = ["sports", "esports", "soccer", "tennis", "basketball", "baseball",
        "hockey", "football", "cricket", "mma"]
for tag in tags:
    data = api_get(f"{GAMMA}/markets?limit=10&active=true&closed=false&tag={tag}", timeout=8)
    if data and len(data) > 0:
        print(f"   ✅ tag={tag}: {len(data)} markets")
        print(f"      → {data[0].get('question','')[:55]}")
    else:
        print(f"   ❌ tag={tag}: 0 markets (tag may not exist)")
    time.sleep(0.1)

# Test 3: Untagged markets — what titles exist?
print("\n3. ALL ACTIVE MARKETS (first 200, showing titles)...")
all_titles = []
for offset in [0, 100]:
    data = api_get(f"{GAMMA}/markets?limit=100&offset={offset}&active=true&closed=false")
    if data:
        for m in data:
            title = m.get("question", "") or m.get("title", "")
            end_date = m.get("endDate", m.get("end_date_iso", ""))
            clob_ids = m.get("clobTokenIds", [])
            if isinstance(clob_ids, str):
                try: clob_ids = json.loads(clob_ids)
                except: clob_ids = []
            has_clob = isinstance(clob_ids, list) and len(clob_ids) >= 2
            all_titles.append((title, end_date, has_clob))

print(f"   Fetched {len(all_titles)} market titles")

# Test 4: Run filter on all titles
print("\n4. FILTER RESULTS...")
accepted = []
rejected_reasons = {}
for title, end_date, has_clob in all_titles:
    if not has_clob:
        rejected_reasons["no_clob"] = rejected_reasons.get("no_clob", 0) + 1
        continue
    ok, reason = is_sports_market(title, end_date)
    if ok:
        accepted.append(title)
    else:
        rejected_reasons[reason] = rejected_reasons.get(reason, 0) + 1

print(f"\n   ACCEPTED: {len(accepted)}")
for t in accepted[:15]:
    print(f"     ✅ {t[:65]}")
if len(accepted) > 15:
    print(f"     ... and {len(accepted)-15} more")

print(f"\n   REJECTED REASONS:")
for reason, count in sorted(rejected_reasons.items(), key=lambda x: -x[1]):
    print(f"     {reason:<25s}: {count}")

# Show some rejected titles for debugging
print(f"\n   SAMPLE REJECTED (no_pattern):")
for title, end_date, has_clob in all_titles[:100]:
    if not has_clob: continue
    ok, reason = is_sports_market(title, end_date)
    if not ok and reason == "no_pattern":
        print(f"     ✗ {title[:65]}")

print(f"\n   SAMPLE REJECTED (enddate_too_far):")
for title, end_date, has_clob in all_titles[:200]:
    if not has_clob: continue
    ok, reason = is_sports_market(title, end_date)
    if not ok and "too_far" in reason:
        print(f"     ✗ [{reason}] {title[:55]}")

print(f"\n{'='*70}")
print(f"SUMMARY: {len(accepted)} sports markets out of {len(all_titles)} total")
if len(accepted) == 0:
    print("⚠ ZERO ACCEPTED — filter is too strict or API doesn't have sports markets")
    print("  Try relaxing the filter or checking if Polymarket has active sports")
print(f"{'='*70}")
