#!/usr/bin/env python3
"""Deep scan ALL markets, group by category, find triad-eligible sports."""
import json, time, re
from urllib.request import urlopen, Request
from urllib.error import HTTPError
from collections import defaultdict, Counter

def api_get(url, timeout=15):
    for attempt in range(3):
        try:
            req = Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                "Accept": "application/json",
            })
            return json.loads(urlopen(req, timeout=timeout).read().decode("utf-8"))
        except HTTPError as e:
            if e.code in (429, 403):
                time.sleep(2 ** attempt)
                continue
            return None
        except Exception:
            time.sleep(1)
            continue
    return None

print("Fetching ALL active markets (this takes ~30 seconds)...")

all_markets = []
offset = 0
limit = 100

while True:
    url = f"https://gamma-api.polymarket.com/markets?limit={limit}&offset={offset}&active=true&closed=false"
    data = api_get(url)
    if not data or not isinstance(data, list) or len(data) == 0:
        break
    all_markets.extend(data)
    print(f"  Fetched {len(all_markets)} markets...", end="\r")
    offset += limit
    time.sleep(0.3)
    if len(data) < limit:
        break

print(f"\nTotal active markets: {len(all_markets)}")

# Group by category
cats = Counter()
for m in all_markets:
    cat = m.get("category", "") or "uncategorized"
    cats[cat] += 1

print(f"\n{'='*60}")
print("CATEGORIES")
print(f"{'='*60}")
for cat, count in cats.most_common(30):
    print(f"  {cat:30s}: {count}")

# Find sports-related markets
sports_keywords = ['vs', 'win', 'draw', 'spread', 'o/u', 'over/under',
                   'nba', 'nfl', 'nhl', 'mlb', 'soccer', 'football',
                   'premier league', 'la liga', 'serie a', 'bundesliga',
                   'champions league', 'mls', 'epl', 'ligue 1',
                   'match', 'game', 'playoffs', 'championship']

sports_markets = []
for m in all_markets:
    title = (m.get("question", "") or m.get("title", "")).lower()
    cat = (m.get("category", "") or "").lower()
    if any(kw in title or kw in cat for kw in sports_keywords):
        sports_markets.append(m)

print(f"\n{'='*60}")
print(f"SPORTS-RELATED MARKETS: {len(sports_markets)}")
print(f"{'='*60}")

# Sub-categorize sports
sport_types = defaultdict(list)
for m in sports_markets:
    title = m.get("question", "") or m.get("title", "")
    tl = title.lower()
    
    if any(w in tl for w in ['nba', 'basketball']):
        sport_types["NBA"].append(title)
    elif any(w in tl for w in ['nfl', 'football', 'super bowl']):
        sport_types["NFL"].append(title)
    elif any(w in tl for w in ['nhl', 'hockey']):
        sport_types["NHL"].append(title)
    elif any(w in tl for w in ['mlb', 'baseball']):
        sport_types["MLB"].append(title)
    elif any(w in tl for w in ['soccer', 'premier league', 'la liga', 'serie a', 
                                'bundesliga', 'ligue 1', 'champions league', 
                                'epl', 'mls', 'world cup', 'fifa', 'fc ', ' fc',
                                'united', 'city', 'real madrid', 'barcelona']):
        sport_types["Soccer"].append(title)
    elif any(w in tl for w in ['counter-strike', 'cs2', 'dota', 'lol', 'valorant', 'esports']):
        sport_types["Esports"].append(title)
    elif 'draw' in tl:
        sport_types["Draw (potential soccer)"].append(title)
    elif 'vs' in tl or 'vs.' in tl:
        sport_types["Generic vs"].append(title)
    else:
        sport_types["Other sports"].append(title)

for sport, titles in sorted(sport_types.items(), key=lambda x: -len(x[1])):
    print(f"\n  {sport} ({len(titles)} markets):")
    for t in titles[:8]:
        print(f"    {t[:85]}")
    if len(titles) > 8:
        print(f"    ... and {len(titles)-8} more")

# Search for draw markets specifically
print(f"\n{'='*60}")
print("DRAW MARKETS (key for triads)")
print(f"{'='*60}")
draw_markets = [m for m in all_markets 
                if 'draw' in (m.get("question","") or m.get("title","")).lower()]
print(f"Total draw markets: {len(draw_markets)}")
for m in draw_markets[:15]:
    t = m.get("question", "") or m.get("title", "")
    print(f"  {t[:85]}")

# Search for "Will X win" markets
print(f"\n{'='*60}")
print("'WILL X WIN' MARKETS (key for triads)")
print(f"{'='*60}")
win_markets = [m for m in all_markets 
               if re.match(r'will .+ win', (m.get("question","") or m.get("title","")).lower())]
print(f"Total 'Will X win' markets: {len(win_markets)}")
for m in win_markets[:15]:
    t = m.get("question", "") or m.get("title", "")
    cat = m.get("category", "")
    print(f"  [{cat:12s}] {t[:80]}")
if len(win_markets) > 15:
    print(f"  ... and {len(win_markets)-15} more")

# Check for "X vs Y" format markets  
print(f"\n{'='*60}")
print("'X vs Y' MATCH MARKETS")
print(f"{'='*60}")
vs_markets = [m for m in all_markets 
              if re.search(r'\bvs\.?\b', (m.get("question","") or m.get("title","")), re.I)
              and not any(w in (m.get("question","") or m.get("title","")).lower() 
                         for w in ['gta', 'trump', 'bitcoin', 'album', 'openai'])]
print(f"Total 'vs' markets: {len(vs_markets)}")
for m in vs_markets[:20]:
    t = m.get("question", "") or m.get("title", "")
    cat = m.get("category", "")
    clob_ids = m.get("clobTokenIds", [])
    n_tokens = len(clob_ids) if isinstance(clob_ids, list) else 0
    print(f"  [{cat:12s}] [{n_tokens}tok] {t[:75]}")
if len(vs_markets) > 20:
    print(f"  ... and {len(vs_markets)-20} more")

# Now check EVENTS endpoint for sports
print(f"\n{'='*60}")
print("EVENTS (paginated)")
print(f"{'='*60}")

all_events = []
offset = 0
while len(all_events) < 500:
    url = f"https://gamma-api.polymarket.com/events?limit=50&offset={offset}&active=true&closed=false"
    data = api_get(url)
    if not data or not isinstance(data, list) or len(data) == 0:
        break
    all_events.extend(data)
    offset += 50
    time.sleep(0.3)

print(f"Total active events: {len(all_events)}")

# Find sports events
sports_events = []
for ev in all_events:
    title = (ev.get("title", "") or "").lower()
    cat = (ev.get("category", "") or "").lower()
    markets = ev.get("markets", [])
    
    is_sports = False
    if isinstance(markets, list):
        for mk in markets:
            mt = (mk.get("question", "") or mk.get("title", "") or "").lower()
            if any(w in mt for w in ['vs', 'win on', 'draw', 'o/u', 'spread',
                                      'nba', 'nfl', 'soccer', 'premier']):
                is_sports = True
                break
    
    if is_sports or any(w in title or w in cat for w in ['nba', 'nfl', 'soccer', 'sport']):
        sports_events.append(ev)

print(f"Sports-related events: {len(sports_events)}")

for ev in sports_events[:15]:
    title = ev.get("title", "")
    markets = ev.get("markets", [])
    n = len(markets) if isinstance(markets, list) else 0
    print(f"\n  EVENT: {title[:70]} ({n} markets)")
    if isinstance(markets, list):
        for mk in markets[:5]:
            mt = mk.get("question", "") or mk.get("title", "")
            print(f"    {mt[:75]}")
        if n > 5:
            print(f"    ... and {n-5} more")

# Summary
print(f"\n{'='*60}")
print("SUMMARY — TRIAD VIABILITY")
print(f"{'='*60}")
print(f"  Total markets: {len(all_markets)}")
print(f"  Sports markets: {len(sports_markets)}")
print(f"  Draw markets: {len(draw_markets)}")
print(f"  'Will X win' markets: {len(win_markets)}")
print(f"  'X vs Y' markets: {len(vs_markets)}")
print(f"  Sports events: {len(sports_events)}")

if draw_markets and win_markets:
    print(f"\n  ✓ TRIADS POSSIBLE — draw + win markets exist")
    print(f"    Need to match team names between draw and win markets")
else:
    print(f"\n  ⚠️  TRIADS MAY NOT BE AVAILABLE RIGHT NOW")
    if not draw_markets:
        print(f"    No draw markets found — soccer may not be active")
    if not win_markets:
        print(f"    No 'Will X win' markets found")
    print(f"    RN1's triads were on European soccer (71% of his volume)")
    print(f"    Check back during European soccer hours (Sat/Sun noon-6pm UTC)")
