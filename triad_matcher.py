#!/usr/bin/env python3
"""
Triad Matcher v2
═════════════════
Start from DRAW markets (we know these exist), extract team names,
then search for matching "Will X win" markets.

Draw format: "Will Portsmouth FC vs. Ipswich Town FC end in a draw?"
Win format:  "Will Portsmouth FC win on 2026-03-01?" (or similar)
"""
import json, time, re
from urllib.request import urlopen, Request
from urllib.error import HTTPError
from collections import defaultdict

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
            else:
                return None
        except:
            time.sleep(1)
    return None

print("="*60)
print("TRIAD MATCHER v2")
print("="*60)

# Step 1: Fetch ALL markets
print("\nFetching all markets...")
all_markets = []
offset = 0
while True:
    url = f"https://gamma-api.polymarket.com/markets?limit=100&offset={offset}&active=true&closed=false"
    data = api_get(url)
    if not data or not isinstance(data, list) or len(data) == 0:
        break
    all_markets.extend(data)
    if len(all_markets) % 1000 == 0:
        print(f"  {len(all_markets)} markets...", end="\r")
    offset += 100
    time.sleep(0.2)
    if len(data) < 100:
        break

print(f"\nTotal markets: {len(all_markets):,}")

# Step 2: Index all markets by normalized title fragments
def normalize(s):
    return re.sub(r'[^a-z0-9\s]', '', s.lower()).strip()

def extract_draw_teams(title):
    """Extract team names from draw market title."""
    m = re.match(r'Will (.+?)\s+vs\.?\s+(.+?)\s+end in a draw', title, re.I)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return None, None

# Build indexes
draw_markets = []
win_markets_by_team = defaultdict(list)  # normalized team name → [market, ...]
all_indexed = {}

for m in all_markets:
    title = m.get("question", "") or m.get("title", "")
    mid = m.get("id", "")
    clob_ids = m.get("clobTokenIds", [])
    if isinstance(clob_ids, str):
        try: clob_ids = json.loads(clob_ids)
        except: clob_ids = []
    
    has_tokens = len(clob_ids) >= 2 if isinstance(clob_ids, list) else False
    
    m["_title"] = title
    m["_has_tokens"] = has_tokens
    m["_clob_ids"] = clob_ids
    all_indexed[mid] = m
    
    # Draw markets
    a, b = extract_draw_teams(title)
    if a and b:
        draw_markets.append({
            "market": m,
            "team_a": a,
            "team_b": b,
            "norm_a": normalize(a),
            "norm_b": normalize(b),
        })
    
    # Win markets: "Will X win on DATE?" or "Will X win?"
    # But NOT "Will X win the 2026 NBA Finals?" (season futures)
    wm = re.match(r'Will (.+?) win(?:\s+on\s+(\d{4}-\d{2}-\d{2}))?\s*\??$', title, re.I)
    if wm:
        team = wm.group(1).strip()
        date = wm.group(2)  # may be None
        norm_team = normalize(team)
        
        # Skip season futures (contain "2026 NBA", "Stanley Cup", etc.)
        title_lower = title.lower()
        is_future = any(kw in title_lower for kw in [
            'finals', 'stanley cup', 'world cup', 'super bowl', 'championship',
            'premier league', 'serie a', 'bundesliga', 'ligue 1', 'la liga',
            'champion', 'playoffs', 'division', 'conference', 'nomination',
            'presidential', 'election', 'award', 'mvp', 'rookie',
            'draft', 'qualify', 'relegat',
        ])
        
        if not is_future:
            win_markets_by_team[norm_team].append({
                "market": m,
                "team": team,
                "date": date,
                "norm": norm_team,
            })

print(f"\nDraw markets: {len(draw_markets)}")
print(f"Match-day win markets: {sum(len(v) for v in win_markets_by_team.values())} across {len(win_markets_by_team)} teams")

# Show sample draw markets
print(f"\nSample draw markets:")
for d in draw_markets[:10]:
    t = d["market"]["_title"]
    has = "✓" if d["market"]["_has_tokens"] else "✗"
    print(f"  [{has}] {t[:80]}")

# Show sample win markets
print(f"\nSample win markets (match-day, not futures):")
shown = 0
for team, wins in sorted(win_markets_by_team.items())[:20]:
    for w in wins[:1]:
        has = "✓" if w["market"]["_has_tokens"] else "✗"
        print(f"  [{has}] {w['market']['_title'][:80]}")
        shown += 1
    if shown > 15:
        break

# Step 3: Match draw markets to win markets
print(f"\n{'='*60}")
print("MATCHING TRIADS")
print(f"{'='*60}")

complete_triads = []
partial_triads = []
no_match = []

for d in draw_markets:
    na = d["norm_a"]
    nb = d["norm_b"]
    
    # Find win markets for team A
    wins_a = win_markets_by_team.get(na, [])
    # Fuzzy: try substring matching
    if not wins_a:
        for k, v in win_markets_by_team.items():
            if na in k or k in na:
                wins_a = v
                break
    
    # Find win markets for team B
    wins_b = win_markets_by_team.get(nb, [])
    if not wins_b:
        for k, v in win_markets_by_team.items():
            if nb in k or k in nb:
                wins_b = v
                break
    
    if wins_a and wins_b:
        # Check if all three have tokens
        draw_tok = d["market"]["_has_tokens"]
        a_tok = any(w["market"]["_has_tokens"] for w in wins_a)
        b_tok = any(w["market"]["_has_tokens"] for w in wins_b)
        
        triad = {
            "team_a": d["team_a"],
            "team_b": d["team_b"],
            "draw": d["market"],
            "win_a": wins_a[0]["market"],
            "win_b": wins_b[0]["market"],
            "all_have_tokens": draw_tok and a_tok and b_tok,
        }
        
        if triad["all_have_tokens"]:
            complete_triads.append(triad)
        else:
            partial_triads.append(triad)
    elif wins_a or wins_b:
        partial_triads.append({
            "team_a": d["team_a"],
            "team_b": d["team_b"],
            "draw": d["market"],
            "win_a": wins_a[0]["market"] if wins_a else None,
            "win_b": wins_b[0]["market"] if wins_b else None,
            "has_a": bool(wins_a),
            "has_b": bool(wins_b),
        })
    else:
        no_match.append(d)

print(f"\n  Complete triads (all 3 have tokens): {len(complete_triads)}")
print(f"  Partial triads (some legs missing):  {len(partial_triads)}")
print(f"  No match (draw only):                {len(no_match)}")

# Show complete triads
if complete_triads:
    print(f"\n{'─'*60}")
    print(f"COMPLETE TRIADS WITH TOKENS ({len(complete_triads)})")
    print(f"{'─'*60}")
    
    for t in complete_triads[:20]:
        print(f"\n  {t['team_a']} vs {t['team_b']}")
        print(f"    Win A: {t['win_a']['_title'][:70]}")
        print(f"    Win B: {t['win_b']['_title'][:70]}")
        print(f"    Draw:  {t['draw']['_title'][:70]}")
        
        # Show token IDs
        a_ids = t['win_a'].get('_clob_ids', [])
        b_ids = t['win_b'].get('_clob_ids', [])
        d_ids = t['draw'].get('_clob_ids', [])
        print(f"    Tokens: A={len(a_ids)} B={len(b_ids)} D={len(d_ids)}")

# Show some partial triads
if partial_triads:
    print(f"\n{'─'*60}")
    print(f"SAMPLE PARTIAL TRIADS ({len(partial_triads)} total)")
    print(f"{'─'*60}")
    for t in partial_triads[:10]:
        print(f"\n  {t['team_a']} vs {t['team_b']}")
        if t.get('win_a'):
            print(f"    ✓ Win A: {t['win_a']['_title'][:60]}")
        else:
            print(f"    ✗ Win A: MISSING ({t['team_a']})")
        if t.get('win_b'):
            print(f"    ✓ Win B: {t['win_b']['_title'][:60]}")
        else:
            print(f"    ✗ Win B: MISSING ({t['team_b']})")
        print(f"    {'✓' if t['draw']['_has_tokens'] else '✗'} Draw: {t['draw']['_title'][:60]}")

# Show no-match samples
if no_match:
    print(f"\n{'─'*60}")
    print(f"SAMPLE NO-MATCH DRAWS ({len(no_match)} total)")
    print(f"{'─'*60}")
    for d in no_match[:10]:
        print(f"  {d['team_a']} vs {d['team_b']}")
        print(f"    Searched: '{d['norm_a']}' and '{d['norm_b']}'")

# Save results
results = {
    "total_markets": len(all_markets),
    "draw_markets": len(draw_markets),
    "win_teams": len(win_markets_by_team),
    "complete_triads": len(complete_triads),
    "partial_triads": len(partial_triads),
    "no_match": len(no_match),
    "complete_details": [
        {
            "team_a": t["team_a"],
            "team_b": t["team_b"],
            "draw_id": t["draw"].get("id"),
            "win_a_id": t["win_a"].get("id"),
            "win_b_id": t["win_b"].get("id"),
            "draw_title": t["draw"]["_title"],
            "win_a_title": t["win_a"]["_title"],
            "win_b_title": t["win_b"]["_title"],
        }
        for t in complete_triads
    ]
}

with open("triad_match_results.json", "w") as f:
    json.dump(results, f, indent=2)

print(f"\nSaved: triad_match_results.json")
print(f"\n{'='*60}")
print("NEXT STEPS")
print(f"{'='*60}")
if complete_triads:
    print(f"  ✓ {len(complete_triads)} tradeable triads found!")
    print(f"  → Run triad_scanner.py to check live prices")
    print(f"  → Or start arb_v6.py to begin trading")
else:
    print(f"  No complete tradeable triads right now.")
    print(f"  European soccer match-day markets typically appear:")
    print(f"    - Friday-Monday for weekend fixtures")
    print(f"    - Tuesday-Wednesday for Champions League")
    print(f"    - Markets usually listed 1-3 days before kickoff")
    print(f"  Run this scan again tomorrow during European hours")
