"""
PolyArb Scanner v4 — Clean + Filtered
=======================================
Fixes:
  - Skips expired markets (end date passed) — removes fake high-spread opps
  - Parallel price fetching for speed
  - Only real arb: both sides 0.05 < price < 0.95, sum < 0.985

Run:         python scanner.py
Watch mode:  python scanner.py --watch
Fast watch:  python scanner.py --watch --interval 10
"""

import json, time, sys, requests
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

CLOB    = "https://clob.polymarket.com"
GAMMA   = "https://gamma-api.polymarket.com"
HEADERS = {"Accept": "application/json", "User-Agent": "PolyArb/4.0"}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

SPORTS_KEYWORDS = [
    "win on","will win","o/u","over/under","spread","moneyline",
    "nba","nfl","nhl","mlb","epl","ucl","mls","ufc","mma",
    "soccer","football","basketball","baseball","hockey","tennis",
    "golf","rugby","cricket","boxing","lakers","celtics","warriors",
    "bulls","heat","knicks","nets","suns","mavs","spurs","bucks",
    "sixers","clippers","nuggets","raptors","thunder","timberwolves",
    "pelicans","grizzlies","real madrid","barcelona","liverpool",
    "arsenal","chelsea","manchester","tottenham","psg","juventus",
    "inter milan","ac milan","bayern","dortmund","ajax","porto",
    "benfica","atletico","patriots","chiefs","eagles","cowboys",
    "49ers","bills","packers","ravens","bengals","broncos","rams",
    "seahawks","yankees","dodgers","astros","braves","red sox","cubs",
    "maple leafs","bruins","rangers","penguins","oilers","flames",
    "vs ","draw","clean sheet","both teams","sets","match winner",
    "game winner","series winner","first half","second half",
    "total goals","total points","total runs","total kills",
    "grêmio","flamengo","botafogo","palmeiras","corinthians",
    "serie a","bundesliga","la liga","ligue 1","champions league",
    "europa league","premier league","copa","libertadores",
    "f1","formula","cs2","valorant","dota","league of legends"
]

def get(url, params=None, timeout=8):
    try:
        r = SESSION.get(url, params=params, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except: pass
    return None

def is_sports(title):
    t = title.lower()
    return any(k in t for k in SPORTS_KEYWORDS)

def is_expired(market):
    end = market.get("endDate") or market.get("end_date_iso") or ""
    if not end:
        return False
    try:
        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
        return end_dt < datetime.now(timezone.utc)
    except:
        return False

def fetch_price(token_id, side="BUY"):
    data = get(f"{CLOB}/price", params={"token_id": token_id, "side": side})
    if data and isinstance(data, dict):
        return float(data.get("price") or 0)
    return 0.0

def fetch_price_pair(token_yes, token_no):
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_yes = ex.submit(fetch_price, token_yes, "BUY")
        f_no  = ex.submit(fetch_price, token_no,  "BUY")
        return f_yes.result(timeout=8), f_no.result(timeout=8)

def fetch_markets():
    sports  = []
    expired = 0
    offset  = 0
    now     = datetime.now(timezone.utc)

    while len(sports) < 500:
        data = get(f"{GAMMA}/markets", params={
            "active": "true", "closed": "false",
            "limit": 200, "offset": offset,
            "order": "volume24hr", "ascending": "false"
        })
        if not data or not isinstance(data, list) or not data:
            break
        for m in data:
            title = m.get("question") or m.get("title") or ""
            if not is_sports(title):
                continue
            # Skip expired markets
            end = m.get("endDate") or m.get("end_date_iso") or ""
            if end:
                try:
                    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
                    if end_dt < now:
                        expired += 1
                        continue  # Already expired
                    hours_until_end = (end_dt - now).total_seconds() / 3600
                    if hours_until_end > 48:
                        expired += 1
                        continue  # Long-term future — skip
                except:
                    pass
            sports.append(m)
        if len(data) < 200:
            break
        offset += 200
        time.sleep(0.1)

    return sports, expired

def check_market(market):
    title  = market.get("question") or market.get("title") or ""
    tokens = market.get("clobTokenIds") or []
    if isinstance(tokens, str):
        try:    tokens = json.loads(tokens)
        except: return None
    if len(tokens) < 2:
        return None

    yes_ask, no_ask = fetch_price_pair(tokens[0], tokens[1])

    # Both sides must be alive and healthy
    if yes_ask <= 0.05 or no_ask <= 0.05: return None  # dead/resolved
    if yes_ask >= 0.95 or no_ask >= 0.95: return None  # one side won
    total = yes_ask + no_ask
    if total >= 0.985: return None  # efficient, no spread
    if total < 0.50:   return None  # drained

    spread_pct = (1.0 - total) * 100
    return {
        "title":      title,
        "yes_ask":    yes_ask,
        "no_ask":     no_ask,
        "total":      total,
        "spread_pct": spread_pct,
        "net_pct":    spread_pct - 2.0,
        "profitable": spread_pct - 2.0 > 0.5,
        "liquidity":  float(market.get("liquidity") or 0),
        "yes_token":  tokens[0],
        "no_token":   tokens[1],
        "condition":  market.get("conditionId", ""),
        "end_date":   market.get("endDate", "")[:10]
    }

def scan():
    now  = datetime.now(timezone.utc)
    hour = now.hour
    print(f"\n{'='*72}")
    print(f"  PolyArb Scanner v4 — {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*72}")

    if hour < 17 or hour > 23:
        print(f"  ⚠️  {hour:02d}:00 UTC — outside peak (20-22 UTC)\n")
    else:
        print(f"  ✅ Peak hours active ({hour:02d}:00 UTC)\n")

    print("Fetching active sports markets (skipping expired)...")
    markets, expired_count = fetch_markets()
    print(f"Found {len(markets)} valid markets ({expired_count} expired/skipped)")
    print("Checking live prices in parallel...\n")

    opps = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(check_market, m): m for m in markets}
        done = 0
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                opps.append(result)
                flag = "🔥" if result["spread_pct"] > 5 else "✅" if result["spread_pct"] > 2.5 else "·"
                print(f"  {flag} {result['spread_pct']:5.2f}%  YES@{result['yes_ask']:.4f}+NO@{result['no_ask']:.4f}  {result['title'][:50]}")
            if done % 50 == 0:
                print(f"  ... {done}/{len(markets)} checked, {len(opps)} opps")

    opps.sort(key=lambda x: x["spread_pct"], reverse=True)
    profitable = [o for o in opps if o["profitable"]]

    print(f"\n{'─'*72}")
    print(f"  REAL PROFITABLE ARB ({len(profitable)} opportunities)")
    print(f"  Both sides alive (0.05-0.95), end date not yet passed")
    print(f"{'─'*72}")

    if profitable:
        print(f"\n  {'SPREAD':>8}  {'NET':>7}  {'YES':>8}  {'NO':>7}  {'ENDS':>10}  MARKET")
        print(f"  {'─'*8}  {'─'*7}  {'─'*8}  {'─'*7}  {'─'*10}  {'─'*38}")
        for o in profitable:
            flag = "🔥" if o["spread_pct"] > 5 else "✅"
            print(
                f"  {flag} {o['spread_pct']:5.2f}%  "
                f"{o['net_pct']:+6.2f}%  "
                f"  ${o['yes_ask']:.4f}  "
                f"${o['no_ask']:.4f}  "
                f"{o['end_date']:>10}  "
                f"{o['title'][:38]}"
            )
        best = profitable[0]
        bet  = 20
        profit = bet * 2 * (best['spread_pct']/100) - 0.4
        print(f"\n  Best: {best['spread_pct']:.2f}% spread on '{best['title'][:55]}'")
        print(f"  Bet $20 each side → ~${profit:.2f} profit  (ends {best['end_date']})")
    else:
        print(f"  No real arb right now.")
        if hour < 17:
            print(f"  Try again at 20:00 UTC ({20-hour}h from now)")

    marginal = [o for o in opps if not o["profitable"] and o["spread_pct"] >= 1.5]
    if marginal:
        print(f"\n  MARGINAL — below 2.5% fee threshold ({len(marginal)}):")
        for o in marginal[:5]:
            print(f"    {o['spread_pct']:4.1f}%  {o['title'][:55]}")

    print(f"\n{'─'*72}")
    print(f"  Valid markets: {len(markets)}  |  Expired skipped: {expired_count}  |  Real arb: {len(opps)}  |  Profitable: {len(profitable)}")
    print(f"  Peak hours: 20:00-22:00 UTC  |  Now: {hour:02d}:00 UTC")
    print(f"{'='*72}\n")
    return opps

if __name__ == "__main__":
    watch    = "--watch" in sys.argv
    interval = int(sys.argv[sys.argv.index("--interval")+1]) if "--interval" in sys.argv else 30
    if watch:
        print(f"Watch mode: scanning every {interval}s. Ctrl+C to stop.")
        while True:
            try:
                scan()
                print(f"Next scan in {interval}s...")
                time.sleep(interval)
            except KeyboardInterrupt:
                print("\nStopped.")
                break
    else:
        scan()
        print("Tips:")
        print("  python scanner.py --watch            # rescan every 30s")
        print("  python scanner.py --watch --interval 10  # rescan every 10s")
