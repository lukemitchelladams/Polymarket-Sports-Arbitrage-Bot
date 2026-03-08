import requests, json
s = requests.Session()
s.headers.update({'Accept':'application/json'})
now_ts = __import__('datetime').datetime.now(__import__('datetime').timezone.utc)
for offset in [0,200,400]:
    r = s.get('https://gamma-api.polymarket.com/markets',params={'active':'true','closed':'false','limit':200,'offset':offset,'order':'volume24hr','ascending':'false'},timeout=15).json()
    for m in r:
        t = m.get('clobTokenIds') or '[]'
        if isinstance(t,str): t=json.loads(t)
        if len(t)!=2: continue
        end = m.get('endDate','')
        try:
            from datetime import datetime,timezone
            ed = datetime.fromisoformat(end.replace('Z','+00:00'))
            h = (ed-now_ts).total_seconds()/3600
            if 0<h<2:
                print(f"{h:.1f}h | {m.get('question','')[:70]}")
        except: pass
