#!/usr/bin/env python3
"""
RN1 Early Stage Reconstruction — First 10K/20K trades
Streams rn1_enriched_trades.csv, matches by token_id.

Usage:
  python3.11 rn1_triad_calibrate.py              # first 20K trades
  python3.11 rn1_triad_calibrate.py --n 10000    # first 10K
  python3.11 rn1_triad_calibrate.py --all         # all 1.1M (slow)
"""
import json, os, re, sys, csv
from collections import defaultdict, Counter

def normalize(s):
    return re.sub(r'[^a-z0-9\s]', '', s.lower()).strip()

FUTURES_KW = ['finals','stanley cup','world cup','super bowl','championship',
    'premier league','serie a','bundesliga','ligue 1','la liga','champion',
    'playoffs','division','conference','nomination','presidential','election',
    'award','mvp','rookie','draft','qualify','relegat','top goal']

def main():
    max_trades = 20000
    if "--all" in sys.argv: max_trades = 99999999
    for i, a in enumerate(sys.argv):
        if a == "--n" and i+1 < len(sys.argv): max_trades = int(sys.argv[i+1])
    
    print(f"RN1 EARLY STAGE — First {max_trades:,} trades")
    print("="*60)
    
    for p in ["rn1_analysis/rn1_token_map.json", "rn1_token_map.json"]:
        if os.path.exists(p):
            with open(p) as f: tm = json.load(f)
            break
    else: sys.exit("Need rn1_token_map.json")
    print(f"Token map: {len(tm):,}")
    
    draw_index = {}; token_class = {}
    for tid, info in tm.items():
        title = info.get("title","")
        dm = re.match(r'Will (.+?)\s+vs\.?\s+(.+?)\s+end in a draw', title, re.I)
        if dm:
            a,b = dm.group(1).strip(), dm.group(2).strip()
            na,nb = normalize(a), normalize(b)
            mk = f"{min(na,nb)}|{max(na,nb)}"
            token_class[tid] = {"type":"draw","mk":mk,"leg":"d"}
            draw_index[(na,nb)]=mk; draw_index[(nb,na)]=mk; continue
        wm = re.match(r'Will (.+?) win(?:\s+on\s+(\d{4}-\d{2}-\d{2}))?\s*\??$', title, re.I)
        if wm and not any(kw in title.lower() for kw in FUTURES_KW):
            token_class[tid] = {"type":"win","nt":normalize(wm.group(1))}; continue
        if re.search(r'o/u\s+\d', title, re.I): token_class[tid]={"type":"ou"}
        elif 'spread' in title.lower(): token_class[tid]={"type":"spread"}
        elif any(w in title.lower() for w in ['counter-strike','dota','valorant','lol:']): token_class[tid]={"type":"esports"}
        elif re.match(r'^(.+?)\s+vs\.?\s+(.+?)$', title, re.I): token_class[tid]={"type":"match"}
        else: token_class[tid]={"type":"other"}
    
    for tid, cls in token_class.items():
        if cls["type"]!="win": continue
        nt=cls["nt"]
        for (na,nb),mk in draw_index.items():
            if nt==na: cls["mk"]=mk;cls["leg"]="a";break
            elif nt==nb: cls["mk"]=mk;cls["leg"]="b";break
        if "mk" not in cls:
            for (na,nb),mk in draw_index.items():
                if nt in na or na in nt: cls["mk"]=mk;cls["leg"]="a";break
                elif nt in nb or nb in nt: cls["mk"]=mk;cls["leg"]="b";break
    
    linked=sum(1 for c in token_class.values() if "mk" in c)
    print(f"Classified: {len(token_class):,}, linked: {linked:,}")
    
    csv_path = None
    for p in ["rn1_analysis/rn1_enriched_trades.csv","rn1_enriched_trades.csv"]:
        if os.path.exists(p): csv_path=p; break
    if not csv_path: sys.exit("Need rn1_enriched_trades.csv")
    print(f"\nStreaming: {csv_path}")
    
    triad_trades=defaultdict(lambda:{"a":[],"b":[],"d":[]})
    by_type=Counter(); running_cost=0; running_rev=0; dates_seen=set()
    daily_spend=defaultdict(float); daily_rev=defaultdict(float); daily_trades=defaultdict(int)
    buy_costs=[]; buy_prices=[]; matched=unmatched=0
    
    with open(csv_path,'r',encoding='utf-8',errors='replace') as f:
        reader=csv.DictReader(f); count=0
        for row in reader:
            count+=1
            if count>max_trades: break
            tid=row.get("token_id","").strip()
            trade_type=row.get("trade_type","").upper()
            try: price=abs(float(row.get("price",0)))
            except: price=0
            try: shares=abs(float(row.get("shares",0)))
            except: shares=0
            try: cost=abs(float(row.get("cost",0)))
            except: cost=0
            try: rev=abs(float(row.get("revenue",0)))
            except: rev=0
            try: epoch=float(row.get("epoch",0))
            except: epoch=0
            date=row.get("date",""); title=row.get("title","")
            if date: dates_seen.add(date)
            if trade_type=="BUY": running_cost+=cost; buy_costs.append(cost); buy_prices.append(price)
            elif trade_type=="SELL": running_rev+=rev
            daily_spend[date]+=cost if trade_type=="BUY" else 0
            daily_rev[date]+=rev if trade_type=="SELL" else 0
            daily_trades[date]+=1
            
            cls=token_class.get(tid)
            if not cls: unmatched+=1; by_type["unmatched"]+=1; continue
            matched+=1; by_type[cls["type"]]+=1
            mk=cls.get("mk"); leg=cls.get("leg")
            if mk and leg:
                triad_trades[mk][leg].append({"price":price,"shares":shares,"cost":cost,
                    "revenue":rev,"epoch":epoch,"type":trade_type,"title":title,"date":date})
            if count%5000==0: print(f"  {count:,}...",end="\r")
    
    print(f"\n\nProcessed: {count:,}")
    print(f"Matched: {matched:,}  Unmatched: {unmatched:,}")
    print(f"Types: {dict(by_type.most_common())}")
    ds=sorted(dates_seen)
    print(f"Dates: {ds[0] if ds else '?'} → {ds[-1] if ds else '?'} ({len(ds)} days)")
    print(f"Spent: ${running_cost:,.2f}  Revenue: ${running_rev:,.2f}")
    
    print(f"\n{'='*60}\nDAILY ACTIVITY\n{'='*60}")
    cum_s=cum_r=0
    for d in sorted(daily_spend.keys()):
        s=daily_spend[d]; r=daily_rev[d]; n=daily_trades[d]
        cum_s+=s; cum_r+=r
        print(f"  {d}: {n:>5d} trades  spent ${s:>8,.0f}  rev ${r:>6,.0f}  cum: -${cum_s:,.0f} +${cum_r:,.0f}")
    
    print(f"\n{'='*60}\nTRADE SIZES\n{'='*60}")
    bc=sorted([c for c in buy_costs if c>0])
    if bc:
        n=len(bc)
        print(f"  Cost: p10=${bc[int(n*.1)]:.2f} p25=${bc[int(n*.25)]:.2f} med=${bc[n//2]:.2f} p75=${bc[int(n*.75)]:.2f} p90=${bc[int(n*.9)]:.2f} mean=${sum(bc)/n:.2f}")
        for lo,hi,lb in [(0,1,"$0-1"),(1,5,"$1-5"),(5,10,"$5-10"),(10,20,"$10-20"),(20,50,"$20-50"),(50,100,"$50-100"),(100,500,"$100-500"),(500,9999,"$500+")]:
            ct=len([c for c in bc if lo<=c<hi])
            if ct: print(f"    {lb:>10s}: {ct:>5d} ({ct/n*100:>5.1f}%) {'█'*min(int(ct/n*50),30)}")
    
    bp=sorted([p for p in buy_prices if p>0])
    if bp:
        n=len(bp)
        print(f"\n  NO prices: med={bp[n//2]:.4f}")
        for lo,hi,lb in [(0,.1,"penny"),(0.1,.3,"low"),(0.3,.5,"mid"),(0.5,.7,"mid-hi"),(0.7,.9,"fav"),(0.9,1.01,"near-cert")]:
            ct=len([p for p in bp if lo<=p<hi])
            print(f"    {lb:>10s}: {ct:>5d} ({ct/n*100:>5.1f}%)")
    
    print(f"\n{'='*60}\nTRIAD RECONSTRUCTION\n{'='*60}")
    results=[]
    for mk,legs in triad_trades.items():
        r={"mk":mk,"legs":{}}
        for leg in ["a","b","d"]:
            buys=[t for t in legs[leg] if t["type"]=="BUY"]
            if not buys: buys=[t for t in legs[leg] if t["cost"]>0]
            cost=sum(t["cost"] for t in buys); shares=sum(t["shares"] for t in buys)
            epochs=[t["epoch"] for t in buys if t["epoch"]>0]
            avg_p=cost/shares if shares>0 else 0
            r["legs"][leg]={"n":len(buys),"cost":round(cost,2),"shares":round(shares,2),
                "avg_p":round(avg_p,4),"first_ts":min(epochs) if epochs else None}
        has=[l for l in "abd" if r["legs"][l]["n"]>0]
        r["complete"]=len(has)==3; r["n_legs"]=len(has)
        r["total_cost"]=round(sum(r["legs"][l]["cost"] for l in "abd"),2)
        r["total_buys"]=sum(r["legs"][l]["n"] for l in "abd")
        if r["complete"]:
            r["cost_per_set"]=round(sum(r["legs"][l]["avg_p"] for l in "abd"),4)
            min_sh=min(r["legs"][l]["shares"] for l in "abd")
            r["payout"]=round(min_sh*2,2); r["edge"]=round(r["payout"]-r["total_cost"],2)
            fts=[r["legs"][l]["first_ts"] for l in "abd" if r["legs"][l]["first_ts"]]
            if len(fts)>=2:
                r["timing_sec"]=round(max(fts)-min(fts),1)
                lt=sorted([(l,r["legs"][l]["first_ts"]) for l in "abd" if r["legs"][l]["first_ts"]],key=lambda x:x[1])
                r["first_leg"]=lt[0][0]; r["leg_order"]=[x[0] for x in lt]
            if r["total_cost"]>0:
                r["ratio"]={l:round(r["legs"][l]["cost"]/r["total_cost"]*100,1) for l in "abd"}
        results.append(r)
    
    complete=[r for r in results if r["complete"]]
    partial=[r for r in results if not r["complete"]]
    print(f"  Complete: {len(complete):,}  Partial: {len(partial):,}")
    
    if complete:
        costs=sorted([r["cost_per_set"] for r in complete if r.get("cost_per_set",0)>0])
        timings=sorted([r["timing_sec"] for r in complete if "timing_sec" in r])
        first_legs=Counter(r.get("first_leg") for r in complete if "first_leg" in r)
        ratios=[r["ratio"] for r in complete if "ratio" in r]
        edges=sorted([r["edge"] for r in complete if "edge" in r],reverse=True)
        
        if costs:
            n=len(costs)
            print(f"\nQ1 COST/SET: min=${min(costs):.4f} med=${costs[n//2]:.4f} max=${max(costs):.4f}")
            for t in [1.80,1.90,1.95,2.00,2.05,2.10,2.50]:
                b=len([c for c in costs if c<=t])
                if b: print(f"  ≤${t}: {b}/{n} ({b/n*100:.0f}%)")
        if timings:
            nt=len(timings)
            print(f"\nQ2 TIMING: med={timings[nt//2]:.0f}s  <10s={len([t for t in timings if t<10])/nt*100:.0f}% <1m={len([t for t in timings if t<60])/nt*100:.0f}%")
            print(f"  First leg: {dict(first_legs.most_common())}")
        if ratios:
            print(f"\nQ3 SIZING: A={sum(r['a'] for r in ratios)/len(ratios):.1f}% B={sum(r['b'] for r in ratios)/len(ratios):.1f}% D={sum(r['d'] for r in ratios)/len(ratios):.1f}%")
            for r in complete[:10]:
                rat=r.get("ratio",{}); mk=r["mk"].replace("|"," v ")[:35]
                print(f"    {mk:<35s} A:{rat.get('a',0):>5.1f}% B:{rat.get('b',0):>5.1f}% D:{rat.get('d',0):>5.1f}%")
        if edges:
            ne=len(edges); prof=len([e for e in edges if e>0])
            print(f"\nQ4 P&L: {prof}/{ne} profitable  total=${sum(edges):,.2f}  avg=${sum(edges)/ne:.2f}")
    
    if partial:
        lc=Counter()
        for r in partial:
            has=tuple(sorted(l for l in "abd" if r["legs"][l]["n"]>0))
            lc[has]+=1
        print(f"\n  Partial leg combos: {dict(lc.most_common())}")
    
    # Save
    cal={"meta":{"n_processed":count,"max":max_trades,"dates":[ds[0],ds[-1]] if ds else [],
        "days":len(ds),"spent":round(running_cost,2),"revenue":round(running_rev,2)},
        "types":dict(by_type.most_common()),"matched":matched,"unmatched":unmatched,
        "complete":len(complete),"partial":len(partial)}
    if bc:
        n=len(bc)
        cal["sizes"]={"p10":round(bc[int(n*.1)],2),"p25":round(bc[int(n*.25)],2),
            "med":round(bc[n//2],2),"p75":round(bc[int(n*.75)],2),"p90":round(bc[int(n*.9)],2),"mean":round(sum(bc)/n,2)}
    if complete and costs:
        n=len(costs); cal["Q1"]={k:round(v,4) for k,v in {"min":min(costs),"p25":costs[int(n*.25)],
            "med":costs[n//2],"p75":costs[int(n*.75)],"max":max(costs)}.items()}
    if timings: nt=len(timings); cal["Q2"]={"med_sec":round(timings[nt//2],1),"u10s":round(len([t for t in timings if t<10])/nt*100,1),"u1m":round(len([t for t in timings if t<60])/nt*100,1)}
    if first_legs: cal["Q2_first"]=dict(first_legs.most_common())
    if ratios: cal["Q3"]={"a":round(sum(r["a"] for r in ratios)/len(ratios),1),"b":round(sum(r["b"] for r in ratios)/len(ratios),1),"d":round(sum(r["d"] for r in ratios)/len(ratios),1)}
    if edges: ne=len(edges); cal["Q4"]={"prof_pct":round(len([e for e in edges if e>0])/ne*100,1),"total":round(sum(edges),2),"avg":round(sum(edges)/ne,2)}
    cal["daily"]={d:{"n":daily_trades[d],"s":round(daily_spend.get(d,0),0),"r":round(daily_rev.get(d,0),0)} for d in sorted(daily_spend.keys())}
    cal["triads_complete"]=complete[:15]
    cal["triads_partial"]=sorted(partial,key=lambda r:-r["total_cost"])[:15]
    
    with open("rn1_triad_calibration.json","w") as f: json.dump(cal,f,indent=2)
    print(f"\nSaved: rn1_triad_calibration.json ({os.path.getsize('rn1_triad_calibration.json')/1024:.1f}KB)")
    print("Upload to Claude!")

if __name__=="__main__": main()
