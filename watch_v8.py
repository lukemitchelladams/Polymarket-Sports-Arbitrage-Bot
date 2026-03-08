#!/usr/bin/env python3
"""
PolyArb v9 — Live Watch Dashboard
═══════════════════════════════════
Shows EVERYTHING. Parallel price fetch. All positions. Full detail.

Usage:
  python3.11 watch_v8.py                # default 5s refresh
  python3.11 watch_v8.py --refresh 3    # faster
  python3.11 watch_v8.py --once         # print once and exit
"""
import os, sys, json, time, glob
from datetime import datetime
from collections import defaultdict
from urllib.request import urlopen, Request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PORTFOLIO  = os.path.join(SCRIPT_DIR, "portfolio_v8.json")
CLOB       = "https://clob.polymarket.com"

R="\033[0m"; B="\033[1m"; D="\033[2m"
GR="\033[92m"; RD="\033[91m"; YL="\033[93m"; CY="\033[96m"; MG="\033[95m"
BG_R="\033[41m"; BG_G="\033[42m"

def clr_pnl(pct):
    if pct >= 50: return f"{BG_G}{B}"
    if pct >= 10: return GR
    if pct >= 0: return D
    if pct >= -20: return YL
    if pct >= -50: return RD
    return f"{BG_R}{B}"

def clr_val(val): return GR if val >= 0 else RD

def fetch_book(token_id):
    try:
        req = Request(f"{CLOB}/book?token_id={token_id}",
                     headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
        book = json.loads(urlopen(req, timeout=6).read().decode("utf-8"))
        best_bid, best_ask = None, None
        for b in book.get("bids", []):
            try:
                p = float(b["price"])
                if best_bid is None or p > best_bid: best_bid = p
            except: pass
        for a in book.get("asks", []):
            try:
                p = float(a["price"])
                if best_ask is None or p < best_ask: best_ask = p
            except: pass
        return best_bid, best_ask
    except:
        return None, None

def batch_fetch(token_ids, max_workers=12):
    """Fetch all books in parallel — 12x faster than serial."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    cache = {}
    unique = list(set(t for t in token_ids if t))
    if not unique: return cache
    with ThreadPoolExecutor(max_workers=min(max_workers, len(unique))) as pool:
        futures = {pool.submit(fetch_book, tid): tid for tid in unique}
        for f in as_completed(futures):
            tid = futures[f]
            try: cache[tid] = f.result()
            except: cache[tid] = (None, None)
    return cache

def find_log():
    d = os.path.join(SCRIPT_DIR, "logs")
    fs = glob.glob(os.path.join(d, "v8_*.log"))
    return max(fs, key=os.path.getmtime) if fs else None

def parse_log_actions(logfile, max_lines=500):
    actions = []
    try:
        with open(logfile, 'r') as f:
            lines = f.readlines()
        for line in lines[-max_lines:]:
            line = line.strip()
            if any(icon in line for icon in ["📗","📕","🔒","🛡️","🎯","⚠️","ERR"]):
                actions.append(line)
    except: pass
    return actions

def read_portfolio():
    try:
        with open(PORTFOLIO) as f:
            return json.load(f)
    except:
        return None

def main():
    once = "--once" in sys.argv
    refresh = 5
    for i, a in enumerate(sys.argv):
        if a == "--refresh" and i+1 < len(sys.argv):
            refresh = int(sys.argv[i+1])
    
    while True:
        data = read_portfolio()
        if not data:
            print(f"{RD}No portfolio_v8.json found. Is the bot running?{R}")
            if once: return
            time.sleep(refresh); continue
        
        positions = data.get("positions", {})
        realized = data.get("realized", 0)
        total_cost = data.get("total_cost", 0)
        n_entries = data.get("entries", 0)
        n_profits = data.get("profit_takes", 0)
        n_cuts = data.get("loss_cuts", 0)
        n_dips = data.get("dip_buys", 0)
        n_arbs = data.get("arb_locks", 0)
        
        pos_list = []
        arb_list = []
        total_deployed = 0
        total_est_value = 0
        type_stats = defaultdict(lambda: {"count":0,"cost":0,"value":0,"w":0,"l":0})
        
        # ── BATCH FETCH all prices in parallel (12x faster) ──
        all_tokens = []
        for mid, p in positions.items():
            if p.get("shares", 0) <= 0: continue
            tok_no = p.get("tok_no", "")
            tok_yes = p.get("tok_yes", "")
            if tok_no: all_tokens.append(tok_no)
            if tok_yes: all_tokens.append(tok_yes)
        
        t0 = time.time()
        price_cache = batch_fetch(all_tokens)
        fetch_ms = (time.time() - t0) * 1000
        
        for mid, p in positions.items():
            if p.get("shares", 0) <= 0: continue
            
            cost = p.get("cost", 0)
            shares = p.get("shares", 0)
            avg_price = p.get("avg_price", 0)
            title = p.get("title", "")
            typ = p.get("typ", "?")
            n_buys = p.get("n_buys", 0)
            n_sells = p.get("n_sells", 0)
            tok_no = p.get("tok_no", "")
            tok_yes = p.get("tok_yes", "")
            created = p.get("created", 0)
            age_h = (time.time() - created) / 3600 if created > 0 else 0
            
            yes_shares = p.get("yes_shares", 0)
            yes_cost = p.get("yes_cost", 0)
            yes_avg_price = p.get("yes_avg_price", 0)
            is_arb = shares > 0 and yes_shares > 0
            
            # Only treat as arb if guaranteed profit is positive
            if is_arb:
                gp_check = min(shares, yes_shares) - (cost + yes_cost)
                if gp_check <= 0:
                    is_arb = False
            total_cost_both = cost + yes_cost
            
            # Live prices from batch cache
            live_bid, live_ask = price_cache.get(tok_no, (None, None))
            
            yes_bid, yes_ask = None, None
            if tok_yes:
                yes_bid, yes_ask = price_cache.get(tok_yes, (None, None))
            
            # P&L using bid (what you'd get selling now)
            if live_bid and live_bid > 0:
                est_val = live_bid * shares
                # Include YES value for ANY two-sided position (arb or not)
                if yes_shares > 0 and yes_bid:
                    est_val += yes_bid * yes_shares
                pnl_pct = (est_val - total_cost_both) / total_cost_both * 100 if total_cost_both > 0 else 0
            else:
                est_val = total_cost_both
                pnl_pct = 0
            
            spread = 0
            if live_bid and live_ask and live_ask > 0:
                spread = (live_ask - live_bid) / live_ask
            
            total_deployed += total_cost_both
            total_est_value += est_val
            pnl_usd = est_val - total_cost_both
            
            type_stats[typ]["count"] += 1
            type_stats[typ]["cost"] += total_cost_both
            type_stats[typ]["value"] += est_val
            if pnl_pct > 0: type_stats[typ]["w"] += 1
            else: type_stats[typ]["l"] += 1
            
            gp, gp_roi = None, 0
            if is_arb:
                gp = min(shares, yes_shares) - total_cost_both
                gp_roi = gp / total_cost_both * 100 if total_cost_both > 0 else 0
            
            # Could we arb this? (NO avg + YES ask)
            combined = None
            if avg_price > 0 and yes_ask and not is_arb:
                combined = avg_price + yes_ask
            
            entry = {
                "title": title[:45], "typ": typ, "cost": cost,
                "total_cost": total_cost_both,
                "shares": shares, "avg_price": avg_price,
                "bid": live_bid, "ask": live_ask, "spread": spread,
                "est_val": est_val, "pnl_pct": pnl_pct, "pnl_usd": pnl_usd,
                "n_buys": n_buys, "n_sells": n_sells, "age_h": age_h,
                "is_arb": is_arb, "yes_shares": yes_shares,
                "yes_avg_price": yes_avg_price, "yes_bid": yes_bid, "yes_ask": yes_ask,
                "guaranteed_profit": gp, "gp_roi": gp_roi,
                "combined": combined,
            }
            
            if is_arb: arb_list.append(entry)
            else: pos_list.append(entry)
        
        pos_list.sort(key=lambda x: x["pnl_pct"])
        arb_list.sort(key=lambda x: -(x["guaranteed_profit"] or 0))
        
        unrealized = total_est_value - total_deployed
        total_positions = len(pos_list) + len(arb_list)
        total_guaranteed = sum(p["guaranteed_profit"] or 0 for p in arb_list)
        winners = [p for p in pos_list if p["pnl_pct"] > 0]
        losers = [p for p in pos_list if p["pnl_pct"] < 0]
        
        # ═══════════════════════════════════════
        # RENDER
        # ═══════════════════════════════════════
        os.system("clear" if os.name != "nt" else "cls")
        now = datetime.now().strftime("%H:%M:%S")
        
        print(f"{B}{CY}╔═══════════════════════════════════════════════════════════════════════════════════════╗{R}")
        print(f"{B}{CY}║  PolyArb v9 WATCH   {now}   ⚡ {fetch_ms:.0f}ms fetch ({len(price_cache)} books parallel)            ║{R}")
        print(f"{B}{CY}╚═══════════════════════════════════════════════════════════════════════════════════════╝{R}")
        
        # ─── PORTFOLIO SUMMARY ───
        deploy_pct = total_deployed / total_cost * 100 if total_cost > 0 else 0
        uc = clr_val(unrealized); rc = clr_val(realized)
        
        print(f"""
  {B}💰 PORTFOLIO{R}
  ┌───────────────────────────────────────────────────────────────────────────┐
  │  Deployed: ${total_deployed:>9,.2f} ({deploy_pct:.0f}%)     Est Value: ${total_est_value:>9,.2f}            │
  │  {uc}Unrealized: ${unrealized:>+9,.2f}{R}             {rc}Realized:  ${realized:>+9,.2f}{R}            │
  │  Positions: {total_positions:>4d}  ({GR}{len(winners)}W{R} / {RD}{len(losers)}L{R})        Win Rate: {len(winners)/max(total_positions,1)*100:.0f}%               │
  └───────────────────────────────────────────────────────────────────────────┘""")
        
        print(f"  {B}📊{R} {n_entries} entries  {n_arbs} arbs  {n_cuts} cuts  "
              f"{n_dips} dips  {n_profits} profit-takes")
        
        # ─── TYPE BREAKDOWN ───
        if type_stats:
            print(f"\n  {B}BY TYPE{R}")
            print(f"  {'Type':<10s} {'#':>3s} {'W/L':>5s} {'Deployed':>10s} {'EstVal':>10s} {'P&L':>9s}")
            for typ in sorted(type_stats, key=lambda t: -type_stats[t]["cost"]):
                ts = type_stats[typ]
                pnl = ts["value"] - ts["cost"]
                c = clr_val(pnl)
                print(f"  {typ:<10s} {ts['count']:>3d} {GR}{ts['w']}{R}/{RD}{ts['l']}{R}"
                      f"  ${ts['cost']:>9,.2f} ${ts['value']:>9,.2f} {c}${pnl:>+8,.2f}{R}")
        
        # ─── ARB-LOCKED ───
        if arb_list:
            gc = clr_val(total_guaranteed)
            print(f"\n  {GR}{B}🔒 ARB-LOCKED ({len(arb_list)})  "
                  f"Total Guaranteed: {gc}${total_guaranteed:+,.2f}{R}")
            print(f"  {'Market':<36s} {'NO':>10s} {'YES':>10s} {'Cost':>8s} {'G.Prof':>9s} {'ROI':>6s}")
            print(f"  {'─'*84}")
            for p in arb_list:
                gp = p["guaranteed_profit"] or 0
                c = clr_val(gp)
                print(f"  {p['title']:<36s} "
                      f"{p['shares']:>5.0f}@{p['avg_price']:.2f} "
                      f"{p['yes_shares']:>5.0f}@{p['yes_avg_price']:.2f} "
                      f"${p['total_cost']:>7.2f} "
                      f"{c}${gp:>+8.2f} {p['gp_roi']:>+5.1f}%{R}")
        
        # ─── ALL POSITIONS ───
        if pos_list:
            print(f"\n  {B}📈 ALL POSITIONS ({len(pos_list)}){R}")
            print(f"  {'Market':<36s} {'Type':>7s} {'Cost':>7s} {'Avg':>6s} {'Bid':>6s} "
                  f"{'Ask':>6s} {'Sprd':>5s} {'P&L%':>7s} {'P&L$':>8s} {'B/S':>4s} {'Age':>5s}")
            print(f"  {'─'*105}")
            
            for p in pos_list:
                c = clr_pnl(p["pnl_pct"])
                bid_s = f"{p['bid']:.3f}" if p['bid'] else " ???"
                ask_s = f"{p['ask']:.3f}" if p['ask'] else " ???"
                avg_s = f"{p['avg_price']:.3f}" if p['avg_price'] else " ???"
                spread_s = f"{p['spread']*100:.0f}%" if p['spread'] else "  ?"
                age_s = f"{p['age_h']:.1f}h" if p['age_h'] < 24 else f"{p['age_h']/24:.0f}d"
                uc = clr_val(p["pnl_usd"])
                
                print(f"  {p['title']:<36s} {p['typ']:>7s} ${p['cost']:>6.2f} "
                      f"{avg_s} {bid_s} {ask_s} {spread_s:>5s} "
                      f"{c}{p['pnl_pct']:>+6.1f}%{R} {uc}${p['pnl_usd']:>+7.2f}{R} "
                      f"{p['n_buys']:>2d}/{p['n_sells']:<1d} {age_s:>5s}")
        
        # ─── ALERTS ───
        near_arb = sorted([p for p in pos_list if p["combined"] and p["combined"] < 1.03],
                          key=lambda x: x["combined"])
        deep_loss = [p for p in pos_list if p["pnl_pct"] <= -55]
        near_cut = [p for p in pos_list if -55 < p["pnl_pct"] <= -40]
        big_win = sorted([p for p in pos_list if p["pnl_pct"] >= 25],
                         key=lambda x: -x["pnl_pct"])
        wide_spread = [p for p in pos_list if p["spread"] and p["spread"] > 0.30]
        
        if near_arb or deep_loss or near_cut or big_win or wide_spread:
            print(f"\n  {B}⚡ ALERTS{R}")
            for p in near_arb[:5]:
                gap = (p["combined"] - 0.97) * 100
                print(f"  {CY}  🔒 NEAR ARB     {p['title']:<30s} NO_avg+YES_ask={p['combined']:.3f} ({gap:+.1f}% to lock){R}")
            for p in big_win[:5]:
                print(f"  {GR}  🎯 BIG WINNER   {p['title']:<30s} {p['pnl_pct']:+.0f}%  ${p['pnl_usd']:+.2f}{R}")
            for p in deep_loss[:5]:
                print(f"  {RD}  ✂️  CUT TARGET   {p['title']:<30s} {p['pnl_pct']:.0f}%  ${p['pnl_usd']:+.2f}{R}")
            for p in near_cut[:3]:
                print(f"  {YL}  📉 NEAR CUT     {p['title']:<30s} {p['pnl_pct']:.0f}%{R}")
            for p in wide_spread[:3]:
                print(f"  {MG}  📊 WIDE SPREAD  {p['title']:<30s} spread={p['spread']*100:.0f}% (P&L unreliable){R}")
        
        # ─── RECENT ACTIONS ───
        logfile = find_log()
        if logfile:
            actions = parse_log_actions(logfile, max_lines=300)
            if actions:
                print(f"\n  {B}📜 RECENT ACTIONS{R} (last 15)")
                for a in actions[-15:]:
                    rest = a[:100]
                    if "📗" in a:     print(f"  {GR}  {rest}{R}")
                    elif "📕" in a:   print(f"  {RD}  {rest}{R}")
                    elif "🔒" in a:   print(f"  {CY}  {rest}{R}")
                    elif "ERR" in a:  print(f"  {RD}{B}  {rest}{R}")
                    elif "⚠" in a:   print(f"  {YL}  {rest}{R}")
                    else:             print(f"  {D}  {rest}{R}")
        
        # ─── PROJECTIONS ───
        all_pos = pos_list + arb_list
        if all_pos:
            w_val = sum(p["pnl_usd"] for p in all_pos if p["pnl_usd"] > 0)
            l_val = sum(p["pnl_usd"] for p in all_pos if p["pnl_usd"] < 0)
            best = sum(p["shares"] for p in all_pos) - total_deployed
            
            print(f"\n  {B}📐 PROJECTIONS{R}")
            print(f"  {GR}  Winners:     ${w_val:>+9,.2f}{R}  ({len(winners)} pos)")
            print(f"  {RD}  Losers:      ${l_val:>+9,.2f}{R}  ({len(losers)} pos)")
            if arb_list:
                print(f"  {CY}  Guaranteed:  ${total_guaranteed:>+9,.2f}{R}  ({len(arb_list)} arbs)")
            print(f"  {D}  Best case:   ${best:>+9,.2f}  (all NOs → $1){R}")
        
        print(f"\n  {D}{'─'*82}")
        print(f"  {total_positions} positions | Refresh {refresh}s | Ctrl+C to exit{R}")
        
        if once: return
        time.sleep(refresh)

if __name__ == "__main__":
    main()
