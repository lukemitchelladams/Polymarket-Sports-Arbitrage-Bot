#!/usr/bin/env python3
"""
PolyArb v5 — NO-ONLY Live Dashboard
═══════════════════════════════════════
Reads ledger.json + bot.log in real-time.

Terminal 1: python3.11 arb_v5.py --bankroll 1000 --fresh
Terminal 2: python3.11 watch.py

Shows:
  • Active NO positions with entry price, size, expected return
  • Signal breakdown (MID / NC / FAV / PENNY / HAMMER)
  • Rate limiter status
  • Startup ramp progress
  • Resolved positions with win/loss tracking
"""
import os, time, re, json, sys
from datetime import datetime
from collections import Counter

# ─── PATHS ──────────────────────────────────────────────────────────
SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
LEDGER_FILE = os.path.join(SCRIPT_DIR, "ledger.json")

def find_latest_log():
    """Find the most recently modified .log file in logs/ directory."""
    import glob
    candidates = []
    # Check logs/ subdirectory
    log_dir = os.path.join(SCRIPT_DIR, "logs")
    if os.path.isdir(log_dir):
        for f in glob.glob(os.path.join(log_dir, "arb_*.log")):
            candidates.append(f)
    # Also check bot.log in script dir
    bot_log = os.path.join(SCRIPT_DIR, "bot.log")
    if os.path.exists(bot_log):
        candidates.append(bot_log)
    if not candidates:
        return bot_log  # fallback
    # Return most recently modified
    return max(candidates, key=os.path.getmtime)

LOG_FILE = find_latest_log()

# Also check ~/polycopy/ if not found locally
if not os.path.exists(LEDGER_FILE):
    alt = os.path.expanduser("~/polycopy/ledger.json")
    if os.path.exists(alt):
        LEDGER_FILE = alt
        LOG_FILE = find_latest_log()

# ─── COLORS ─────────────────────────────────────────────────────────
RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"
GREEN = "\033[92m"; RED = "\033[91m"; CYAN = "\033[96m"
YELLOW = "\033[93m"; WHITE = "\033[97m"; MAGENTA = "\033[95m"

def clear():
    os.system("clear" if os.name != "nt" else "cls")


# ─── LEDGER READER ──────────────────────────────────────────────────
def read_ledger():
    try:
        with open(LEDGER_FILE) as f:
            data = json.load(f)
    except:
        return {}
    positions = {}
    for mid, d in data.items():
        title = d.get("title", "Unknown")[:55]
        nf = d.get("no_fills", [])
        yf = d.get("yes_fills", [])  # legacy from v3/v4

        n_usdc = sum(a for _, a in nf)
        n_avg  = sum(p * a for p, a in nf) / n_usdc if n_usdc else 0
        n_buys = len(nf)

        y_usdc = sum(a for _, a in yf)

        total = n_usdc + y_usdc
        resolved = d.get("resolved", False)
        resolved_pl = d.get("resolved_pl", None)
        stagnant = d.get("stagnant", False)

        # Expected return if our NO resolves to $1
        if n_avg > 0 and n_usdc > 0:
            no_shares = n_usdc / n_avg
            expected_profit = no_shares - n_usdc
            expected_return_pct = (expected_profit / n_usdc) * 100
        else:
            no_shares = expected_profit = expected_return_pct = 0

        # P&L
        if resolved_pl is not None:
            est = resolved_pl
        elif resolved:
            est = -n_usdc  # conservative: assume loss if unknown
        else:
            est = 0  # unrealized — don't count until resolved

        positions[mid] = {
            "title": title,
            "no_usdc": n_usdc, "no_avg": n_avg, "no_buys": n_buys,
            "no_shares": no_shares,
            "yes_usdc": y_usdc,
            "total": total,
            "expected_profit": round(expected_profit, 2),
            "expected_return_pct": round(expected_return_pct, 1),
            "est": round(est, 2),
            "resolved": resolved,
            "resolved_pl": resolved_pl,
            "stagnant": stagnant,
        }
    return positions


# ─── LOG READER ─────────────────────────────────────────────────────
def read_log_stats():
    stats = {}
    recent_signals = Counter()

    try:
        with open(LOG_FILE, "rb") as f:
            f.seek(0, 2)
            fsize = f.tell()
            f.seek(max(0, fsize - 120_000))
            tail = f.read().decode("utf-8", errors="replace")
        lines = tail.split("\n")
    except:
        return stats, recent_signals

    for line in reversed(lines):
        # Parse the STATS line
        if "STATS:" in line and not stats:
            # Parse signal counts
            m_signals = re.findall(r'(Dutch|Spike|OBI|NC|Fav|Mid|Penny|Sell):(\d+)', line)
            signal_dict = {k: int(v) for k, v in m_signals}
            
            # Parse other fields
            runtime = re.search(r'Runtime:([\d.]+\w+)', line)
            markets = re.search(r'Markets:(\d+)', line)
            ws = re.search(r'WS:(\d+)', line)
            fills = re.search(r'Fills:(\d+)', line)
            signals_total = re.search(r'Signals:(\d+)', line)
            deployed = re.search(r'\|\s+\$([\d.]+)', line)
            active = re.search(r'Active:(\d+)/(\d+)', line)
            bankroll = re.search(r'BRL:\$([\d,.]+)', line)
            pl = re.search(r'P&L:\$([+\-\d.]+)', line)
            rej = re.search(r'Rej\[rate:(\d+)\s+sz:(\d+)\s+deploy:(\d+)\s+event:(\d+)\]', line)
            
            stats = {
                "runtime": runtime.group(1) if runtime else "?",
                "markets": markets.group(1) if markets else "?",
                "ws": ws.group(1) if ws else "?",
                "dutch": signal_dict.get("Dutch", 0),
                "spike": signal_dict.get("Spike", 0),
                "obi": signal_dict.get("OBI", 0),
                "nc": signal_dict.get("NC", 0),
                "fav": signal_dict.get("Fav", 0),
                "mid": signal_dict.get("Mid", 0),
                "penny": signal_dict.get("Penny", 0),
                "sell": signal_dict.get("Sell", 0),
                "fills": fills.group(1) if fills else "0",
                "signals": signals_total.group(1) if signals_total else "?",
                "deployed": deployed.group(1) if deployed else "0",
                "active": active.group(1) if active else "?",
                "max_active": active.group(2) if active else "?",
                "bankroll": bankroll.group(1) if bankroll else "?",
                "pl": pl.group(1) if pl else "+0",
                "rej_rate": int(rej.group(1)) if rej else 0,
                "rej_size": int(rej.group(2)) if rej else 0,
                "rej_deploy": int(rej.group(3)) if rej else 0,
                "rej_event": int(rej.group(4)) if rej else 0,
            }

        # Count recent signal types
        for sig_tag in ["CORE MID:", "NEAR-CERTAIN:", "FAVORED SIDE:", "PENNY SCOOP:",
                        "HAMMER NO:", "ADDING NO:", "BATCH SELL:"]:
            if sig_tag in line:
                recent_signals[sig_tag.rstrip(":")] += 1

    return stats, recent_signals


# ─── RENDER ─────────────────────────────────────────────────────────
def render(positions, stats_tuple):
    stats, recent_signals = stats_tuple
    clear()
    now_str = datetime.now().strftime("%H:%M:%S")

    # ── Header ──
    print(f"{BOLD}{CYAN}╔══════════════════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{CYAN}║  PolyArb v5 — NO-ONLY Dashboard          {now_str}                      ║{RESET}")
    print(f"{BOLD}{CYAN}╚══════════════════════════════════════════════════════════════════════════╝{RESET}")
    print(f"  {DIM}Log: {os.path.basename(LOG_FILE)}{RESET}")

    # ── Stats bar ──
    if stats:
        pl = stats.get("pl", "+0")
        pl_c = GREEN if not pl.startswith("-") else RED
        print(f"\n  {DIM}Up:{RESET} {stats['runtime']}  "
              f"{DIM}Mkts:{RESET} {stats['markets']}  "
              f"{DIM}WS:{RESET} {stats['ws']}  "
              f"{DIM}Fills:{RESET} {BOLD}{stats['fills']}{RESET}  "
              f"{DIM}Open:{RESET} {stats['active']}/{stats['max_active']}  "
              f"{DIM}Dep:{RESET} ${stats['deployed']}  "
              f"{DIM}BRL:{RESET} ${stats['bankroll']}  "
              f"{DIM}P&L:{RESET} {pl_c}{BOLD}${pl}{RESET}")

        # Signal breakdown bar
        parts = []
        for key, label, clr in [
            ("mid",   "MID",  WHITE),  ("nc",    "NC",   GREEN),
            ("fav",   "FAV",  CYAN),   ("penny", "PEN",  YELLOW),
            ("dutch", "DUTCH",MAGENTA), ("sell",  "SELL", RED),
        ]:
            val = stats.get(key, 0)
            if val > 0:
                parts.append(f"{clr}{label}:{val}{RESET}")
        total_signals = stats.get("mid",0) + stats.get("nc",0) + stats.get("fav",0) + stats.get("penny",0)
        if parts:
            print(f"  {DIM}Signals:{RESET} {'  '.join(parts)}  {DIM}(total entries: {total_signals}){RESET}")
        
        # Rejection breakdown
        rej_r = stats.get("rej_rate", 0)
        rej_s = stats.get("rej_size", 0)
        rej_d = stats.get("rej_deploy", 0)
        rej_e = stats.get("rej_event", 0)
        rej_total = rej_r + rej_s + rej_d + rej_e
        if rej_total > 0:
            print(f"  {DIM}Rejects:{RESET} {RED}rate:{rej_r}{RESET}  sz:{rej_s}  deploy:{rej_d}  event:{rej_e}  {DIM}(total: {rej_total}){RESET}")
    else:
        print(f"\n  {DIM}Waiting for bot stats line...{RESET}")

    # ── Categorize ──
    active_pos = {k: v for k, v in positions.items() if not v["resolved"]}
    resolved_pos = {k: v for k, v in positions.items() if v["resolved"]}

    # ── Active NO Positions Table ──
    no_positions = {k: v for k, v in active_pos.items() if v["no_usdc"] > 0}
    total_no = sum(v["no_usdc"] for v in no_positions.values())
    total_shares = sum(v["no_shares"] for v in no_positions.values())
    total_exp = sum(v["expected_profit"] for v in no_positions.values())

    if no_positions:
        print(f"\n  {BOLD}NO POSITIONS  ({len(no_positions)})    "
              f"Cost: ${total_no:.2f}   "
              f"Shares: {total_shares:.0f}   "
              f"If all win: {GREEN}+${total_exp:.2f}{RESET}")
        print(f"  {'Market':<53} {'#':>2} {'Avg':>6} {'Cost':>7} {'Shrs':>5} {'IfWin':>7} {'Ret':>6}")
        print(f"  {'─'*53} {'─'*2} {'─'*6} {'─'*7} {'─'*5} {'─'*7} {'─'*6}")

        for mid, v in sorted(no_positions.items(), key=lambda x: -x[1]["no_usdc"]):
            ret = v["expected_return_pct"]
            rc = GREEN if ret > 50 else (WHITE if ret > 10 else YELLOW)
            stag = f" {DIM}●{RESET}" if v["stagnant"] else ""
            print(f"  {v['title']:<53} {v['no_buys']:>2} @{v['no_avg']:.3f} "
                  f"${v['no_usdc']:>6.2f} {v['no_shares']:>5.0f} "
                  f"{rc}+${v['expected_profit']:>5.2f} {ret:>5.1f}%{RESET}{stag}")

    # Legacy YES check
    legacy = {k: v for k, v in active_pos.items() if v["yes_usdc"] > 0}
    if legacy:
        print(f"\n  {RED}{BOLD}⚠ LEGACY YES ({len(legacy)}) — leftover from v3/v4, will resolve out{RESET}")
        for mid, v in legacy.items():
            print(f"    {RED}{v['title']:<53} YES ${v['yes_usdc']:.2f}{RESET}")

    # ── Price Distribution ──
    if no_positions:
        buckets = [
            ("Penny <10¢",  0.00, 0.10),
            ("Low  10-20¢", 0.10, 0.20),
            ("Mid  20-60¢", 0.20, 0.60),
            ("Fav  60-80¢", 0.60, 0.80),
            ("NC   80-97¢", 0.80, 1.00),
        ]
        print(f"\n  {BOLD}PRICE MIX{RESET}  ", end="")
        pieces = []
        for label, lo, hi in buckets:
            cnt = sum(1 for v in no_positions.values() if lo <= v["no_avg"] < hi)
            vol = sum(v["no_usdc"] for v in no_positions.values() if lo <= v["no_avg"] < hi)
            if cnt > 0:
                pct = cnt / len(no_positions) * 100
                pieces.append(f"{label}:{cnt}({pct:.0f}%) ${vol:.0f}")
        print(f"  {'  │  '.join(pieces)}")

    # ── Resolved ──
    if resolved_pos:
        wins   = {k: v for k, v in resolved_pos.items() if v["est"] >= 0}
        losses = {k: v for k, v in resolved_pos.items() if v["est"] < 0}
        total_pl = sum(v["est"] for v in resolved_pos.values())
        win_pl   = sum(v["est"] for v in wins.values())
        loss_pl  = sum(v["est"] for v in losses.values())
        wr = len(wins) / len(resolved_pos) * 100 if resolved_pos else 0

        pl_c = GREEN if total_pl >= 0 else RED
        wr_c = GREEN if wr >= 50 else RED
        print(f"\n  {BOLD}RESOLVED  ({len(resolved_pos)}){RESET}   "
              f"{GREEN}W:{len(wins)}{RESET}/{RED}L:{len(losses)}{RESET}   "
              f"Rate: {wr_c}{wr:.0f}%{RESET}   "
              f"P&L: {pl_c}${total_pl:+.2f}{RESET}  "
              f"({GREEN}+${win_pl:.2f}{RESET} / {RED}${loss_pl:.2f}{RESET})")

        # Last 8 resolved
        recent = sorted(resolved_pos.items(), key=lambda x: x[0])[-8:]
        for mid, v in recent:
            c = GREEN if v["est"] >= 0 else RED
            tag = "WIN " if v["est"] >= 0 else "LOSS"
            pl_show = f"${v['est']:+.2f}" if v["resolved_pl"] is not None else f"${v['est']:+.2f}?"
            print(f"    {c}{tag}{RESET} {DIM}{v['title']:<50} NO@{v['no_avg']:.3f} ${v['no_usdc']:.2f} → {c}{pl_show}{RESET}")

    # ── Footer ──
    total_deployed = sum(v["total"] for v in active_pos.values())
    realized = sum(v["est"] for v in resolved_pos.values()) if resolved_pos else 0
    unrealized = sum(v["expected_profit"] for v in no_positions.values()) if no_positions else 0

    print(f"\n  {'═'*75}")
    rc = GREEN if realized >= 0 else RED
    print(f"  {BOLD}DEPLOYED: ${total_deployed:.2f}  │  "
          f"UNREALIZED: {GREEN}${unrealized:+.2f}{RESET}{BOLD}  │  "
          f"REALIZED: {rc}${realized:+.2f}{RESET}{BOLD}  │  "
          f"NET: {GREEN if (realized+unrealized)>=0 else RED}${realized+unrealized:+.2f}{RESET}")
    print()


# ─── MAIN ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not os.path.exists(LEDGER_FILE):
        print(f"Ledger not found: {LEDGER_FILE}")
        print(f"Run arb_v5.py first, or place watch.py in the same directory.")
        sys.exit(1)

    print(f"Reading {LEDGER_FILE} — refresh every 2s  (Ctrl+C to exit)")
    time.sleep(1)
    try:
        while True:
            render(read_ledger(), read_log_stats())
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nDone.")
