#!/usr/bin/env python3
"""
PolyArb v10 — LIVE DASHBOARD
═════════════════════════════
Real-time view of the arb engine: positions, P&L, locked arbs, discovery stats.

Terminal 1: python3.11 arb_v10.py --bankroll 1000 --fresh --live
Terminal 2: python3.11 watch_v10.py

Options:
  --portfolio PATH   Custom portfolio file (default: portfolio_v10.json)
  --log-dir PATH     Custom log directory (default: logs/)
  --refresh N        Refresh interval in seconds (default: 3)
"""
import os, sys, json, time, glob, re, argparse
from datetime import datetime, timezone
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# ═══════════════════════════════════════════════════════════════
# COLORS
# ═══════════════════════════════════════════════════════════════
RESET  = "\033[0m";  BOLD   = "\033[1m";  DIM    = "\033[2m"
GREEN  = "\033[92m";  RED    = "\033[91m";  CYAN   = "\033[96m"
YELLOW = "\033[93m";  WHITE  = "\033[97m";  MAGENTA = "\033[95m"
BG_GREEN = "\033[42m"; BG_RED = "\033[41m"; BG_CYAN = "\033[46m"

def clear():
    os.system("clear" if os.name != "nt" else "cls")


# ═══════════════════════════════════════════════════════════════
# FILE READERS
# ═══════════════════════════════════════════════════════════════
def find_latest_log(log_dir):
    """Find the most recent v10 log file."""
    candidates = []
    if os.path.isdir(log_dir):
        for f in glob.glob(os.path.join(log_dir, "v10_*.log")):
            candidates.append(f)
    if not candidates:
        return os.path.join(SCRIPT_DIR, "bot.log")
    return max(candidates, key=os.path.getmtime)


def read_portfolio(path):
    """Read portfolio_v10.json — returns (positions_dict, meta_dict)."""
    try:
        with open(path) as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}, {}

    positions = data.get("positions", {})
    meta = {
        "realized": data.get("realized", 0),
        "total_cost": data.get("total_cost", 0),
        "entries": data.get("entries", 0),
        "profit_takes": data.get("profit_takes", 0),
        "loss_cuts": data.get("loss_cuts", 0),
        "dip_buys": data.get("dip_buys", 0),
        "arb_locks": data.get("arb_locks", 0),
    }
    return positions, meta


def read_live_tracker(path):
    """Read live_tracker.json exported by arb_v10.py."""
    try:
        with open(path) as f:
            data = json.load(f)
        age = time.time() - data.get("updated", 0)
        if age > 120:  # stale if >2min old (bot cycles can take time)
            return None
        return data
    except (FileNotFoundError, json.JSONDecodeError, TypeError):
        return None


def read_log_tail(log_file, lines=500):
    """Read last N lines of the log file (efficient tail read)."""
    try:
        with open(log_file, "rb") as f:
            # Seek from end — read ~200 bytes per line estimate
            try:
                f.seek(0, 2)  # end of file
                size = f.tell()
                read_size = min(size, lines * 200)
                f.seek(max(0, size - read_size))
                chunk = f.read().decode("utf-8", errors="replace")
                return chunk.splitlines()[-lines:]
            except Exception:
                f.seek(0)
                all_lines = f.read().decode("utf-8", errors="replace").splitlines()
                return all_lines[-lines:]
    except (FileNotFoundError, IOError):
        return []


# ═══════════════════════════════════════════════════════════════
# LOG PARSERS
# ═══════════════════════════════════════════════════════════════
def parse_log_stats(lines):
    """Extract live stats from log tail."""
    stats = {
        "markets_found": 0,
        "arb_trades": 0,
        "arbs_detected": [],
        "deep_arbs": [],
        "siblings_added": 0,
        "newmarket_added": 0,
        "recent_buys": [],
        "recent_sells": [],
        "discovery_age": "?",
        "cycle_rate": "?",
        "errors": [],
        "running_total": "",
        "mode": "DRY",
        "bankroll": 0,
    }

    for line in lines:
        # Discovery count
        m = re.search(r'(\d+),?\d* sports markets in', line)
        if m:
            stats["markets_found"] = int(m.group(1).replace(",", ""))

        # Mode
        if "Mode: LIVE" in line:
            stats["mode"] = "LIVE"
        if "Mode: DRY" in line:
            stats["mode"] = "DRY"

        # Bankroll
        m = re.search(r'Bankroll: \$([0-9,.]+)', line)
        if m:
            stats["bankroll"] = float(m.group(1).replace(",", ""))

        # ARB DETECTED
        if "ARB DETECTED" in line:
            m = re.search(r'ARB DETECTED: (.+)$', line)
            if m:
                title = m.group(1).strip()
                if title not in stats["arbs_detected"]:
                    stats["arbs_detected"].append(title)

        # NEW ARB locked
        if "NEW ARB!" in line:
            m = re.search(r'NEW ARB! (.+)$', line)
            if m:
                title = m.group(1).strip()
                if title not in stats["arbs_detected"]:
                    stats["arbs_detected"].append(title)

        # BUY orders
        if "BUY" in line and ("📗" in line or "🟢" in line):
            m = re.search(r'@([\d.]+)\s+\$([\d.]+).+\|\s*(.+)$', line)
            if m:
                stats["recent_buys"].append({
                    "price": float(m.group(1)),
                    "amount": float(m.group(2)),
                    "label": m.group(3).strip()[:50],
                    "time": line[:8].strip()
                })

        # Running total
        if "Running total" in line:
            stats["running_total"] = line.strip()

        # ARB ENGINE trades
        m = re.search(r'ARB ENGINE: (\d+) trades', line)
        if m:
            stats["arb_trades"] = max(stats["arb_trades"], int(m.group(1)))

        # Deep arbs found
        if "DEEP ARB" in line:
            m = re.search(r'DEEP ARB: (.+)$', line)
            if m:
                title = m.group(1).strip()[:60]
                if title not in stats["deep_arbs"]:
                    stats["deep_arbs"].append(title)

        # Sibling expansion
        m = re.search(r'Expanded: \+(\d+) sibling', line)
        if m:
            stats["siblings_added"] = max(stats["siblings_added"], int(m.group(1)))

        # New-market scanner
        m = re.search(r'NEW MARKETS: \+(\d+) fresh', line)
        if m:
            stats["newmarket_added"] += int(m.group(1))

        # Errors
        if "ERR" in line or "ERROR" in line:
            stats["errors"].append(line.strip()[-80:])

    # Keep only recent
    stats["recent_buys"] = stats["recent_buys"][-20:]
    stats["errors"] = stats["errors"][-5:]

    return stats


# ═══════════════════════════════════════════════════════════════
# POSITION ANALYSIS
# ═══════════════════════════════════════════════════════════════
FEE_PCT = 0.00  # Fee is on PROFIT not payout — threshold (0.98) already accounts for fees

def analyze_positions(positions):
    """Analyze positions into categories."""
    locked = []
    hedged = []
    hammered = []  # positions that were hammered (intentionally uneven)
    triads = []  # triad arb groups (3-leg soccer arbs)
    open_pos = []
    wallet_open = []  # one-sided wallet positions (bot scanning for completion)
    total_deployed = 0
    total_guaranteed = 0

    # First pass: find triad groups
    triad_groups = defaultdict(list)  # triad_group → [(mid, position_data)]
    for mid, p in positions.items():
        tg = p.get("triad_group", "")
        if tg:
            triad_groups[tg].append((mid, p))

    # Track which mids are part of a triad so we handle them separately
    triad_mids = set()
    for tg, legs in triad_groups.items():
        if len(legs) >= 1:  # any triad leg gets shown in triad section
            for mid, _ in legs:
                triad_mids.add(mid)

    for mid, p in positions.items():
        # Wallet positions: categorize locked/hedged ones normally, show one-sided separately
        if p.get("from_wallet", False):
            shares = p.get("shares", 0)
            yes_shares = p.get("yes_shares", 0)
            # Fix missing cost data — wallet sync sometimes has shares+avg but no cost
            w_no_cost = p.get("cost", 0)
            w_yes_cost = p.get("yes_cost", 0)
            if w_no_cost <= 0 and shares > 0:
                avg_p = p.get("avg_price", 0)
                est = avg_p if avg_p > 0 else (p.get("current_bid", 0) or 0.50)
                w_no_cost = round(shares * est, 2)
            if w_yes_cost <= 0 and yes_shares > 0:
                avg_p = p.get("yes_avg_price", 0)
                est = avg_p if avg_p > 0 else (p.get("current_yes_bid", 0) or 0.50)
                w_yes_cost = round(yes_shares * est, 2)
            w_cost = w_no_cost + w_yes_cost
            if w_cost <= 0:
                continue
            total_deployed += w_cost
            # Two-sided wallet positions go into locked/hedged/hammered
            if shares > 0 and yes_shares > 0:
                payout = min(shares, yes_shares) * (1.0 - FEE_PCT)
                gp = payout - w_cost
                extra_yes = max(0, yes_shares - shares)
                extra_no = max(0, shares - yes_shares)
                pos_data = {
                    "mid": mid, "title": p.get("title", mid[:20]),
                    "typ": p.get("typ", "?"), "no_shares": shares,
                    "yes_shares": yes_shares, "no_avg": p.get("avg_price", 0),
                    "yes_avg": p.get("yes_avg_price", 0), "cost": w_cost,
                    "guaranteed": gp, "roi": (gp / w_cost * 100) if w_cost > 0 else 0,
                    "n_buys": p.get("n_buys", 0) + p.get("n_yes_buys", 0),
                    "extra_yes": extra_yes, "extra_no": extra_no,
                    "total_recovered": p.get("total_recovered", 0),
                    "hammer_spent": p.get("total_hammered", 0),
                    "total_skimmed": p.get("total_skimmed", 0),
                    "total_trimmed": p.get("total_trimmed", 0),
                }
                if gp > 0:
                    locked.append(pos_data)
                    total_guaranteed += gp
                else:
                    # Check if hammered (total_hammered > 0 or big share imbalance)
                    w_is_hammered = (p.get("total_hammered", 0) > 0
                                     or max(extra_yes, extra_no) >= 5)
                    if w_is_hammered:
                        hammered.append(pos_data)
                    else:
                        hedged.append(pos_data)
            else:
                # One-sided wallet position — show in wallet_open
                display_avg = p.get("avg_price", 0)
                side = "NO"
                side_shares = shares
                if shares <= 0 and yes_shares > 0:
                    display_avg = p.get("yes_avg_price", 0)
                    side = "YES"
                    side_shares = yes_shares
                wallet_open.append({
                    "mid": mid, "title": p.get("title", mid[:20]),
                    "typ": p.get("typ", "?"), "side": side,
                    "shares": side_shares, "avg_price": display_avg,
                    "cost": w_cost,
                    "current_bid": p.get("current_bid", 0),
                    "current_yes_bid": p.get("current_yes_bid", 0),
                })
            continue

        shares = p.get("shares", 0)
        yes_shares = p.get("yes_shares", 0)
        cost = p.get("cost", 0)
        yes_cost = p.get("yes_cost", 0)
        # Fix missing cost data — wallet sync sometimes has shares+avg but no cost
        if cost <= 0 and shares > 0:
            avg_p = p.get("avg_price", 0)
            est = avg_p if avg_p > 0 else (p.get("current_bid", 0) or 0.50)
            cost = round(shares * est, 2)
        if yes_cost <= 0 and yes_shares > 0:
            avg_p = p.get("yes_avg_price", 0)
            est = avg_p if avg_p > 0 else (p.get("current_yes_bid", 0) or 0.50)
            yes_cost = round(yes_shares * est, 2)
        total_cost = cost + yes_cost

        if total_cost <= 0:
            continue

        total_deployed += total_cost

        # Skip triad legs here — they get their own section
        if mid in triad_mids:
            continue

        # Check if arb-locked (both sides with guaranteed profit)
        if shares > 0 and yes_shares > 0:
            payout = min(shares, yes_shares) * (1.0 - FEE_PCT)
            gp = payout - total_cost
            # Extra shares beyond the matched pairs (bonus if that side wins)
            extra_yes = max(0, yes_shares - shares)
            extra_no = max(0, shares - yes_shares)
            if gp > 0:
                locked.append({
                    "mid": mid,
                    "title": p.get("title", mid[:20]),
                    "typ": p.get("typ", "?"),
                    "no_shares": shares,
                    "yes_shares": yes_shares,
                    "no_avg": p.get("avg_price", 0),
                    "yes_avg": p.get("yes_avg_price", 0),
                    "cost": total_cost,
                    "guaranteed": gp,
                    "roi": (gp / total_cost * 100) if total_cost > 0 else 0,
                    "n_buys": p.get("n_buys", 0) + p.get("n_yes_buys", 0),
                    "extra_yes": extra_yes,
                    "extra_no": extra_no,
                })
                total_guaranteed += gp
                continue
            else:
                # TWO-SIDED but not profitable
                pos_data = {
                    "mid": mid,
                    "title": p.get("title", mid[:20]),
                    "typ": p.get("typ", "?"),
                    "no_shares": shares,
                    "yes_shares": yes_shares,
                    "no_avg": p.get("avg_price", 0),
                    "yes_avg": p.get("yes_avg_price", 0),
                    "cost": total_cost,
                    "guaranteed": gp,  # will be negative or zero
                    "extra_yes": extra_yes,
                    "extra_no": extra_no,
                    "n_buys": p.get("n_buys", 0) + p.get("n_yes_buys", 0),
                    "total_recovered": p.get("total_recovered", 0),
                    "hammer_spent": p.get("total_hammered", 0),
                    "total_skimmed": p.get("total_skimmed", 0),
                    "total_trimmed": p.get("total_trimmed", 0),
                }
                # Detect hammered: either total_hammered > 0, OR shares are
                # significantly uneven (>= 5 share difference = clearly hammered,
                # even if total_hammered wasn't tracked in an older session)
                is_hammered = (p.get("total_hammered", 0) > 0
                               or max(extra_yes, extra_no) >= 5)
                if is_hammered:
                    hammered.append(pos_data)  # intentionally uneven — show separately
                else:
                    hedged.append(pos_data)    # regular hedge
                continue

        # Open (truly one-sided)
        # For YES-only positions, use yes_avg_price for display
        display_avg = p.get("avg_price", 0)
        if shares <= 0 and yes_shares > 0:
            display_avg = p.get("yes_avg_price", 0)
        open_pos.append({
            "mid": mid,
            "title": p.get("title", mid[:20]),
            "typ": p.get("typ", "?"),
            "shares": shares,
            "yes_shares": yes_shares,
            "avg_price": display_avg,
            "cost": total_cost,
            "n_buys": p.get("n_buys", 0) + p.get("n_yes_buys", 0),
            "from_wallet": p.get("from_wallet", False),
            "is_accumulation": p.get("is_accumulation", False),
            "accum_side": p.get("accum_side", ""),
            "created": p.get("created", 0),
            "current_bid": p.get("current_bid", 0),
            "current_yes_bid": p.get("current_yes_bid", 0),
        })

    # Build triad display data
    for tg, legs in triad_groups.items():
        if len(legs) < 1:
            continue
        triad_cost = 0
        triad_legs_data = []
        min_no_shares = float('inf')
        all_have_no = True
        for mid, p in legs:
            no_sh = p.get("shares", 0)
            no_c = p.get("cost", 0)
            no_avg = p.get("avg_price", 0)
            triad_cost += no_c
            if no_sh > 0:
                min_no_shares = min(min_no_shares, no_sh)
            else:
                all_have_no = False
            triad_legs_data.append({
                "mid": mid,
                "title": p.get("title", mid[:20]),
                "typ": p.get("typ", "?"),
                "no_shares": no_sh,
                "no_avg": no_avg,
                "no_cost": no_c,
            })
        # Calculate guaranteed profit if all legs have NO shares
        # Exactly 1 outcome wins → 2 NOs pay $1 each = $2 payout
        if all_have_no and len(legs) >= 3 and min_no_shares > 0:
            gp = min_no_shares * 2.0 - triad_cost  # 2 legs pay $1 each
            is_locked = gp > 0
        else:
            gp = 0
            is_locked = False
            min_no_shares = 0
        triads.append({
            "triad_group": tg,
            "legs": triad_legs_data,
            "total_cost": triad_cost,
            "guaranteed": gp,
            "is_locked": is_locked,
            "n_legs": len(legs),
            "min_shares": min_no_shares if min_no_shares < float('inf') else 0,
        })
        if is_locked:
            total_guaranteed += gp

    # Sort locked by guaranteed profit desc
    locked.sort(key=lambda x: -x["guaranteed"])
    # Sort hedged by guaranteed (least negative first)
    hedged.sort(key=lambda x: -x["guaranteed"])
    # Sort hammered by potential win profit desc
    hammered.sort(key=lambda x: -(max(x.get("extra_no", 0), x.get("extra_yes", 0)) * (1.0 - FEE_PCT) + x["guaranteed"]))
    # Sort open by cost desc
    open_pos.sort(key=lambda x: -x["cost"])
    # Sort wallet open by cost desc
    wallet_open.sort(key=lambda x: -x["cost"])

    return locked, hedged, hammered, triads, open_pos, wallet_open, total_deployed, total_guaranteed


# ═══════════════════════════════════════════════════════════════
# DASHBOARD RENDERER
# ═══════════════════════════════════════════════════════════════
def render(positions, meta, stats, log_file, tracker=None):
    clear()
    now_str = datetime.now().strftime("%H:%M:%S")

    locked, hedged, hammered, triads, open_pos, wallet_open, deployed, guaranteed = analyze_positions(positions)

    # Game scores from ESPN (exported by arb_v10.py)
    game_scores = tracker.get("game_scores", {}) if tracker else {}

    # Mode: prefer tracker (direct from bot), fall back to log parsing
    if tracker and tracker.get("mode"):
        mode = tracker["mode"]
    else:
        mode = stats.get("mode", "DRY")
    bankroll = stats.get("bankroll", 0) or meta.get("total_cost", 0)
    deploy_pct = (deployed / bankroll * 100) if bankroll > 0 else 0

    mode_color = RED if mode == "LIVE" else YELLOW
    mode_str = f"{mode_color}{BOLD}{mode}{RESET}"

    # ─── Header ───
    print(f"{BOLD}{CYAN}╔═══════════════════════════════════════════════════════════════════════════════╗{RESET}")
    # Runtime timer — read from tracker (bot exports _start_time)
    start_time = tracker.get("start_time", 0) if tracker else 0
    if start_time > 0:
        runtime_s = time.time() - start_time
        rm, rs = divmod(int(runtime_s), 60)
        rh, rm = divmod(rm, 60)
        runtime_str = f"{rh}h{rm:02d}m" if rh > 0 else f"{rm}m{rs:02d}s"
    else:
        runtime_str = "???"
    print(f"{BOLD}{CYAN}║  PolyArb v10 — ARB ENGINE Dashboard    {now_str}  ⏱ {runtime_str:>7s}  {mode_str}            {CYAN}║{RESET}")
    print(f"{BOLD}{CYAN}╚═══════════════════════════════════════════════════════════════════════════════╝{RESET}")
    print(f"  {DIM}Log: {os.path.basename(log_file)}{RESET}")

    # ─── Summary Bar ───
    print(f"\n  {BOLD}BANKROLL:{RESET} ${bankroll:,.0f}  "
          f"{BOLD}DEPLOYED:{RESET} ${deployed:,.2f} ({deploy_pct:.0f}%)  "
          f"{BOLD}REALIZED:{RESET} ${meta.get('realized', 0):,.2f}")

    entries = meta.get("entries", 0)
    arb_locks = meta.get("arb_locks", 0)
    n_triads = len(triads)
    triad_str = f"  {BOLD}TRIADS:{RESET} {GREEN}{n_triads}{RESET}" if n_triads else ""
    hammer_str = f"  {BOLD}🔨 HAMMERED:{RESET} {YELLOW}{len(hammered)}{RESET}" if hammered else ""
    print(f"  {BOLD}ENTRIES:{RESET} {entries}  "
          f"{BOLD}ARB LOCKS:{RESET} {GREEN}{arb_locks}{RESET}  "
          f"{BOLD}HEDGED:{RESET} {CYAN}{len(hedged)}{RESET}{triad_str}{hammer_str}  "
          f"{BOLD}CUTS:{RESET} {meta.get('loss_cuts', 0)}  "
          f"{BOLD}ADDS:{RESET} {meta.get('dip_buys', 0)}")

    markets_found = stats.get("markets_found", 0)
    siblings = stats.get("siblings_added", 0)
    newmkt = stats.get("newmarket_added", 0)
    deep_count = len(stats.get("deep_arbs", []))
    print(f"  {BOLD}MARKETS:{RESET} {markets_found} discovered  "
          f"{BOLD}+SIBLINGS:{RESET} {siblings}  "
          f"{BOLD}+FRESH:{RESET} {newmkt}  "
          f"{BOLD}ARBS SEEN:{RESET} {len(stats.get('arbs_detected', []))}  "
          f"{BOLD}DEEP:{RESET} {deep_count}")

    # ─── GOVERNOR STATUS ───
    gov = tracker.get("governor") if tracker else None
    if gov:
        pos_open = gov.get("positions_open", 0)
        pos_pend = gov.get("positions_pending", 0)
        pos_max = gov.get("positions_max", 3)
        total_pos = pos_open + pos_pend
        pos_clr = GREEN if total_pos < pos_max else RED
        pend_str = f" +{pos_pend}pend" if pos_pend > 0 else ""
        slots_left = max(0, pos_max - total_pos)
        print(f"  {BOLD}SLOTS:{RESET} "
              f"{pos_clr}{pos_open}/{pos_max}{RESET}{pend_str}  "
              f"({slots_left} available)")

    # ─── WS INSTANT ARB STATS ───
    ws_stats = tracker.get("ws_stats") if tracker else None
    if ws_stats:
        ws_checks = ws_stats.get("checks", 0)
        ws_fires = ws_stats.get("fires", 0)
        pct = (ws_fires / ws_checks * 100) if ws_checks > 0 else 0
        fire_clr = GREEN if ws_fires > 0 else DIM
        print(f"  {BOLD}WS:{RESET} {ws_checks:,} checks, "
              f"{fire_clr}{ws_fires} arbs ({pct:.2f}%){RESET}")

    # ─── LOCKED ARBS (the money maker) ───
    if locked:
        print(f"\n  {GREEN}{BOLD}🔒 ARB-LOCKED POSITIONS ({len(locked)}) "
              f"— GUARANTEED: ${guaranteed:+,.2f}{RESET}")
        print(f"  {'Market':<36s} {'Type':>6s} {'NO':>12s} {'YES':>12s} "
              f"{'Cost':>8s} {'Profit':>9s} {'ROI':>7s} {'Buys':>4s}")
        print(f"  {'─'*36} {'─'*6} {'─'*12} {'─'*12} {'─'*8} {'─'*9} {'─'*7} {'─'*4}")

        for p in locked:
            title = p["title"][:36]
            clr = GREEN if p["guaranteed"] > 0 else RED
            print(f"  {title:<36s} {p['typ']:>6s} "
                  f"{p['no_shares']:>5.0f}@{p['no_avg']:.3f} "
                  f"{p['yes_shares']:>5.0f}@{p['yes_avg']:.3f} "
                  f"${p['cost']:>7.2f} "
                  f"{clr}${p['guaranteed']:>+7.2f}{RESET} "
                  f"{clr}{p['roi']:>+6.1f}%{RESET} "
                  f"{p['n_buys']:>4d}")
    else:
        print(f"\n  {DIM}No arb-locked positions yet{RESET}")

    # ─── SEPARATE: bot vs accumulation (wallet positions excluded in analyze_positions) ───
    accum_pos = [p for p in open_pos if p.get("is_accumulation")]
    bot_pos = [p for p in open_pos if not p.get("is_accumulation")]

    # ─── BOT POSITIONS (arb engine trades) ───
    if bot_pos:
        bot_cost = sum(p["cost"] for p in bot_pos)
        print(f"\n  {BOLD}BOT POSITIONS ({len(bot_pos)}) — ${bot_cost:,.2f} deployed{RESET}")
        print(f"  {'Market':<42s} {'Type':>6s} {'Shares':>8s} {'Avg':>6s} {'Cost':>8s} {'Buys':>4s}")
        print(f"  {'─'*42} {'─'*6} {'─'*8} {'─'*6} {'─'*8} {'─'*4}")
        for p in bot_pos[:12]:
            title = p["title"][:42]
            sh_str = f"{p['shares']:.0f}" if p['shares'] > 0 else ""
            if p['yes_shares'] > 0:
                sh_str += ("+" if sh_str else "") + f"{p['yes_shares']:.0f}Y"
            print(f"  {title:<42s} {p['typ']:>6s} {sh_str:>8s} "
                  f"@{p['avg_price']:.3f} ${p['cost']:>7.2f} {p['n_buys']:>4d}")

    # ─── TRIAD POSITIONS (3-leg soccer arbs) ───
    if triads:
        triad_cost = sum(t["total_cost"] for t in triads)
        triad_gp = sum(t["guaranteed"] for t in triads if t["is_locked"])
        n_locked = sum(1 for t in triads if t["is_locked"])
        n_partial = sum(1 for t in triads if not t["is_locked"])
        status = f"{GREEN}🔒 {n_locked} LOCKED{RESET}" if n_locked else ""
        if n_partial:
            status += f" {YELLOW}{n_partial} partial{RESET}"
        gp_str = f" — GUARANTEED: ${GREEN}+{triad_gp:.2f}{RESET}" if triad_gp > 0 else ""
        print(f"\n  {BOLD}🔺 TRIAD ARBS ({len(triads)}) — ${triad_cost:,.2f} deployed  "
              f"{status}{gp_str}{RESET}")
        for tri in triads:
            lock_icon = f"{GREEN}🔒{RESET}" if tri["is_locked"] else f"{YELLOW}⏳{RESET}"
            n = tri["n_legs"]
            gp = tri["guaranteed"]
            roi = (gp / tri["total_cost"] * 100) if tri["total_cost"] > 0 and gp > 0 else 0
            gp_display = f"{GREEN}+${gp:.2f} ({roi:.1f}%){RESET}" if gp > 0 else f"{DIM}building...{RESET}"
            print(f"  {lock_icon} {n}/3 legs  ${tri['total_cost']:.2f} cost  {gp_display}")
            # Show each leg
            for leg in tri["legs"]:
                sh = leg["no_shares"]
                avg = leg["no_avg"]
                title = leg["title"][:40]
                typ_short = leg["typ"][:5]
                if sh > 0:
                    print(f"     {GREEN}✓{RESET} {typ_short:>5s}  {sh:>5.0f} NO @{avg:.3f}  ${leg['no_cost']:.2f}  {title}")
                else:
                    print(f"     {RED}✗{RESET} {typ_short:>5s}  {DIM}--- need NO ---{RESET}  {title}")

    # ─── HEDGED POSITIONS (both sides held, not yet profitable) ───
    if hedged:
        hedged_cost = sum(p["cost"] for p in hedged)
        hedged_total_loss = sum(p["guaranteed"] for p in hedged)
        recovering = sum(1 for p in hedged if p.get("total_recovered", 0) > 0)
        loss_str = f"{RED}${hedged_total_loss:+,.2f} unrealized{RESET}"
        rec_str = f" — {GREEN}{recovering} recovering{RESET}" if recovering else ""
        print(f"\n  {CYAN}{BOLD}🛡️ HEDGED POSITIONS ({len(hedged)}) — ${hedged_cost:,.2f} deployed — {loss_str}{rec_str}{RESET}")
        print(f"  {'Market':<32s} {'Type':>6s} {'NO':>10s} {'YES':>10s} "
              f"{'GP':>8s} {'Recov':>6s} {'Game':>18s}")
        print(f"  {'─'*32} {'─'*6} {'─'*10} {'─'*10} "
              f"{'─'*8} {'─'*6} {'─'*18}")
        for p in hedged:
            title = p["title"][:32]
            no_str = f"{p['no_shares']:.0f}@{p['no_avg']:.2f}"
            yes_str = f"{p['yes_shares']:.0f}@{p['yes_avg']:.2f}"
            gp = p["guaranteed"]
            gp_clr = GREEN if gp >= 0 else RED
            gp_str = f"{gp_clr}${gp:+.2f}{RESET}"
            rec_spent = p.get("total_recovered", 0)
            rec_str = f"{GREEN}${rec_spent:.0f}{RESET}" if rec_spent > 0 else f"{DIM}--{RESET}"
            gs = game_scores.get(p.get("mid", ""), {})
            if gs:
                g_state = gs.get("state", "")
                if g_state == "in":
                    g_str = f"{BOLD}{gs.get('away_score',0)}-{gs.get('home_score',0)}{RESET} {gs.get('detail','')[:8]}"
                elif g_state == "post":
                    g_str = f"{DIM}{gs.get('away_score',0)}-{gs.get('home_score',0)} FINAL{RESET}"
                else:
                    g_str = f"{DIM}pregame{RESET}"
            else:
                g_str = ""
            print(f"  {title:<32s} {p['typ']:>6s} {no_str:>10s} {yes_str:>10s} "
                  f"{gp_str:>8s} {rec_str:>6s} {g_str:>18s}")

    # ─── HAMMERED POSITIONS (winner doubled down — intentionally uneven) ───
    if hammered:
        ham_cost = sum(p["cost"] for p in hammered)
        ham_spent_total = sum(p.get("hammer_spent", 0) for p in hammered)
        ham_potential = 0
        for p in hammered:
            extra = max(p.get("extra_no", 0), p.get("extra_yes", 0))
            ham_potential += extra * (1.0 - FEE_PCT) + p["guaranteed"]
        pot_clr = GREEN if ham_potential > 0 else RED
        print(f"\n  {YELLOW}{BOLD}🔨 HAMMERED POSITIONS ({len(hammered)}) — ${ham_cost:,.2f} deployed "
              f"— ${ham_spent_total:,.2f} hammered — {pot_clr}${ham_potential:+,.2f} if wins{RESET}")
        print(f"  {'Market':<32s} {'Type':>6s} {'NO':>10s} {'YES':>10s} "
              f"{'Worst':>8s} {'If Wins':>12s} {'Spent':>6s} {'Game':>18s}")
        print(f"  {'─'*32} {'─'*6} {'─'*10} {'─'*10} "
              f"{'─'*8} {'─'*12} {'─'*6} {'─'*18}")
        for p in hammered:
            title = p["title"][:32]
            no_str = f"{p['no_shares']:.0f}@{p['no_avg']:.2f}"
            yes_str = f"{p['yes_shares']:.0f}@{p['yes_avg']:.2f}"
            gp = p["guaranteed"]
            gp_str = f"{RED}${gp:+.2f}{RESET}"
            # Calculate "if wins" profit from extra shares
            extra_no = p.get("extra_no", 0)
            extra_yes = p.get("extra_yes", 0)
            extra = max(extra_no, extra_yes)
            side_lbl = "NO" if extra_no > extra_yes else "YES"
            win_profit = extra * (1.0 - FEE_PCT) + gp
            win_clr = GREEN if win_profit > 0 else RED
            win_str = f"{win_clr}${win_profit:+.2f}{RESET} {side_lbl}"
            # Hammer spend
            ham_s = p.get("hammer_spent", 0)
            ham_str = f"{YELLOW}${ham_s:.0f}{RESET}"
            # Game score
            gs = game_scores.get(p.get("mid", ""), {})
            if gs:
                g_state = gs.get("state", "")
                if g_state == "in":
                    g_str = f"{BOLD}{gs.get('away_score',0)}-{gs.get('home_score',0)}{RESET} {gs.get('detail','')[:8]}"
                elif g_state == "post":
                    g_str = f"{DIM}{gs.get('away_score',0)}-{gs.get('home_score',0)} FINAL{RESET}"
                else:
                    g_str = f"{DIM}pregame{RESET}"
            else:
                g_str = ""
            print(f"  {title:<32s} {p['typ']:>6s} {no_str:>10s} {yes_str:>10s} "
                  f"{gp_str:>8s} {win_str:>12s} {ham_str:>6s} {g_str:>18s}")

    # ─── WALLET ONE-SIDED (bot scanning for completion) ───
    if wallet_open:
        wallet_cost = sum(p["cost"] for p in wallet_open)
        print(f"\n  {MAGENTA}{BOLD}👛 WALLET POSITIONS ({len(wallet_open)}) — ${wallet_cost:,.2f} deployed "
              f"(one-sided, bot scanning for arb completion){RESET}")
        print(f"  {'Market':<40s} {'Type':>6s} {'Side':>4s} {'Shares':>7s} {'Avg':>6s} {'Cost':>8s}")
        print(f"  {'─'*40} {'─'*6} {'─'*4} {'─'*7} {'─'*6} {'─'*8}")
        for p in wallet_open[:12]:
            title = p["title"][:40]
            print(f"  {title:<40s} {p['typ']:>6s} {p['side']:>4s} "
                  f"{p['shares']:>5.0f}sh @{p['avg_price']:.3f} ${p['cost']:>7.2f}")

    # ─── ACCUMULATION / ONE-SIDED (waiting for other side) ───
    SLOT_FREE_SECS = 420   # must match LIVE_SLOT_FREE_SECS in arb_v10.py
    STALE_TIMEOUT = 600    # must match LIVE_STALE_TIMEOUT_SECS in arb_v10.py
    if accum_pos:
        accum_cost = sum(p["cost"] for p in accum_pos)
        now_ts = time.time()
        print(f"\n  {YELLOW}{BOLD}📦 ONE-SIDED POSITIONS ({len(accum_pos)}) — ${accum_cost:,.2f} deployed "
              f"(waiting for other side to lock arb){RESET}")
        print(f"  {'Market':<34s} {'Type':>6s} {'Have':>9s} {'Avg':>6s} {'Cost':>8s} {'Need':>5s} {'Age':>7s}  {'Hedge':>10s}")
        print(f"  {'─'*34} {'─'*6} {'─'*9} {'─'*6} {'─'*8} {'─'*5} {'─'*7}  {'─'*10}")
        for p in accum_pos[:8]:
            title = p["title"][:34]
            side = p.get("accum_side", "")
            if not side:
                side = "YES" if p["yes_shares"] > 0 and p["shares"] <= 0 else "NO"
            sh = p["yes_shares"] if side == "YES" else p["shares"]
            need = "NO" if side == "YES" else "YES"
            avg_price = p.get("avg_price", 0)
            current_bid = p.get("current_bid", 0) if side == "NO" else p.get("current_yes_bid", 0)
            # Age + slot status
            created = p.get("created", 0)
            if created > 0:
                age_s = now_ts - created
                age_m = int(age_s // 60)
                age_sec = int(age_s % 60)
                if age_s < SLOT_FREE_SECS:
                    remaining = int(SLOT_FREE_SECS - age_s)
                    age_str = f"{YELLOW}{age_m}:{age_sec:02d}{RESET}"
                    slot_str = f" {DIM}slot frees in {remaining}s{RESET}"
                else:
                    age_str = f"{DIM}{age_m}:{age_sec:02d}{RESET}"
                    slot_str = f" {GREEN}slot free{RESET}"
            else:
                age_str = "???"
                slot_str = ""
            # Hedge status — show stale exit countdown
            hedge_str = ""
            if current_bid and current_bid > 0 and avg_price > 0:
                drop_pct = (avg_price - current_bid) / avg_price
                if drop_pct > 0.15:
                    hedge_str = f" {YELLOW}⚠️{drop_pct:.0%} drop{RESET}"
                if age_s >= STALE_TIMEOUT:
                    if current_bid < 0.55:
                        hedge_str = f" {RED}🗑️STALE{RESET}"
                    else:
                        hedge_str = f" {GREEN}HOLD ✓{RESET}"
                elif age_s >= STALE_TIMEOUT * 0.7:
                    remaining_stale = int(STALE_TIMEOUT - age_s)
                    hedge_str = f" {DIM}exit in {remaining_stale}s{RESET}"
            print(f"  {title:<34s} {p['typ']:>6s} {sh:>5.0f} {side:>3s} "
                  f"@{p['avg_price']:.3f} ${p['cost']:>7.2f} {CYAN}{need:>5s}{RESET} "
                  f"{age_str}{slot_str}{hedge_str}")

    # ─── TYPE BREAKDOWN ───
    all_pos = locked + hedged + hammered + open_pos + wallet_open
    if all_pos:
        type_counts = defaultdict(int)
        type_cost = defaultdict(float)
        type_gp = defaultdict(float)
        for p in locked:
            type_counts[p["typ"]] += 1
            type_cost[p["typ"]] += p["cost"]
            type_gp[p["typ"]] += p["guaranteed"]
        for p in hedged + hammered:
            type_counts[p["typ"]] += 1
            type_cost[p["typ"]] += p["cost"]
        for p in open_pos:
            type_counts[p["typ"]] += 1
            type_cost[p["typ"]] += p["cost"]

        print(f"\n  {BOLD}BY TYPE:{RESET}")
        for typ in sorted(type_counts, key=lambda t: -type_counts[t]):
            gp_str = f"  {GREEN}+${type_gp[typ]:.2f} locked{RESET}" if type_gp.get(typ) else ""
            print(f"    {typ:<10s}: {type_counts[typ]:>3d} pos  ${type_cost[typ]:>8,.2f}{gp_str}")

    # ─── ARBS DETECTED THIS SESSION ───
    arbs_detected = stats.get("arbs_detected", [])
    deep_arbs = stats.get("deep_arbs", [])
    if arbs_detected:
        print(f"\n  {CYAN}{BOLD}🎯 ARBS DETECTED THIS SESSION ({len(arbs_detected)}):{RESET}")
        for title in arbs_detected[-10:]:
            print(f"    {CYAN}→{RESET} {title[:70]}")
        if len(arbs_detected) > 10:
            print(f"    {DIM}... and {len(arbs_detected) - 10} more{RESET}")
    if deep_arbs:
        print(f"\n  {MAGENTA}{BOLD}🔬 DEEP BOOK ARBS ({len(deep_arbs)}):{RESET}")
        for title in deep_arbs[-5:]:
            print(f"    {MAGENTA}→{RESET} {title[:70]}")
        if len(deep_arbs) > 5:
            print(f"    {DIM}... and {len(deep_arbs) - 5} more{RESET}")

    # ─── RECENT TRADES ───
    recent = stats.get("recent_buys", [])
    if recent:
        print(f"\n  {BOLD}RECENT TRADES ({len(recent)}):{RESET}")
        for trade in recent[-8:]:
            t = trade.get("time", "")
            price = trade.get("price", 0)
            amt = trade.get("amount", 0)
            label = trade.get("label", "")[:55]
            is_arb = "ARB" in label or "🔒" in label
            clr = GREEN if is_arb else ""
            end = RESET if is_arb else ""
            print(f"    {DIM}{t}{RESET} {clr}@{price:.3f} ${amt:.2f} {label}{end}")

    # ─── ERRORS ───
    errors = stats.get("errors", [])
    if errors:
        print(f"\n  {RED}{BOLD}⚠ RECENT ERRORS:{RESET}")
        for e in errors[-3:]:
            print(f"    {RED}{e}{RESET}")

    # ─── RUNNING TOTAL ───
    rt = stats.get("running_total", "")
    if rt:
        # Extract key numbers from running total line
        m = re.search(r'(\d+) trades \| (\d+) buys \| (\d+) markets \| \$([\d,.]+) volume', rt)
        if m:
            print(f"\n  {BOLD}SESSION:{RESET} {m.group(1)} trades | {m.group(2)} buys | "
                  f"{m.group(3)} markets | ${m.group(4)} volume")

    # ─── LIVE TRACKER ───
    if tracker and tracker.get("markets"):
        mkts = tracker["markets"]
        # Count tiers
        hot  = [m for m in mkts if m["score"] >= 60]
        warm = [m for m in mkts if 30 <= m["score"] < 60]
        cold = [m for m in mkts if 0 < m["score"] < 30]
        total_tracked = tracker.get("count", len(mkts))
        age_secs = time.time() - tracker.get("updated", 0)

        print(f"\n  {MAGENTA}{BOLD}📡 LIVE TRACKER ({total_tracked} tracked, "
              f"updated {age_secs:.0f}s ago){RESET}")
        print(f"    {GREEN}🔥 HOT:{RESET} {len(hot)}  "
              f"{YELLOW}⚡ WARM:{RESET} {len(warm)}  "
              f"{DIM}❄ COLD:{RESET} {len(cold)}")

        # Show top 12 markets
        show = mkts[:12]
        if show:
            print(f"  {'Score':>5s}  {'Market':<36s} {'Type':>5s} "
                  f"{'YES now':>8s} {'NO now':>8s} "
                  f"{'Y low':>7s} {'N low':>7s} {'Floor':>7s} {'Vol':>6s}")
            print(f"  {'─'*5}  {'─'*36} {'─'*5} "
                  f"{'─'*8} {'─'*8} "
                  f"{'─'*7} {'─'*7} {'─'*7} {'─'*6}")

            for m in show:
                sc = m["score"]
                if sc >= 60:
                    sc_clr = GREEN
                elif sc >= 30:
                    sc_clr = YELLOW
                else:
                    sc_clr = DIM

                title = m.get("title", "?")[:36]
                typ = m.get("typ", "?")

                yes_now = m.get("yes_now")
                no_now = m.get("no_now")
                yes_str = f"${yes_now:.3f}" if yes_now else "  ---"
                no_str = f"${no_now:.3f}" if no_now else "  ---"

                y_low = m.get("yes_low")
                n_low = m.get("no_low")
                yl_str = f"${y_low:.3f}" if y_low else "  ---"
                nl_str = f"${n_low:.3f}" if n_low else "  ---"

                floor = m.get("combined_floor")
                if floor and floor < 900:
                    floor_clr = GREEN if floor < 0.92 else (YELLOW if floor < 0.97 else "")
                    floor_end = RESET if floor_clr else ""
                    fl_str = f"{floor_clr}${floor:.3f}{floor_end}"
                else:
                    fl_str = "  ---  "

                vol_y = m.get("vol_yes", 0)
                vol_n = m.get("vol_no", 0)
                vol = max(vol_y, vol_n)
                vol_str = f"{vol:.1%}" if vol > 0 else " ---"

                # Momentum / falling-side / triad indicators
                mom_side = m.get("momentum_side")
                mom_sc = m.get("momentum_score", 0)
                fall_side = m.get("falling_side")
                triad_comb = m.get("triad_combined")
                mom_tag = ""
                if triad_comb and triad_comb > 0:
                    tri_clr = GREEN if triad_comb < 0.98 else YELLOW
                    mom_tag += f" {tri_clr}🔺{triad_comb:.2f}{RESET}"
                if mom_side and mom_sc > 0:
                    mom_tag += f" {GREEN}⚡{mom_side[0]}+{mom_sc}{RESET}"
                if fall_side:
                    mom_tag += f" {RED}⬇{fall_side[0]}{RESET}"

                print(f"  {sc_clr}{sc:>5d}{RESET}  {title:<36s} {typ:>5s} "
                      f"{yes_str:>8s} {no_str:>8s} "
                      f"{yl_str:>7s} {nl_str:>7s} {fl_str:>7s} {vol_str:>6s}{mom_tag}")

            if len(mkts) > 12:
                remaining_hot = len([m for m in mkts[12:] if m["score"] >= 60])
                print(f"  {DIM}  ... and {len(mkts) - 12} more tracked"
                      f"{f' ({remaining_hot} HOT)' if remaining_hot else ''}{RESET}")

    # ─── FOOTER ───
    print(f"\n  {'═'*78}")
    # Calculate hammer potential for footer
    ham_pot = 0
    for p in hammered:
        extra = max(p.get("extra_no", 0), p.get("extra_yes", 0))
        ham_pot += extra * (1.0 - FEE_PCT) + p["guaranteed"]
    total_value = deployed + guaranteed  # conservative: deployed + locked profit
    ham_pot_str = f"  │  {BOLD}🔨 IF WINS:{RESET} {YELLOW}${ham_pot:+,.2f}{RESET}" if hammered else ""
    # Skim and trim totals across all positions
    all_pos = locked + hedged + hammered + triads + open_pos + wallet_open
    total_skimmed = sum(p.get("total_skimmed", 0) for p in all_pos)
    total_trimmed = sum(p.get("total_trimmed", 0) for p in all_pos)
    extras = ""
    if total_skimmed > 0:
        extras += f"  │  💰 Skimmed: ${total_skimmed:,.2f}"
    if total_trimmed > 0:
        extras += f"  │  ✂️ Trimmed: ${total_trimmed:,.2f}"
    print(f"  {BOLD}TOTAL GUARANTEED:{RESET} {GREEN}${guaranteed:+,.2f}{RESET}  │  "
          f"{BOLD}DEPLOYED:{RESET} ${deployed:,.2f}  │  "
          f"{BOLD}EST VALUE:{RESET} ${total_value:,.2f}{ham_pot_str}")
    if extras:
        print(f"  {DIM}{extras.strip('  │  ')}{RESET}")
    print(f"  {DIM}Refreshing every {args.refresh}s  |  Ctrl+C to stop{RESET}")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PolyArb v10 Live Dashboard")
    parser.add_argument("--portfolio", default=os.path.join(SCRIPT_DIR, "portfolio_v10.json"),
                        help="Path to portfolio file")
    parser.add_argument("--log-dir", default=os.path.join(SCRIPT_DIR, "logs"),
                        help="Path to logs directory")
    parser.add_argument("--refresh", type=int, default=3,
                        help="Refresh interval in seconds")
    args = parser.parse_args()

    print(f"Starting v10 dashboard... (Ctrl+C to stop)")
    print(f"  Portfolio: {args.portfolio}")
    print(f"  Logs: {args.log_dir}")

    tracker_path = os.path.join(SCRIPT_DIR, "live_tracker.json")

    try:
        while True:
            log_file = find_latest_log(args.log_dir)
            positions, meta = read_portfolio(args.portfolio)
            log_lines = read_log_tail(log_file, lines=1000)
            stats = parse_log_stats(log_lines)
            tracker = read_live_tracker(tracker_path)
            render(positions, meta, stats, log_file, tracker)
            time.sleep(args.refresh)
    except KeyboardInterrupt:
        print(f"\n{DIM}Dashboard stopped.{RESET}")
