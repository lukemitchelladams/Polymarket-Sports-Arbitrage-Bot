#!/usr/bin/env python3
"""
DAILY PERFORMANCE LOGGER
=========================
Saves a daily summary after each trading session so you can look back
at days/weeks at a time and track progress.

Reads from: portfolio_v10.json, live_tracker.json, game_data/rn1_portfolio.json
Writes to:  history/daily_YYYYMMDD.json  (one file per day)
            history/cumulative.json      (rolling stats across all days)

Usage:
  python3 daily_log.py                  # save today's snapshot + show summary
  python3 daily_log.py --review         # review all historical days
  python3 daily_log.py --review 7       # review last 7 days
  python3 daily_log.py --week           # weekly summary
  python3 daily_log.py --auto           # run in background, auto-save every hour + at midnight

Run this AFTER your trading session ends (Ctrl+C the arb bot, then run daily_log.py).
Or run it with --auto alongside arb_v10.py for automatic hourly snapshots.
"""

import os, sys, json, time, glob, argparse
from datetime import datetime, timezone, timedelta
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
HISTORY_DIR = os.path.join(SCRIPT_DIR, "history")
PORTFOLIO_FILE = os.path.join(SCRIPT_DIR, "portfolio_v10.json")
LIVE_TRACKER_FILE = os.path.join(SCRIPT_DIR, "live_tracker.json")
RN1_PORTFOLIO_FILE = os.path.join(SCRIPT_DIR, "game_data", "rn1_portfolio.json")
LOG_DIR = os.path.join(SCRIPT_DIR, "logs")

os.makedirs(HISTORY_DIR, exist_ok=True)

# ═══════════════════════════════════════════════════════════════
# COLORS
# ═══════════════════════════════════════════════════════════════
RESET  = "\033[0m";  BOLD   = "\033[1m";  DIM    = "\033[2m"
GREEN  = "\033[92m";  RED    = "\033[91m";  CYAN   = "\033[96m"
YELLOW = "\033[93m";  WHITE  = "\033[97m";  MAGENTA = "\033[95m"


# ═══════════════════════════════════════════════════════════════
# DATA READERS
# ═══════════════════════════════════════════════════════════════
def read_portfolio():
    """Read the current portfolio_v10.json."""
    try:
        with open(PORTFOLIO_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def read_live_tracker():
    """Read live_tracker.json for real-time stats."""
    try:
        with open(LIVE_TRACKER_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def read_rn1_portfolio():
    """Read RN1's tracked portfolio."""
    try:
        with open(RN1_PORTFOLIO_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def count_log_lines_today(date_str):
    """Count how many log lines were written today (proxy for activity level)."""
    pattern = os.path.join(LOG_DIR, f"v10_{date_str.replace('-','')}_*.log")
    total = 0
    for f in glob.glob(pattern):
        try:
            with open(f) as fh:
                total += sum(1 for _ in fh)
        except:
            pass
    # Also check more general pattern
    if total == 0:
        for f in glob.glob(os.path.join(LOG_DIR, "v10_*.log")):
            try:
                mtime = os.path.getmtime(f)
                if datetime.fromtimestamp(mtime).strftime("%Y-%m-%d") == date_str:
                    with open(f) as fh:
                        total += sum(1 for _ in fh)
            except:
                pass
    return total

def get_today_session_data():
    """Read any game_data session files from today."""
    today_str = datetime.now().strftime("%Y%m%d")
    snapshots = []
    for d in glob.glob(os.path.join(SCRIPT_DIR, "game_data", f"session_{today_str}_*")):
        snap_file = os.path.join(d, "snapshots.jsonl")
        if os.path.exists(snap_file):
            try:
                with open(snap_file) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            snapshots.append(json.loads(line))
            except:
                pass
    return snapshots


# ═══════════════════════════════════════════════════════════════
# CAPTURE DAILY SNAPSHOT
# ═══════════════════════════════════════════════════════════════
def capture_daily_snapshot():
    """Capture the current state into a daily snapshot."""
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")

    portfolio = read_portfolio()
    tracker = read_live_tracker()
    rn1 = read_rn1_portfolio()

    positions = portfolio.get("positions", {})

    # Analyze positions
    total_cost = 0
    total_gp = 0
    locked_arbs = 0
    hedged = 0
    one_sided = 0
    wallet_positions = 0
    positions_detail = []

    for mid, p in positions.items():
        cost = p.get("cost", 0) + p.get("yes_cost", 0)
        total_cost += cost

        no_shares = p.get("shares", 0)
        yes_shares = p.get("yes_shares", 0)
        no_avg = p.get("avg_price", 0)
        yes_avg = p.get("yes_avg_price", 0)

        is_two_sided = no_shares > 0 and yes_shares > 0
        combined = no_avg + yes_avg if is_two_sided else 0
        matched = min(no_shares, yes_shares) if is_two_sided else 0
        gp = matched * (1 - combined) if combined > 0 and combined < 1 else 0
        is_locked = is_two_sided and combined < 1.0

        if p.get("from_wallet"):
            wallet_positions += 1
        elif is_locked:
            locked_arbs += 1
            total_gp += gp
        elif is_two_sided:
            hedged += 1
        else:
            one_sided += 1

        positions_detail.append({
            "title": (p.get("title", "?"))[:60],
            "cost": round(cost, 2),
            "no_avg": round(no_avg, 4),
            "yes_avg": round(yes_avg, 4),
            "combined": round(combined, 4),
            "gp": round(gp, 2),
            "locked": is_locked,
            "from_wallet": p.get("from_wallet", False),
        })

    realized = portfolio.get("realized", 0)

    # RN1 stats
    rn1_positions = rn1.get("positions", {})
    rn1_arbs = sum(1 for p in rn1_positions.values() if p.get("is_arb", False))
    rn1_total_cost = sum(p.get("total_cost", 0) for p in rn1_positions.values())

    # Session activity from tracker
    tracker_stats = {
        "cycle_count": tracker.get("cycle", 0),
        "arb_checks": tracker.get("arb_checks", 0),
        "arb_fires": tracker.get("arb_fires", 0),
    }

    log_lines = count_log_lines_today(date_str)

    snapshot = {
        "date": date_str,
        "timestamp": now.isoformat(),
        "unix_ts": time.time(),

        # Our bot performance
        "portfolio": {
            "total_positions": len(positions),
            "locked_arbs": locked_arbs,
            "hedged": hedged,
            "one_sided": one_sided,
            "wallet_positions": wallet_positions,
            "total_cost": round(total_cost, 2),
            "guaranteed_profit": round(total_gp, 2),
            "realized_pnl": round(realized, 2),
            "net_pnl": round(realized + total_gp, 2),
        },

        # Raw portfolio stats from file
        "portfolio_meta": {
            "entries": portfolio.get("entries", 0),
            "profit_takes": portfolio.get("profit_takes", 0),
            "loss_cuts": portfolio.get("loss_cuts", 0),
            "dip_buys": portfolio.get("dip_buys", 0),
            "arb_locks": portfolio.get("arb_locks", 0),
            "accum_buys": portfolio.get("accum_buys", 0),
            "accum_completed": portfolio.get("accum_completed", 0),
        },

        # RN1 intel
        "rn1": {
            "tracked_positions": len(rn1_positions),
            "arb_positions": rn1_arbs,
            "total_deployed": round(rn1_total_cost, 2),
        },

        # Activity level
        "activity": {
            "log_lines": log_lines,
            **tracker_stats,
        },

        # Position details (for drill-down)
        "positions": sorted(positions_detail, key=lambda x: -abs(x["gp"])),
    }

    return snapshot


def save_daily(snapshot):
    """Save snapshot to history/daily_YYYYMMDD.json."""
    date_str = snapshot["date"].replace("-", "")
    path = os.path.join(HISTORY_DIR, f"daily_{date_str}.json")

    # If file already exists for today, keep a list of snapshots (hourly)
    existing = []
    if os.path.exists(path):
        try:
            with open(path) as f:
                data = json.load(f)
            if isinstance(data, list):
                existing = data
            else:
                existing = [data]
        except:
            existing = []

    existing.append(snapshot)

    with open(path, "w") as f:
        json.dump(existing, f, indent=2)

    return path


def update_cumulative(snapshot):
    """Update the rolling cumulative.json with today's final numbers."""
    cum_path = os.path.join(HISTORY_DIR, "cumulative.json")

    try:
        with open(cum_path) as f:
            cumulative = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        cumulative = {"days": [], "summary": {}}

    # Remove any existing entry for today
    date_str = snapshot["date"]
    cumulative["days"] = [d for d in cumulative["days"] if d.get("date") != date_str]

    # Add today's summary (compact version)
    day_entry = {
        "date": date_str,
        "positions": snapshot["portfolio"]["total_positions"],
        "locked_arbs": snapshot["portfolio"]["locked_arbs"],
        "total_cost": snapshot["portfolio"]["total_cost"],
        "guaranteed_profit": snapshot["portfolio"]["guaranteed_profit"],
        "realized_pnl": snapshot["portfolio"]["realized_pnl"],
        "net_pnl": snapshot["portfolio"]["net_pnl"],
        "entries": snapshot["portfolio_meta"]["entries"],
        "arb_locks": snapshot["portfolio_meta"]["arb_locks"],
        "rn1_positions": snapshot["rn1"]["tracked_positions"],
        "rn1_arbs": snapshot["rn1"]["arb_positions"],
        "log_lines": snapshot["activity"]["log_lines"],
    }
    cumulative["days"].append(day_entry)
    cumulative["days"].sort(key=lambda x: x["date"])

    # Update rolling summary
    days = cumulative["days"]
    cumulative["summary"] = {
        "total_days": len(days),
        "first_day": days[0]["date"] if days else "",
        "last_day": days[-1]["date"] if days else "",
        "best_gp_day": max(days, key=lambda x: x["guaranteed_profit"])["date"] if days else "",
        "best_gp": max(d["guaranteed_profit"] for d in days) if days else 0,
        "total_realized": sum(d["realized_pnl"] for d in days) if days else 0,
        "avg_positions": round(sum(d["positions"] for d in days) / len(days), 1) if days else 0,
        "avg_locked_arbs": round(sum(d["locked_arbs"] for d in days) / len(days), 1) if days else 0,
        "total_entries": max(d["entries"] for d in days) if days else 0,
        "total_arb_locks": max(d["arb_locks"] for d in days) if days else 0,
    }

    with open(cum_path, "w") as f:
        json.dump(cumulative, f, indent=2)

    return cum_path


# ═══════════════════════════════════════════════════════════════
# DISPLAY / REVIEW
# ═══════════════════════════════════════════════════════════════
def display_snapshot(snap, compact=False):
    """Pretty-print a single snapshot."""
    p = snap["portfolio"]
    m = snap["portfolio_meta"]
    r = snap["rn1"]
    a = snap["activity"]

    if compact:
        # One-line summary for review mode
        gp_color = GREEN if p["guaranteed_profit"] > 0 else RED
        pnl_color = GREEN if p["net_pnl"] > 0 else RED
        print(f"  {BOLD}{snap['date']}{RESET}  "
              f"{p['total_positions']:3d} pos  "
              f"{p['locked_arbs']:2d} arbs  "
              f"cost ${p['total_cost']:>7.2f}  "
              f"GP {gp_color}${p['guaranteed_profit']:>6.2f}{RESET}  "
              f"realized {pnl_color}${p['realized_pnl']:>7.2f}{RESET}  "
              f"net {pnl_color}${p['net_pnl']:>7.2f}{RESET}")
        return

    print(f"\n{'═'*65}")
    print(f"  {BOLD}{CYAN}DAILY SNAPSHOT — {snap['date']}{RESET}")
    print(f"  {DIM}Captured: {snap['timestamp']}{RESET}")
    print(f"{'═'*65}")

    # Portfolio overview
    print(f"\n  {BOLD}📊 PORTFOLIO{RESET}")
    print(f"  Positions:  {p['total_positions']}  "
          f"({p['locked_arbs']} locked arbs, {p['hedged']} hedged, "
          f"{p['one_sided']} one-sided, {p['wallet_positions']} wallet)")

    gp_color = GREEN if p["guaranteed_profit"] > 0 else RED
    print(f"  Total cost: ${p['total_cost']:.2f}")
    print(f"  Guaranteed: {gp_color}${p['guaranteed_profit']:.2f}{RESET}")

    pnl_color = GREEN if p["realized_pnl"] >= 0 else RED
    print(f"  Realized:   {pnl_color}${p['realized_pnl']:.2f}{RESET}")
    net_color = GREEN if p["net_pnl"] >= 0 else RED
    print(f"  Net P&L:    {net_color}${p['net_pnl']:.2f}{RESET}")

    # Activity stats
    print(f"\n  {BOLD}⚡ ACTIVITY{RESET}")
    print(f"  Entries: {m['entries']}  |  Arb locks: {m['arb_locks']}  |  "
          f"Profit takes: {m['profit_takes']}  |  Loss cuts: {m['loss_cuts']}")
    print(f"  Dip buys: {m['dip_buys']}  |  Accum buys: {m['accum_buys']}")
    if a.get("log_lines"):
        print(f"  Log lines: {a['log_lines']:,}")

    # RN1 intel
    print(f"\n  {BOLD}🔍 RN1 INTEL{RESET}")
    print(f"  Tracked: {r['tracked_positions']} positions  |  {r['arb_positions']} arbs  |  ${r['total_deployed']:.2f} deployed")

    # Top positions by GP
    if snap.get("positions"):
        locked = [p for p in snap["positions"] if p.get("locked")]
        if locked:
            print(f"\n  {BOLD}🔒 TOP LOCKED ARBS{RESET}")
            for pos in locked[:5]:
                gp_c = GREEN if pos["gp"] > 0 else RED
                print(f"    {gp_c}+${pos['gp']:.2f}{RESET}  "
                      f"combined {pos['combined']:.3f}  "
                      f"cost ${pos['cost']:.2f}  "
                      f"{pos['title']}")

    print(f"{'═'*65}\n")


def review_history(n_days=None):
    """Show all historical daily snapshots."""
    files = sorted(glob.glob(os.path.join(HISTORY_DIR, "daily_*.json")))
    if not files:
        print(f"\n  {YELLOW}No history yet. Run daily_log.py after a trading session to start building history.{RESET}\n")
        return

    if n_days:
        files = files[-n_days:]

    print(f"\n{'═'*90}")
    print(f"  {BOLD}{CYAN}TRADING HISTORY — {len(files)} days{RESET}")
    print(f"{'═'*90}")
    print(f"  {'Date':<12} {'Pos':>4} {'Arbs':>5} {'Cost':>10} {'GP':>9} {'Realized':>10} {'Net P&L':>10}")
    print(f"  {'─'*12} {'─'*4} {'─'*5} {'─'*10} {'─'*9} {'─'*10} {'─'*10}")

    total_realized = 0
    total_gp = 0

    for fp in files:
        try:
            with open(fp) as f:
                data = json.load(f)
            # Take the last snapshot of the day
            if isinstance(data, list):
                snap = data[-1]
            else:
                snap = data

            display_snapshot(snap, compact=True)
            total_realized += snap["portfolio"]["realized_pnl"]
            total_gp = snap["portfolio"]["guaranteed_profit"]  # latest GP (not cumulative)
        except Exception as e:
            print(f"  {RED}Error reading {fp}: {e}{RESET}")

    print(f"  {'─'*12} {'─'*4} {'─'*5} {'─'*10} {'─'*9} {'─'*10} {'─'*10}")
    net_color = GREEN if total_realized + total_gp >= 0 else RED
    print(f"  {BOLD}{'TOTALS':<12} {'':>4} {'':>5} {'':>10} "
          f"${total_gp:>8.2f} ${total_realized:>9.2f} "
          f"{net_color}${total_realized + total_gp:>9.2f}{RESET}")
    print(f"{'═'*90}\n")


def weekly_summary():
    """Show a week-by-week summary."""
    files = sorted(glob.glob(os.path.join(HISTORY_DIR, "daily_*.json")))
    if not files:
        print(f"\n  {YELLOW}No history yet.{RESET}\n")
        return

    # Group by week
    weeks = defaultdict(list)
    for fp in files:
        try:
            with open(fp) as f:
                data = json.load(f)
            snap = data[-1] if isinstance(data, list) else data
            date = datetime.strptime(snap["date"], "%Y-%m-%d")
            week_start = date - timedelta(days=date.weekday())
            week_key = week_start.strftime("%Y-%m-%d")
            weeks[week_key].append(snap)
        except:
            pass

    print(f"\n{'═'*80}")
    print(f"  {BOLD}{CYAN}WEEKLY SUMMARIES{RESET}")
    print(f"{'═'*80}")

    for week_key in sorted(weeks.keys()):
        days = weeks[week_key]
        week_end = datetime.strptime(week_key, "%Y-%m-%d") + timedelta(days=6)

        avg_positions = sum(d["portfolio"]["total_positions"] for d in days) / len(days)
        avg_arbs = sum(d["portfolio"]["locked_arbs"] for d in days) / len(days)
        max_cost = max(d["portfolio"]["total_cost"] for d in days)
        last_gp = days[-1]["portfolio"]["guaranteed_profit"]
        last_realized = days[-1]["portfolio"]["realized_pnl"]
        total_entries = max(d["portfolio_meta"]["entries"] for d in days)
        total_arb_locks = max(d["portfolio_meta"]["arb_locks"] for d in days)

        net = last_realized + last_gp
        net_color = GREEN if net >= 0 else RED

        print(f"\n  {BOLD}Week of {week_key} → {week_end.strftime('%Y-%m-%d')}{RESET}  ({len(days)} days)")
        print(f"    Avg positions: {avg_positions:.0f}  |  Avg arbs: {avg_arbs:.0f}  |  Peak cost: ${max_cost:.2f}")
        print(f"    Entries: {total_entries}  |  Arb locks: {total_arb_locks}")
        print(f"    GP: {GREEN}${last_gp:.2f}{RESET}  |  Realized: ${last_realized:.2f}  |  Net: {net_color}${net:.2f}{RESET}")

    print(f"\n{'═'*80}\n")


# ═══════════════════════════════════════════════════════════════
# AUTO MODE (run alongside bot)
# ═══════════════════════════════════════════════════════════════
def auto_mode():
    """Run continuously — save hourly snapshots + final at midnight."""
    print(f"\n  {BOLD}{CYAN}📝 Daily Logger — AUTO MODE{RESET}")
    print(f"  Saving snapshots every hour + at midnight")
    print(f"  History dir: {HISTORY_DIR}")
    print(f"  Press Ctrl+C to stop\n")

    HOUR = 3600
    last_save = 0
    last_date = datetime.now().strftime("%Y-%m-%d")

    while True:
        try:
            now = time.time()
            current_date = datetime.now().strftime("%Y-%m-%d")

            # Date rollover — save end-of-day summary
            if current_date != last_date:
                print(f"\n  {YELLOW}📅 Day rollover: {last_date} → {current_date}{RESET}")
                snap = capture_daily_snapshot()
                snap["date"] = last_date  # Save under yesterday's date
                path = save_daily(snap)
                update_cumulative(snap)
                print(f"  Saved end-of-day: {path}")
                last_date = current_date
                last_save = now

            # Hourly snapshot
            elif now - last_save >= HOUR:
                snap = capture_daily_snapshot()
                path = save_daily(snap)
                update_cumulative(snap)
                ts = datetime.now().strftime("%H:%M")
                p = snap["portfolio"]
                print(f"  {DIM}{ts}{RESET}  "
                      f"{p['total_positions']} pos  "
                      f"{p['locked_arbs']} arbs  "
                      f"GP ${p['guaranteed_profit']:.2f}  "
                      f"realized ${p['realized_pnl']:.2f}")
                last_save = now

            time.sleep(60)  # check every minute

        except KeyboardInterrupt:
            print(f"\n  Saving final snapshot...")
            snap = capture_daily_snapshot()
            save_daily(snap)
            update_cumulative(snap)
            print(f"  Done. Run `python3 daily_log.py --review` to see history.\n")
            break
        except Exception as e:
            print(f"  Error: {e}")
            time.sleep(60)


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Daily Performance Logger")
    parser.add_argument("--review", nargs="?", const=0, type=int,
                       help="Review history (optional: last N days)")
    parser.add_argument("--week", action="store_true", help="Weekly summary")
    parser.add_argument("--auto", action="store_true",
                       help="Auto-save hourly + at midnight (run alongside bot)")
    args = parser.parse_args()

    if args.review is not None:
        review_history(args.review if args.review > 0 else None)
        return

    if args.week:
        weekly_summary()
        return

    if args.auto:
        auto_mode()
        return

    # Default: capture + display today's snapshot
    print(f"\n  {BOLD}Capturing daily snapshot...{RESET}")
    snap = capture_daily_snapshot()
    path = save_daily(snap)
    cum_path = update_cumulative(snap)

    display_snapshot(snap)

    print(f"  {GREEN}✓ Saved to: {path}{RESET}")
    print(f"  {GREEN}✓ Updated:  {cum_path}{RESET}")
    print(f"\n  {DIM}Tip: Run with --review to see all history, --week for weekly summary{RESET}\n")


if __name__ == "__main__":
    main()
