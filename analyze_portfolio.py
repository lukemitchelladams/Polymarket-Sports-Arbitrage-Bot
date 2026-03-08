#!/usr/bin/env python3
"""
Portfolio P&L Analyzer — traces every trade across all log files
and calculates position-by-position profit/loss.

Run: python3 analyze_portfolio.py
"""

import re
import os
import glob
from collections import defaultdict
from datetime import datetime

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")

# Patterns to extract trades
# BUY: 📗 LIVE BUY AGG @0.530 $10.00 (18sh) ... | label
# SELL: 📕 LIVE SELL LMT @0.260 15sh | label
# VERIFIED FILL: ✅ VERIFIED FILL: 15.5sh $4.95 (status=MATCHED) | label
# HAMMER: 🔨 HAMMER! ... NO@0.930 +15sh $13.95
# TRIM: ✂️ TRIM! ... sold 15 YES@0.260 = $3.90
# WRITE-OFF: ✂️ WRITE-OFF: ... $2.46 loss written off
# LOCKED: 🔒 LOCKED! ...
# SKIM: 🥛 SKIM! ...

BUY_PATTERN = re.compile(
    r'(\d{2}:\d{2}:\d{2}).*📗 LIVE BUY (?:AGG|LMT) @([\d.]+) \$([\d.]+) \((\d+)sh\).*\| (.+)'
)
VERIFIED_FILL = re.compile(
    r'(\d{2}:\d{2}:\d{2}).*✅ (?:VERIFIED|BATCH VERIFIED) FILL: ([\d.]+)sh \$([\d.]+).*\| (.+)'
)
SELL_PATTERN = re.compile(
    r'(\d{2}:\d{2}:\d{2}).*📕 LIVE SELL.*@([\d.]+) (\d+)sh.*\| (.+)'
)
HAMMER_DONE = re.compile(
    r'(\d{2}:\d{2}:\d{2}).*🔨 HAMMER! (.+?) (NO|YES)@([\d.]+) \+(\d+)sh \$([\d.]+)'
)
HAMMER_SENT = re.compile(
    r'(\d{2}:\d{2}:\d{2}).*🔨 HAMMER SENT but verification failed: (.+?) (NO|YES)@([\d.]+)'
)
HAMMER_FAILED = re.compile(
    r'(\d{2}:\d{2}:\d{2}).*🔨 HAMMER FAILED'
)
TRIM_DONE = re.compile(
    r'(\d{2}:\d{2}:\d{2}).*✂️ TRIM! (.+?) sold (\d+) (YES|NO)@([\d.]+) = \$([\d.]+)'
)
WRITEOFF = re.compile(
    r'(\d{2}:\d{2}:\d{2}).*✂️ WRITE-OFF: (.+?) (\d+) (YES|NO) shares.*\$([\d.]+) loss'
)
LOCKED = re.compile(
    r'(\d{2}:\d{2}:\d{2}).*🔒 LOCKED! (.+?) NO@([\d.]+)\+YES@([\d.]+).*→ \$([-\d.]+) guaranteed'
)
NEW_POS = re.compile(
    r'(\d{2}:\d{2}:\d{2}).*📦 .*LIVE NEW (NO|YES): (.+?) @([\d.]+) (\d+)sh \$([\d.]+)'
)

class Position:
    def __init__(self, name):
        self.name = name
        self.no_shares = 0
        self.yes_shares = 0
        self.no_cost = 0.0
        self.yes_cost = 0.0
        self.no_avg = 0.0
        self.yes_avg = 0.0
        self.trim_revenue = 0.0
        self.trim_count = 0
        self.hammer_cost = 0.0
        self.hammer_count = 0
        self.hammer_unverified = 0
        self.writeoff_loss = 0.0
        self.gp = None  # guaranteed profit if locked
        self.buys = []   # (time, side, shares, price, cost, label)
        self.sells = []  # (time, side, shares, price, revenue)
        self.is_locked = False
        self.resolved = False
        self.outcome = None  # which side won

    @property
    def total_cost(self):
        return self.no_cost + self.yes_cost

    @property
    def total_shares(self):
        return self.no_shares + self.yes_shares

    def payout_if_no_wins(self):
        return self.no_shares * 1.0  # NO pays $1 each

    def payout_if_yes_wins(self):
        return self.yes_shares * 1.0  # YES pays $1 each

    def best_payout(self):
        return max(self.payout_if_no_wins(), self.payout_if_yes_wins())

    def worst_payout(self):
        return min(self.payout_if_no_wins(), self.payout_if_yes_wins())

    def best_pnl(self):
        return self.best_payout() + self.trim_revenue - self.total_cost

    def worst_pnl(self):
        return self.worst_payout() + self.trim_revenue - self.total_cost

    def current_value(self, no_price=None, yes_price=None):
        """Market value at current prices."""
        no_p = no_price or self.no_avg
        yes_p = yes_price or self.yes_avg
        return self.no_shares * no_p + self.yes_shares * yes_p


def normalize_name(label):
    """Extract market name from label."""
    # Try to get name from after the last |
    parts = label.split("|")
    if len(parts) >= 2:
        name = parts[-1].strip()
    else:
        name = label.strip()
    # Remove score/common suffixes
    name = re.sub(r'\s*\(score=.*', '', name)
    name = re.sub(r'\s*\[.*', '', name)
    return name[:50]


def extract_market_from_label(label):
    """Get the market title from various label formats."""
    # 🔨HAMMER-NO|@0.840|vel=0.0000|Trail Blazers vs. Rockets
    # 📦⚡MOM:YESLIVE-NO|@0.530|pot=0.940|sc=189|Spread: Suns (-6.5)
    # ✂️TRIM-YES|@0.260|75%|Pelicans vs. Suns: O/U 22
    # 🔒ARB-LOCK-YES|2.3%|slip+0.02|...
    # ⚡WS-NO|@0.310|Clippers vs. Spurs
    parts = label.split("|")
    # Last part is usually the market name
    if parts:
        name = parts[-1].strip()
        # Clean up truncated names and common artifacts
        name = re.sub(r'\s*\(score=.*', '', name)
        name = re.sub(r'\s*\[.*', '', name)
        return name[:50]
    return label[:50]


def main():
    # Collect all log files sorted by name (chronological)
    log_files = sorted(glob.glob(os.path.join(LOG_DIR, "v10_*.log")))
    if not log_files:
        print("No v10_*.log files found in logs/")
        return

    print(f"Analyzing {len(log_files)} log files...\n")

    positions = defaultdict(lambda: Position(""))
    all_hammers = []
    all_trims = []
    all_writeoffs = []
    total_buys = 0
    total_sells = 0
    total_spent = 0.0
    total_trim_rev = 0.0

    for lf in log_files:
        log_date = os.path.basename(lf).replace("v10_", "").replace(".log", "")
        with open(lf) as f:
            for line in f:
                # NEW POSITION
                m = NEW_POS.search(line)
                if m:
                    time_s, side, name, price, shares, cost = m.groups()
                    price, shares, cost = float(price), int(shares), float(cost)
                    name = name.strip()[:50]
                    pos = positions[name]
                    pos.name = name
                    if side == "NO":
                        pos.no_shares += shares
                        pos.no_cost += cost
                        pos.no_avg = pos.no_cost / pos.no_shares if pos.no_shares else 0
                    else:
                        pos.yes_shares += shares
                        pos.yes_cost += cost
                        pos.yes_avg = pos.yes_cost / pos.yes_shares if pos.yes_shares else 0
                    pos.buys.append((log_date + " " + time_s, side, shares, price, cost, "NEW"))
                    total_buys += 1
                    total_spent += cost
                    continue

                # VERIFIED FILL (from delayed orders)
                m = VERIFIED_FILL.search(line)
                if m:
                    time_s, shares_s, cost_s, label = m.groups()
                    shares, cost = float(shares_s), float(cost_s)
                    name = extract_market_from_label(label)
                    # Determine side from label
                    side = "NO" if "NO|" in label or "HAMMER-NO" in label or "WS-NO" in label or "LOCK-NO" in label else "YES"
                    if "LOCK-YES" in label or "WS-YES" in label or "HAMMER-YES" in label:
                        side = "YES"
                    pos = positions[name]
                    pos.name = name
                    # Don't double-count — verified fill replaces the initial buy log
                    # We track it but the main counting is done via HAMMER/NEW patterns
                    continue

                # HAMMER CONFIRMED
                m = HAMMER_DONE.search(line)
                if m:
                    time_s, name, side, price, shares, cost = m.groups()
                    price, shares, cost = float(price), int(shares), float(cost)
                    name = name.strip()[:50]
                    pos = positions[name]
                    pos.name = name
                    if side == "NO":
                        pos.no_shares += shares
                        pos.no_cost += cost
                    else:
                        pos.yes_shares += shares
                        pos.yes_cost += cost
                    pos.hammer_cost += cost
                    pos.hammer_count += 1
                    pos.buys.append((log_date + " " + time_s, side, shares, price, cost, "HAMMER"))
                    all_hammers.append((name, side, price, shares, cost, "CONFIRMED"))
                    total_buys += 1
                    total_spent += cost
                    continue

                # HAMMER SENT (verification failed but likely filled)
                m = HAMMER_SENT.search(line)
                if m:
                    time_s, name, side, price = m.groups()
                    price = float(price)
                    name = name.strip()[:50]
                    # Estimate cost: $15 hammer
                    est_shares = int(15.0 / price)
                    est_cost = 15.0
                    pos = positions[name]
                    pos.name = name
                    if side == "NO":
                        pos.no_shares += est_shares
                        pos.no_cost += est_cost
                    else:
                        pos.yes_shares += est_shares
                        pos.yes_cost += est_cost
                    pos.hammer_cost += est_cost
                    pos.hammer_count += 1
                    pos.hammer_unverified += 1
                    pos.buys.append((log_date + " " + time_s, side, est_shares, price, est_cost, "HAMMER-UNVERIFIED"))
                    all_hammers.append((name, side, price, est_shares, est_cost, "UNVERIFIED"))
                    total_buys += 1
                    total_spent += est_cost
                    continue

                # TRIM
                m = TRIM_DONE.search(line)
                if m:
                    time_s, name, shares_s, side, price, revenue = m.groups()
                    shares, price, revenue = int(shares_s), float(price), float(revenue)
                    name = name.strip()[:50]
                    pos = positions[name]
                    pos.name = name
                    pos.trim_revenue += revenue
                    pos.trim_count += 1
                    if side == "NO":
                        pos.no_shares -= shares
                        if pos.no_shares < 0: pos.no_shares = 0
                    else:
                        pos.yes_shares -= shares
                        if pos.yes_shares < 0: pos.yes_shares = 0
                    pos.sells.append((log_date + " " + time_s, side, shares, price, revenue))
                    all_trims.append((name, side, price, shares, revenue))
                    total_sells += 1
                    total_trim_rev += revenue
                    continue

                # WRITE-OFF
                m = WRITEOFF.search(line)
                if m:
                    time_s, name, shares_s, side, loss = m.groups()
                    shares, loss = int(shares_s), float(loss)
                    name = name.strip()[:50]
                    pos = positions[name]
                    pos.name = name
                    pos.writeoff_loss += loss
                    if side == "NO":
                        pos.no_shares -= shares
                        if pos.no_shares < 0: pos.no_shares = 0
                    else:
                        pos.yes_shares -= shares
                        if pos.yes_shares < 0: pos.yes_shares = 0
                    all_writeoffs.append((name, side, shares, loss))
                    continue

                # LOCKED (arb completed)
                m = LOCKED.search(line)
                if m:
                    time_s, name_raw, no_price, yes_price, gp = m.groups()
                    # name is embedded in the short() format — extract first part
                    name = name_raw.strip()[:50]
                    pos = positions[name]
                    pos.name = name
                    pos.is_locked = True
                    pos.gp = float(gp)
                    continue

    # Also scan the portfolio status output from latest log for current prices
    print("=" * 100)
    print(f"  FULL PORTFOLIO ANALYSIS — {len(positions)} positions found across {len(log_files)} log files")
    print("=" * 100)

    # Categorize
    locked_positions = []
    hammered_positions = []
    hedged_positions = []
    one_sided = []
    writeoff_positions = []

    for name, pos in sorted(positions.items(), key=lambda x: x[1].total_cost, reverse=True):
        if pos.total_cost < 0.50:
            continue  # skip tiny dust

        has_hammer = pos.hammer_count > 0
        is_locked = pos.is_locked or (pos.no_shares > 0 and pos.yes_shares > 0 and not has_hammer)
        is_hedged = pos.no_shares > 0 and pos.yes_shares > 0 and not is_locked
        is_one_sided = (pos.no_shares > 0) != (pos.yes_shares > 0)

        if has_hammer:
            hammered_positions.append((name, pos))
        elif pos.writeoff_loss > 0 and pos.no_shares == 0 and pos.yes_shares == 0:
            writeoff_positions.append((name, pos))
        elif is_locked:
            locked_positions.append((name, pos))
        elif is_hedged:
            hedged_positions.append((name, pos))
        else:
            one_sided.append((name, pos))

    # ─── LOCKED ARBS ───
    total_locked_gp = 0
    total_locked_cost = 0
    print(f"\n  🔒 LOCKED ARBS ({len(locked_positions)}) — guaranteed profit positions")
    print(f"  {'Market':<50} {'NO':>10} {'YES':>10} {'Cost':>8} {'GP':>8} {'ROI':>6}")
    print(f"  {'─'*50} {'─'*10} {'─'*10} {'─'*8} {'─'*8} {'─'*6}")
    for name, pos in sorted(locked_positions, key=lambda x: x[1].total_cost, reverse=True):
        matched = min(pos.no_shares, pos.yes_shares)
        cost = pos.total_cost
        payout = matched * 1.0
        gp = payout - cost + pos.trim_revenue
        roi = (gp / cost * 100) if cost > 0 else 0
        total_locked_gp += gp
        total_locked_cost += cost
        print(f"  {name:<50} {pos.no_shares:>3}@{pos.no_avg:.2f}  {pos.yes_shares:>3}@{pos.yes_avg:.2f}"
              f"  ${cost:>6.2f}  ${gp:>+6.2f}  {roi:>+.1f}%")
    if locked_positions:
        print(f"  {'TOTAL':<50} {'':>10} {'':>10}  ${total_locked_cost:>6.2f}  ${total_locked_gp:>+6.2f}")

    # ─── HAMMERED POSITIONS ───
    total_hammer_best = 0
    total_hammer_worst = 0
    total_hammer_cost = 0
    total_hammer_spent = 0
    print(f"\n  🔨 HAMMERED POSITIONS ({len(hammered_positions)}) — directional bets")
    print(f"  {'Market':<50} {'NO':>10} {'YES':>10} {'Cost':>8} {'Hammer':>8} {'If Wins':>8} {'If Loses':>8} {'Trimmed':>8}")
    print(f"  {'─'*50} {'─'*10} {'─'*10} {'─'*8} {'─'*8} {'─'*8} {'─'*8} {'─'*8}")
    for name, pos in sorted(hammered_positions, key=lambda x: x[1].hammer_cost, reverse=True):
        cost = pos.total_cost
        best = pos.best_pnl()
        worst = pos.worst_pnl()
        win_side = "NO" if pos.no_shares > pos.yes_shares else "YES"
        total_hammer_best += best
        total_hammer_worst += worst
        total_hammer_cost += cost
        total_hammer_spent += pos.hammer_cost
        unv = f"({pos.hammer_unverified}?)" if pos.hammer_unverified else ""
        print(f"  {name:<50} {pos.no_shares:>3}@{pos.no_avg:.2f}  {pos.yes_shares:>3}@{pos.yes_avg:.2f}"
              f"  ${cost:>6.2f}  ${pos.hammer_cost:>5.0f}{unv:<4}  ${best:>+6.2f}  ${worst:>+6.2f}  ${pos.trim_revenue:>5.2f}")
    if hammered_positions:
        print(f"  {'TOTAL':<50} {'':>10} {'':>10}  ${total_hammer_cost:>6.2f}  ${total_hammer_spent:>5.0f}      ${total_hammer_best:>+6.2f}  ${total_hammer_worst:>+6.2f}")

    # ─── HEDGED (both sides, not locked, not hammered) ───
    total_hedge_cost = 0
    total_hedge_gp = 0
    print(f"\n  🛡️ HEDGED ({len(hedged_positions)}) — both sides held, not locked")
    print(f"  {'Market':<50} {'NO':>10} {'YES':>10} {'Cost':>8} {'Best':>8} {'Worst':>8}")
    print(f"  {'─'*50} {'─'*10} {'─'*10} {'─'*8} {'─'*8} {'─'*8}")
    for name, pos in sorted(hedged_positions, key=lambda x: x[1].total_cost, reverse=True):
        cost = pos.total_cost
        best = pos.best_pnl()
        worst = pos.worst_pnl()
        total_hedge_cost += cost
        total_hedge_gp += worst  # conservative
        print(f"  {name:<50} {pos.no_shares:>3}@{pos.no_avg:.2f}  {pos.yes_shares:>3}@{pos.yes_avg:.2f}"
              f"  ${cost:>6.2f}  ${best:>+6.2f}  ${worst:>+6.2f}")

    # ─── ONE-SIDED ───
    total_onesided_cost = 0
    print(f"\n  👛 ONE-SIDED ({len(one_sided)}) — single side only")
    print(f"  {'Market':<50} {'Side':>6} {'Shares':>8} {'Avg':>6} {'Cost':>8}")
    print(f"  {'─'*50} {'─'*6} {'─'*8} {'─'*6} {'─'*8}")
    for name, pos in sorted(one_sided, key=lambda x: x[1].total_cost, reverse=True):
        side = "NO" if pos.no_shares > 0 else "YES"
        shares = pos.no_shares if side == "NO" else pos.yes_shares
        avg = pos.no_avg if side == "NO" else pos.yes_avg
        cost = pos.total_cost
        total_onesided_cost += cost
        print(f"  {name:<50} {side:>6} {shares:>8} {avg:>6.3f}  ${cost:>6.2f}")

    # ─── WRITE-OFFS ───
    total_writeoff = sum(pos.writeoff_loss for _, pos in positions.items())
    if writeoff_positions:
        print(f"\n  ☠️ WRITTEN OFF ({len(writeoff_positions)}) — total losses")
        print(f"  {'Market':<50} {'Loss':>8}")
        print(f"  {'─'*50} {'─'*8}")
        for name, pos in sorted(writeoff_positions, key=lambda x: x[1].writeoff_loss, reverse=True):
            print(f"  {name:<50}  ${pos.writeoff_loss:>6.2f}")

    # ─── SUMMARY ───
    print(f"\n{'='*100}")
    print(f"  SUMMARY")
    print(f"{'='*100}")
    print(f"  Total positions tracked:     {len([p for p in positions.values() if p.total_cost >= 0.5])}")
    print(f"  Total buys:                  {total_buys}")
    print(f"  Total $ spent:               ${total_spent:,.2f}")
    print(f"")
    print(f"  Locked arb GP:               ${total_locked_gp:>+,.2f}  ({len(locked_positions)} positions, ${total_locked_cost:,.2f} deployed)")
    print(f"  Hammer (if ALL win):         ${total_hammer_best:>+,.2f}  ({len(hammered_positions)} positions, ${total_hammer_cost:,.2f} deployed, ${total_hammer_spent:,.0f} hammered)")
    print(f"  Hammer (if ALL lose):        ${total_hammer_worst:>+,.2f}")
    print(f"  Hedged (worst case):         ${total_hedge_gp:>+,.2f}  ({len(hedged_positions)} positions)")
    print(f"  One-sided exposure:          ${total_onesided_cost:>,.2f}  ({len(one_sided)} positions)")
    print(f"  Write-off losses:            ${total_writeoff:>,.2f}")
    print(f"  Trim revenue recovered:      ${total_trim_rev:>+,.2f}  ({len(all_trims)} trims)")
    print(f"")

    # Hammer analysis
    print(f"  ─── HAMMER BREAKDOWN ───")
    confirmed = [h for h in all_hammers if h[5] == "CONFIRMED"]
    unverified = [h for h in all_hammers if h[5] == "UNVERIFIED"]
    print(f"  Confirmed fills:             {len(confirmed)}  (${sum(h[4] for h in confirmed):,.2f})")
    print(f"  Unverified (likely filled):  {len(unverified)}  (~${sum(h[4] for h in unverified):,.2f})")
    total_hammer_spent_all = sum(h[4] for h in all_hammers)
    print(f"  Total hammer $:              ${total_hammer_spent_all:,.2f}")
    print(f"")

    # Trim analysis
    print(f"  ─── TRIM BREAKDOWN ───")
    print(f"  Total trims:                 {len(all_trims)}")
    print(f"  Total revenue recovered:     ${total_trim_rev:,.2f}")
    print(f"  Average per trim:            ${total_trim_rev/len(all_trims) if all_trims else 0:,.2f}")
    print(f"  Total write-offs:            ${total_writeoff:,.2f}")
    print(f"")

    # NET P&L estimate
    # Locked GP + trim revenue - writeoffs
    # Hammers: best case vs worst case
    net_guaranteed = total_locked_gp - total_writeoff + total_trim_rev
    net_best = net_guaranteed + total_hammer_best + total_hedge_gp
    net_worst = net_guaranteed + total_hammer_worst + total_hedge_gp
    print(f"  ─── NET P&L ESTIMATE ───")
    print(f"  Guaranteed (locked arbs + trims - writeoffs):  ${net_guaranteed:>+,.2f}")
    print(f"  Best case (all hammers win):                   ${net_best:>+,.2f}")
    print(f"  Worst case (all hammers lose):                 ${net_worst:>+,.2f}")
    print(f"")

    # Double-hammer detection
    print(f"  ─── DOUBLE-HAMMER CHECK ───")
    hammer_by_pos = defaultdict(list)
    for h in all_hammers:
        hammer_by_pos[h[0]].append(h)
    doubles = {k: v for k, v in hammer_by_pos.items() if len(v) > 1}
    if doubles:
        print(f"  ⚠️ {len(doubles)} positions were hammered MORE THAN ONCE:")
        for name, hlist in doubles.items():
            total_h = sum(h[4] for h in hlist)
            print(f"    {name}: {len(hlist)} hammers, ${total_h:.2f} total")
            for h in hlist:
                print(f"      {h[5]:>12} {h[1]} @{h[2]:.3f} {h[3]}sh ${h[4]:.2f}")
    else:
        print(f"  ✅ No double-hammers detected")

    print(f"\n{'='*100}")


if __name__ == "__main__":
    main()
