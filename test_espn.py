#!/usr/bin/env python3
"""
ESPN API Test — check what live game data we can get.
Tests NBA, NHL, MLB scoreboards for game clock, period, and progress.

Run: python3 test_espn.py
"""

import requests
import json
from datetime import datetime

SPORTS = {
    "NBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "NHL": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
    "MLB": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    "NCAAB": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard",
    "MLS": "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard",
}

# Total regulation periods and minutes per sport
SPORT_CONFIG = {
    "NBA":   {"periods": 4, "period_mins": 12, "total_mins": 48},
    "NHL":   {"periods": 3, "period_mins": 20, "total_mins": 60},
    "MLB":   {"periods": 9, "period_mins": None, "total_mins": None},  # innings, no clock
    "NCAAB": {"periods": 2, "period_mins": 20, "total_mins": 40},
    "MLS":   {"periods": 2, "period_mins": 45, "total_mins": 90},
}

def parse_clock(clock_str):
    """Parse clock string like '5:23' or '12:00' to seconds remaining."""
    if not clock_str:
        return 0
    parts = clock_str.strip().split(":")
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 1:
            return int(parts[0])
    except (ValueError, IndexError):
        return 0
    return 0

def calc_game_progress(sport, period, clock_str, status_type):
    """Calculate game progress as 0.0 to 1.0 (or >1.0 for overtime)."""
    cfg = SPORT_CONFIG.get(sport, {})
    total_periods = cfg.get("periods", 4)
    period_mins = cfg.get("period_mins")
    total_mins = cfg.get("total_mins")

    if status_type == "STATUS_FINAL":
        return 1.0
    if status_type == "STATUS_SCHEDULED":
        return 0.0

    if sport == "MLB":
        # Baseball: use innings. period = current inning
        # Each inning has top and half, but ESPN just gives inning number
        if total_periods > 0:
            return min((period - 1) / total_periods, 1.0)  # rough estimate
        return 0.0

    if not period_mins or not total_mins:
        return 0.0

    # Clock-based sports (NBA, NHL, NCAAB, MLS)
    clock_secs = parse_clock(clock_str)
    period_secs = period_mins * 60

    # Completed periods + time elapsed in current period
    completed_secs = (period - 1) * period_secs
    current_period_elapsed = period_secs - clock_secs
    total_elapsed = completed_secs + current_period_elapsed
    total_regulation = total_mins * 60

    progress = total_elapsed / total_regulation
    return round(progress, 3)

def test_sport(sport, url):
    """Fetch and display live game data for a sport."""
    print(f"\n{'='*60}")
    print(f"  {sport} SCOREBOARD")
    print(f"{'='*60}")

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  ERROR: {e}")
        return

    events = data.get("events", [])
    if not events:
        print("  No games today.")
        return

    for ev in events:
        name = ev.get("name", "?")
        short_name = ev.get("shortName", "?")

        comp = ev.get("competitions", [{}])[0]
        status = comp.get("status", {})
        status_type = status.get("type", {}).get("name", "")
        status_desc = status.get("type", {}).get("shortDetail", "")
        clock = status.get("displayClock", "")
        period = status.get("period", 0)

        # Teams and scores
        competitors = comp.get("competitors", [])
        teams = []
        for c in competitors:
            team_name = c.get("team", {}).get("displayName", "?")
            team_abbr = c.get("team", {}).get("abbreviation", "?")
            score = c.get("score", "0")
            home_away = c.get("homeAway", "?")
            teams.append({
                "name": team_name,
                "abbr": team_abbr,
                "score": score,
                "home_away": home_away
            })

        # Calculate progress
        progress = calc_game_progress(sport, period, clock, status_type)
        pct = progress * 100

        # Display
        away = next((t for t in teams if t["home_away"] == "away"), teams[0] if teams else {"name":"?","score":"0","abbr":"?"})
        home = next((t for t in teams if t["home_away"] == "home"), teams[1] if len(teams) > 1 else {"name":"?","score":"0","abbr":"?"})

        status_emoji = "🟢" if status_type == "STATUS_IN_PROGRESS" else "⏳" if status_type == "STATUS_SCHEDULED" else "🏁"

        print(f"\n  {status_emoji} {away['name']} ({away['score']}) @ {home['name']} ({home['score']})")
        print(f"     Status:   {status_desc}")
        print(f"     Period:   {period}  |  Clock: {clock}")
        print(f"     Progress: {pct:.1f}%  ({'LIVE' if status_type == 'STATUS_IN_PROGRESS' else status_type})")
        print(f"     Short:    {short_name}")

        # Show what we'd tell the bot
        if status_type == "STATUS_IN_PROGRESS":
            if pct >= 80:
                print(f"     >>> HAMMER OK — game {pct:.0f}% complete (last 20%)")
            elif pct >= 60:
                print(f"     >>> CAUTION — game {pct:.0f}% complete (middle stages)")
            else:
                print(f"     >>> TOO EARLY — game only {pct:.0f}% complete (hold hammers)")

    # Also dump raw structure for first live game (for debugging)
    live_games = [ev for ev in events if ev.get("competitions", [{}])[0].get("status", {}).get("type", {}).get("name") == "STATUS_IN_PROGRESS"]
    if live_games:
        ev = live_games[0]
        comp = ev["competitions"][0]
        status = comp["status"]
        print(f"\n  --- RAW STATUS for first live game ---")
        print(f"  {json.dumps(status, indent=2)[:1000]}")


def main():
    print(f"ESPN API Test — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Checking live games across {len(SPORTS)} sports...\n")

    for sport, url in SPORTS.items():
        test_sport(sport, url)

    print(f"\n{'='*60}")
    print("  DONE — check above for game progress data.")
    print("  If live games show period/clock/progress, ESPN API works!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
