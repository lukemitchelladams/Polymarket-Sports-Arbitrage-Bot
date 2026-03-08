#!/usr/bin/env python3
"""
LIVE GAME SCORE FETCHER
========================
Polls ESPN's free scoreboard API for live game scores across NBA, NHL, MLB,
tennis (ATP/WTA), soccer, and more. Matches games to Polymarket positions
by fuzzy-matching team/player names.

Designed to be imported by arb_v10.py and called every snapshot cycle.

ESPN endpoints (free, no API key):
  NBA:    site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard
  NHL:    site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard
  MLB:    site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard
  NFL:    site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard
  MLS:    site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard
  Tennis: site.api.espn.com/apis/site/v2/sports/tennis/atp/scoreboard

Usage:
    from game_scores import GameScoreFetcher
    fetcher = GameScoreFetcher()
    fetcher.update()  # poll all leagues
    scores = fetcher.match_positions(portfolio_positions)
    # scores = {mid: {"home": "Celtics", "away": "Heat", "home_score": 102, ...}}
"""

import requests
import time
import re
import logging
from difflib import SequenceMatcher

log = logging.getLogger("arb")

# ESPN scoreboard endpoints — covers all major Polymarket sports
ESPN_ENDPOINTS = {
    "nba": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard",
    "nhl": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard",
    "mlb": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard",
    "nfl": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard",
    "ncaam": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard",
    "ncaaf": "https://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard",
    "mls": "https://site.api.espn.com/apis/site/v2/sports/soccer/usa.1/scoreboard",
    "epl": "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1/scoreboard",
    "bundesliga": "https://site.api.espn.com/apis/site/v2/sports/soccer/ger.1/scoreboard",
    "laliga": "https://site.api.espn.com/apis/site/v2/sports/soccer/esp.1/scoreboard",
    "seriea": "https://site.api.espn.com/apis/site/v2/sports/soccer/ita.1/scoreboard",
    "ligue1": "https://site.api.espn.com/apis/site/v2/sports/soccer/fra.1/scoreboard",
    "atp": "https://site.api.espn.com/apis/site/v2/sports/tennis/atp/scoreboard",
    "wta": "https://site.api.espn.com/apis/site/v2/sports/tennis/wta/scoreboard",
}

# How often to poll each league (seconds)
POLL_INTERVAL = 30
# Don't re-fetch a league if we fetched it recently
MIN_FETCH_GAP = 25

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0"})


def _parse_espn_event(event, league):
    """Parse a single ESPN event into a normalized game dict."""
    try:
        name = event.get("name", "")
        short_name = event.get("shortName", "")

        status_obj = event.get("status", {})
        status_type = status_obj.get("type", {})
        clock = status_obj.get("displayClock", "")
        period = status_obj.get("period", 0)
        state = status_type.get("state", "")  # pre, in, post
        completed = status_type.get("completed", False)
        detail = status_type.get("detail", status_type.get("shortDetail", ""))

        competitions = event.get("competitions", [])
        if not competitions:
            return None

        comp = competitions[0]
        competitors = comp.get("competitors", [])
        if len(competitors) < 2:
            return None

        teams = {}
        for c in competitors:
            ha = c.get("homeAway", "")
            team_obj = c.get("team", {})
            score_str = c.get("score", "0")
            try:
                score_val = int(score_str) if score_str else 0
            except (ValueError, TypeError):
                score_val = 0

            teams[ha] = {
                "name": team_obj.get("displayName", ""),
                "short": team_obj.get("shortDisplayName", ""),
                "abbrev": team_obj.get("abbreviation", ""),
                "score": score_val,
            }

        home = teams.get("home", {})
        away = teams.get("away", {})

        # For tennis, competitors may not have home/away
        if not home and not away:
            if len(competitors) >= 2:
                home = {
                    "name": competitors[0].get("team", {}).get("displayName",
                            competitors[0].get("athlete", {}).get("displayName", "")),
                    "short": competitors[0].get("team", {}).get("shortDisplayName", ""),
                    "abbrev": competitors[0].get("team", {}).get("abbreviation", ""),
                    "score": int(competitors[0].get("score", "0") or 0),
                }
                away = {
                    "name": competitors[1].get("team", {}).get("displayName",
                            competitors[1].get("athlete", {}).get("displayName", "")),
                    "short": competitors[1].get("team", {}).get("shortDisplayName", ""),
                    "abbrev": competitors[1].get("team", {}).get("abbreviation", ""),
                    "score": int(competitors[1].get("score", "0") or 0),
                }

        game = {
            "league": league,
            "name": name,
            "short_name": short_name,
            "home_name": home.get("name", ""),
            "home_short": home.get("short", ""),
            "home_abbrev": home.get("abbrev", ""),
            "home_score": home.get("score", 0),
            "away_name": away.get("name", ""),
            "away_short": away.get("short", ""),
            "away_abbrev": away.get("abbrev", ""),
            "away_score": away.get("score", 0),
            "state": state,  # pre, in, post
            "completed": completed,
            "clock": clock,
            "period": period,
            "detail": detail,  # e.g. "Q3 4:32" or "Final" or "Halftime"
            "fetched_at": time.time(),
        }

        # Build list of searchable names for matching
        search_names = set()
        for field in ["home_name", "home_short", "home_abbrev",
                      "away_name", "away_short", "away_abbrev"]:
            val = game.get(field, "")
            if val:
                search_names.add(val.lower())
                # Also add individual words (for matching "Celtics" from "Boston Celtics")
                for word in val.split():
                    if len(word) > 2:
                        search_names.add(word.lower())
        game["_search_names"] = search_names

        return game
    except Exception as e:
        return None


def _fuzzy_match_score(title, game):
    """Score how well a Polymarket title matches an ESPN game. Higher = better."""
    title_lower = title.lower()
    search_names = game.get("_search_names", set())

    score = 0
    matches = 0

    # Check for team name matches
    for name in search_names:
        if len(name) > 3 and name in title_lower:
            # Longer names get higher scores (avoid matching "NO" or "vs")
            score += len(name)
            matches += 1

    # Need at least one team to match
    if matches < 1:
        return 0

    # Bonus for matching both teams
    if matches >= 2:
        score += 20

    # Check for league-specific keywords
    league = game.get("league", "")
    if league in ("nba", "ncaam") and any(w in title_lower for w in ["points", "rebounds", "assists", "o/u"]):
        score += 5
    if league in ("nhl",) and any(w in title_lower for w in ["goals", "o/u"]):
        score += 5

    return score


class GameScoreFetcher:
    """Fetches and caches live game scores from ESPN."""

    def __init__(self):
        self.games = {}           # {game_key: game_dict}
        self._last_fetch = {}     # {league: timestamp}
        self._match_cache = {}    # {mid: game_key} — cached position-to-game mappings
        self._enabled_leagues = set(ESPN_ENDPOINTS.keys())

    def update(self, leagues=None):
        """Poll ESPN for current scores. Pass specific leagues or None for all."""
        now = time.time()
        to_fetch = leagues or self._enabled_leagues

        for league in to_fetch:
            url = ESPN_ENDPOINTS.get(league)
            if not url:
                continue

            # Skip if fetched recently
            last = self._last_fetch.get(league, 0)
            if now - last < MIN_FETCH_GAP:
                continue

            try:
                r = session.get(url, timeout=8)
                if r.status_code != 200:
                    continue

                data = r.json()
                events = data.get("events", [])

                for event in events:
                    game = _parse_espn_event(event, league)
                    if game:
                        # Use event name as key
                        key = f"{league}:{game['home_abbrev']}v{game['away_abbrev']}"
                        self.games[key] = game

                self._last_fetch[league] = now

            except Exception as e:
                pass  # non-critical

    def get_all_live(self):
        """Return all games currently in progress."""
        return {k: g for k, g in self.games.items() if g.get("state") == "in"}

    def match_position(self, mid, title, typ=""):
        """Find the best matching game for a Polymarket position.
        Returns game dict or None."""
        # Check cache first
        if mid in self._match_cache:
            cached_key = self._match_cache[mid]
            if cached_key in self.games:
                return self.games[cached_key]

        best_key = None
        best_score = 0

        for key, game in self.games.items():
            score = _fuzzy_match_score(title, game)
            if score > best_score:
                best_score = score
                best_key = key

        # Need minimum confidence
        if best_score >= 5 and best_key:
            self._match_cache[mid] = best_key
            return self.games[best_key]

        return None

    def match_positions(self, positions):
        """Match all portfolio positions to games.
        positions: dict of {mid: Position} or list of (mid, pos) tuples.
        Returns {mid: game_dict} for positions with matches."""
        results = {}

        items = positions.items() if isinstance(positions, dict) else positions
        for mid, pos in items:
            title = pos.title if hasattr(pos, 'title') else pos.get('title', '')
            typ = pos.typ if hasattr(pos, 'typ') else pos.get('typ', '')
            game = self.match_position(mid, title, typ)
            if game:
                results[mid] = game

        return results

    def enrich_snapshot(self, positions):
        """Create game state data for the data collection snapshot.
        Returns dict of enriched game states keyed by mid."""
        matched = self.match_positions(positions)
        enriched = {}

        for mid, game in matched.items():
            enriched[mid] = {
                "league": game["league"],
                "home": game["home_short"] or game["home_name"],
                "away": game["away_short"] or game["away_name"],
                "home_score": game["home_score"],
                "away_score": game["away_score"],
                "state": game["state"],
                "period": game["period"],
                "clock": game["clock"],
                "detail": game["detail"],
                "score_diff": game["home_score"] - game["away_score"],
            }

        return enriched

    def format_game_status(self, game):
        """Format a game for dashboard display."""
        if not game:
            return ""
        state = game.get("state", "")
        if state == "pre":
            return "PREGAME"
        elif state == "post":
            return "FINAL"
        elif state == "in":
            home_s = game.get("home_score", 0)
            away_s = game.get("away_score", 0)
            clock = game.get("clock", "")
            period = game.get("period", 0)
            detail = game.get("detail", "")
            # Use detail if available, otherwise build from period+clock
            if detail:
                return f"{away_s}-{home_s} {detail}"
            elif clock and period:
                return f"{away_s}-{home_s} P{period} {clock}"
            else:
                return f"{away_s}-{home_s}"
        return ""

    def stats(self):
        """Return summary stats."""
        total = len(self.games)
        live = sum(1 for g in self.games.values() if g.get("state") == "in")
        final = sum(1 for g in self.games.values() if g.get("state") == "post")
        pre = sum(1 for g in self.games.values() if g.get("state") == "pre")
        leagues = len(set(g["league"] for g in self.games.values()))
        return {
            "total": total,
            "live": live,
            "final": final,
            "pregame": pre,
            "leagues": leagues,
        }


# ── Standalone test ──
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=" * 60)
    print("  GAME SCORE FETCHER — Test Mode")
    print("=" * 60)

    fetcher = GameScoreFetcher()
    print("\nFetching scores from ESPN...")
    fetcher.update()

    stats = fetcher.stats()
    print(f"\nGames: {stats['total']} total ({stats['live']} live, "
          f"{stats['final']} final, {stats['pregame']} pregame) "
          f"across {stats['leagues']} leagues")

    live = fetcher.get_all_live()
    if live:
        print(f"\n  LIVE GAMES ({len(live)}):")
        print(f"  {'League':>8s}  {'Game':<40s} {'Score':<15s} {'Status'}")
        print(f"  {'─'*8}  {'─'*40} {'─'*15} {'─'*20}")
        for key, g in sorted(live.items()):
            name = f"{g['away_short'] or g['away_name']} @ {g['home_short'] or g['home_name']}"
            score = f"{g['away_score']}-{g['home_score']}"
            status = fetcher.format_game_status(g)
            print(f"  {g['league']:>8s}  {name:<40s} {score:<15s} {status}")
    else:
        print("\n  No live games right now.")

    # Test matching against some example Polymarket titles
    test_titles = [
        "Warriors vs. Rockets",
        "Raptors vs. Timberwolves: O/U 225.5",
        "BNP Paribas Open: Nuno Borges vs Emilio Nava",
        "Spread: Heat (-13.5)",
        "Panthers vs. Blue Jackets",
    ]
    print(f"\n  MATCH TEST:")
    for title in test_titles:
        game = fetcher.match_position("test", title)
        if game:
            status = fetcher.format_game_status(game)
            print(f"  '{title[:40]}' → {game['away_short']} @ {game['home_short']}: {status}")
        else:
            print(f"  '{title[:40]}' → no match")

    print("\nDone.")
