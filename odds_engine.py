"""
odds_engine.py — External sportsbook odds intelligence for Polymarket arb bot.

Fetches live odds from The Odds API, computes consensus probabilities,
tracks line movements, and provides edge calculations vs Polymarket prices.

Background thread fetches odds every ~55 min per sport (stays within
500 req/month free tier). Results cached in memory for fast scoring lookups.

Usage:
    from odds_engine import OddsEngine
    engine = OddsEngine(api_key="YOUR_KEY")
    engine.start_background()  # daemon thread

    # In scoring loop:
    edge = engine.get_edge("Lightning vs Maple Leafs", 0.55, 0.45)
    # → {"edge_cents": 0.09, "sportsbook_prob": 0.64, "confidence": 0.95, ...}
"""

import json
import os
import threading
import time
import logging
import requests
from collections import defaultdict

from team_name_mapper import match_title, ALL_SPORT_DICTS
from elo_ratings import EloModel

log = logging.getLogger("arb")

# ═══════════════════════════════════════════════════════════════
# API CONFIG
# ═══════════════════════════════════════════════════════════════

API_BASE = "https://api.the-odds-api.com/v4"

# The Odds API sport keys → our internal sport names
SPORT_KEY_MAP = {
    "basketball_nba":       "nba",
    "icehockey_nhl":        "nhl",
    "baseball_mlb":         "mlb",
    "americanfootball_nfl": "nfl",
    # Soccer leagues
    "soccer_epl":           "soccer",
    "soccer_spain_la_liga": "soccer",
    "soccer_germany_bundesliga": "soccer",
    "soccer_italy_serie_a": "soccer",
    "soccer_france_ligue_one": "soccer",
    "soccer_usa_mls":       "soccer",
    "soccer_mexico_ligamx": "soccer",
    "soccer_uefa_champs_league": "soccer",
    "soccer_uefa_europa_league": "soccer",
    # MMA
    "mma_mixed_martial_arts": "mma",
}

# Which API sport keys to fetch (rotate through them)
FETCH_SPORTS = [
    "basketball_nba",
    "icehockey_nhl",
    "baseball_mlb",
    "americanfootball_nfl",
    "soccer_epl",
    "soccer_spain_la_liga",
    "soccer_mexico_ligamx",
    "soccer_usa_mls",
    "mma_mixed_martial_arts",
]

# Preferred bookmakers (in priority order)
PREFERRED_BOOKS = [
    "draftkings", "fanduel", "betmgm", "caesars",
    "pointsbetus", "bovada", "betonlineag",
]

CACHE_TTL_SECS = 3300      # 55 minutes
FETCH_INTERVAL_SECS = 300  # check every 5 min
LINE_SNAPSHOT_MAX = 24     # keep 24 snapshots per event (~24 hours at 1/hr)


# ═══════════════════════════════════════════════════════════════
# ODDS ENGINE
# ═══════════════════════════════════════════════════════════════

class OddsEngine:
    """
    Manages external sportsbook odds — fetching, caching, matching, scoring.

    Thread-safe: background thread writes cache, scoring thread reads it.
    """

    def __init__(self, api_key=None):
        self.api_key = api_key or os.getenv("THE_ODDS_API_KEY", "")
        self._lock = threading.Lock()

        # Cache: {api_sport_key: [event_dict, ...]}
        self._cache = {}
        self._cache_ts = {}  # {api_sport_key: timestamp}

        # Line movement snapshots: {event_id: [(ts, home_prob, away_prob), ...]}
        self._line_snapshots = defaultdict(list)

        # Match cache: {poly_title_lower: edge_result_dict}
        # Invalidated on each new fetch cycle
        self._match_cache = {}
        self._match_cache_ts = 0

        # Elo model
        self.elo = EloModel()

        # Stats
        self.n_fetches = 0
        self.n_errors = 0
        self.n_events_cached = 0
        self.last_fetch_ts = 0
        self.requests_remaining = None  # from API header

        self._running = False

    # ─── BACKGROUND THREAD ────────────────────────────────────

    def start_background(self):
        """Start the background odds-fetching thread."""
        if self._running:
            return
        self._running = True
        t = threading.Thread(target=self._background_loop, daemon=True, name="OddsEngine")
        t.start()
        log.info(f"  📊 Odds engine started (API key={'set' if self.api_key else 'MISSING'})")

    def _background_loop(self):
        """Continuously poll odds. Fetches one sport per cycle."""
        # Initial delay — let the bot warm up first
        time.sleep(30)

        fetch_idx = 0
        while self._running:
            try:
                # Pick the next sport to fetch
                sport_key = FETCH_SPORTS[fetch_idx % len(FETCH_SPORTS)]
                fetch_idx += 1

                # Only fetch if cache is stale
                last = self._cache_ts.get(sport_key, 0)
                if time.time() - last > CACHE_TTL_SECS:
                    self._fetch_odds(sport_key)

                time.sleep(FETCH_INTERVAL_SECS)

            except Exception as e:
                log.debug(f"  Odds engine error: {e}")
                self.n_errors += 1
                time.sleep(60)

    def _fetch_odds(self, sport_key):
        """Fetch odds from The Odds API for a specific sport."""
        if not self.api_key:
            return

        try:
            url = f"{API_BASE}/sports/{sport_key}/odds"
            params = {
                "apiKey": self.api_key,
                "regions": "us",
                "markets": "h2h",
                "oddsFormat": "decimal",
            }

            resp = requests.get(url, params=params, timeout=15)

            # Track API usage from headers
            remaining = resp.headers.get("x-requests-remaining")
            if remaining:
                self.requests_remaining = int(remaining)

            if resp.status_code == 401:
                log.warning("  ⚠️ Odds API: invalid API key")
                return
            if resp.status_code == 429:
                log.warning(f"  ⚠️ Odds API: rate limited (remaining={self.requests_remaining})")
                return
            if resp.status_code != 200:
                log.debug(f"  Odds API returned {resp.status_code} for {sport_key}")
                return

            events = resp.json()
            if not isinstance(events, list):
                return

            # Process events
            processed = []
            for event in events:
                try:
                    parsed = self._parse_event(event, sport_key)
                    if parsed:
                        processed.append(parsed)
                        # Record line snapshot for movement detection
                        self._record_snapshot(parsed)
                except Exception:
                    continue

            # Update cache (thread-safe)
            with self._lock:
                self._cache[sport_key] = processed
                self._cache_ts[sport_key] = time.time()
                self.n_events_cached = sum(len(v) for v in self._cache.values())
                # Invalidate match cache
                self._match_cache.clear()
                self._match_cache_ts = time.time()

            self.n_fetches += 1
            self.last_fetch_ts = time.time()

            our_sport = SPORT_KEY_MAP.get(sport_key, sport_key)
            log.info(f"  📊 Odds: fetched {len(processed)} {our_sport} events "
                     f"(remaining={self.requests_remaining})")

        except requests.exceptions.Timeout:
            log.debug(f"  Odds API timeout for {sport_key}")
            self.n_errors += 1
        except requests.exceptions.ConnectionError:
            log.debug(f"  Odds API connection error for {sport_key}")
            self.n_errors += 1
        except Exception as e:
            log.debug(f"  Odds API error: {e}")
            self.n_errors += 1

    def _parse_event(self, event, sport_key):
        """Parse a single event from the API response."""
        event_id = event.get("id", "")
        home_team = event.get("home_team", "")
        away_team = event.get("away_team", "")

        if not home_team or not away_team:
            return None

        bookmakers = event.get("bookmakers", [])
        if not bookmakers:
            return None

        # Calculate consensus from all bookmakers
        home_probs = []
        away_probs = []
        book_odds = {}  # {bookmaker: {home_prob, away_prob}}

        for book in bookmakers:
            book_key = book.get("key", "")
            markets = book.get("markets", [])
            for market in markets:
                if market.get("key") != "h2h":
                    continue
                outcomes = market.get("outcomes", [])
                home_prob = None
                away_prob = None
                for outcome in outcomes:
                    name = outcome.get("name", "")
                    price = float(outcome.get("price", 0))
                    if price <= 1:
                        continue
                    implied = 1.0 / price
                    if name == home_team:
                        home_prob = implied
                    elif name == away_team:
                        away_prob = implied

                if home_prob and away_prob:
                    # Remove vig: normalize to sum=1
                    total = home_prob + away_prob
                    if total > 0:
                        home_prob /= total
                        away_prob /= total
                    home_probs.append(home_prob)
                    away_probs.append(away_prob)
                    book_odds[book_key] = {
                        "home_prob": round(home_prob, 4),
                        "away_prob": round(away_prob, 4),
                    }

        if not home_probs:
            return None

        # Consensus = average across all books
        consensus_home = sum(home_probs) / len(home_probs)
        consensus_away = sum(away_probs) / len(away_probs)

        # Sharp book consensus (DK, FD — these move on sharp money first)
        sharp_home_probs = []
        for bk in PREFERRED_BOOKS[:3]:
            if bk in book_odds:
                sharp_home_probs.append(book_odds[bk]["home_prob"])
        sharp_consensus_home = (sum(sharp_home_probs) / len(sharp_home_probs)) if sharp_home_probs else consensus_home

        our_sport = SPORT_KEY_MAP.get(sport_key, "other")

        return {
            "event_id": event_id,
            "sport": our_sport,
            "sport_key": sport_key,
            "home_team": home_team,
            "away_team": away_team,
            "consensus_home": round(consensus_home, 4),
            "consensus_away": round(consensus_away, 4),
            "sharp_home": round(sharp_consensus_home, 4),
            "n_books": len(home_probs),
            "book_odds": book_odds,
            "commence_time": event.get("commence_time", ""),
            "fetched_at": time.time(),
        }

    def _record_snapshot(self, parsed_event):
        """Record a line snapshot for movement detection."""
        eid = parsed_event["event_id"]
        snapshot = (
            time.time(),
            parsed_event["consensus_home"],
            parsed_event["consensus_away"],
        )
        with self._lock:
            self._line_snapshots[eid].append(snapshot)
            # Keep only last N snapshots
            if len(self._line_snapshots[eid]) > LINE_SNAPSHOT_MAX:
                self._line_snapshots[eid] = self._line_snapshots[eid][-LINE_SNAPSHOT_MAX:]

    # ─── EDGE CALCULATION ─────────────────────────────────────

    def get_edge(self, poly_title, yes_price, no_price):
        """
        Calculate edge between sportsbook consensus and Polymarket price.

        Args:
            poly_title: Polymarket market title (e.g., "Lightning vs Maple Leafs")
            yes_price: current YES price on Polymarket (e.g., 0.55)
            no_price: current NO price on Polymarket (e.g., 0.45)

        Returns dict or None:
            {
                "edge_cents": float,        # +0.09 = sportsbook 9¢ higher than PM
                "edge_pct": float,           # edge as fraction of yes_price
                "sportsbook_prob": float,    # consensus probability (0-1)
                "sharp_prob": float,         # sharp book probability
                "polymarket_prob": float,    # YES price as probability
                "confidence": float,         # match confidence (0-1)
                "n_books": int,              # number of sportsbooks
                "line_movement": float,      # change since first snapshot
                "line_direction": str,       # "HOME_UP", "HOME_DOWN", "STABLE"
                "elo_prob": float,           # Elo predicted probability
                "elo_agrees": bool,          # Elo within 10% of PM price
                "home_team": str,
                "away_team": str,
                "poly_is_home": bool,        # True if YES side maps to home team
            }
        """
        if not poly_title or yes_price is None or no_price is None:
            return None

        # Check match cache first
        cache_key = poly_title.lower().strip()
        with self._lock:
            if cache_key in self._match_cache:
                cached = self._match_cache[cache_key]
                # Update prices (they change every tick, but match doesn't)
                if cached:
                    return self._compute_edge(cached["_matched_event"], poly_title,
                                              yes_price, no_price, cached["_match_info"])
                return None

        # Find matching sportsbook event
        matched_event, match_info = self._find_match(poly_title)

        with self._lock:
            self._match_cache[cache_key] = {
                "_matched_event": matched_event,
                "_match_info": match_info,
            } if matched_event else None

        if not matched_event:
            return None

        return self._compute_edge(matched_event, poly_title, yes_price, no_price, match_info)

    def _find_match(self, poly_title):
        """
        Find the best matching sportsbook event for a Polymarket title.
        Returns (event_dict, match_info) or (None, None).
        """
        # Parse Polymarket title to get team names
        parsed = match_title(poly_title)
        if not parsed or parsed["overall_confidence"] < 0.65:
            return None, None

        a_full = parsed["team_a"]["full"].lower()
        b_full = parsed["team_b"]["full"].lower()
        sport = parsed["sport"]

        best_event = None
        best_score = 0
        best_info = None

        with self._lock:
            all_events = []
            for sport_key, events in self._cache.items():
                our_sport = SPORT_KEY_MAP.get(sport_key, "other")
                # Prefer same sport but check all if sport is "mixed"
                if sport != "mixed" and our_sport != sport:
                    continue
                all_events.extend(events)

        for event in all_events:
            home_lower = event["home_team"].lower()
            away_lower = event["away_team"].lower()

            # Score: how well does poly team_a/b match sportsbook home/away?
            # Try both orderings: (a=home, b=away) and (a=away, b=home)
            score_ab = self._team_match_score(a_full, home_lower) + self._team_match_score(b_full, away_lower)
            score_ba = self._team_match_score(a_full, away_lower) + self._team_match_score(b_full, home_lower)

            if score_ab >= score_ba and score_ab > best_score:
                best_score = score_ab
                best_event = event
                best_info = {"poly_a_is_home": True, "match_score": score_ab}
            elif score_ba > score_ab and score_ba > best_score:
                best_score = score_ba
                best_event = event
                best_info = {"poly_a_is_home": False, "match_score": score_ba}

        # Need reasonable match quality
        if best_score < 1.0:  # at least one strong match
            return None, None

        return best_event, best_info

    def _team_match_score(self, poly_name, sb_name):
        """Score how well a Polymarket team name matches a sportsbook team name."""
        if not poly_name or not sb_name:
            return 0

        # Exact match
        if poly_name == sb_name:
            return 1.0

        # Substring: "lightning" in "tampa bay lightning"
        if poly_name in sb_name or sb_name in poly_name:
            return 0.85

        # Word overlap
        poly_words = set(poly_name.split())
        sb_words = set(sb_name.split())
        overlap = poly_words & sb_words
        if overlap:
            return 0.7 * len(overlap) / max(len(poly_words), len(sb_words))

        return 0

    def _compute_edge(self, event, poly_title, yes_price, no_price, match_info):
        """Compute the edge given a matched event and current PM prices."""
        if not event or not match_info:
            return None

        # Determine which PM side (YES) maps to home or away
        # In Polymarket: title is "Team A vs Team B"
        # YES typically = Team A wins, NO = Team B wins
        # We need to figure out if Team A = home or away in the sportsbook
        poly_a_is_home = match_info.get("poly_a_is_home", True)

        if poly_a_is_home:
            # YES (Team A) = Home
            sb_yes_prob = event["consensus_home"]
            sharp_yes_prob = event["sharp_home"]
        else:
            # YES (Team A) = Away
            sb_yes_prob = event["consensus_away"]
            sharp_yes_prob = 1.0 - event["sharp_home"]

        # Edge: sportsbook probability minus Polymarket price
        edge_cents = sb_yes_prob - yes_price
        edge_pct = edge_cents / yes_price if yes_price > 0 else 0

        # Line movement
        movement_info = self._get_line_movement(event["event_id"])
        line_movement = movement_info.get("magnitude", 0) if movement_info else 0
        line_direction = movement_info.get("direction", "STABLE") if movement_info else "STABLE"

        # Elo prediction
        parsed = match_title(poly_title)
        elo_prob = None
        elo_agrees = False
        if parsed and parsed["sport"] not in ("mixed", "soccer", "mma"):
            a_key = parsed["team_a"]["key"]
            b_key = parsed["team_b"]["key"]
            sport = parsed["sport"]
            home = "a" if poly_a_is_home else "b"
            pred = self.elo.predict(sport, a_key, b_key, home=home)
            elo_prob = pred["a_prob"]  # probability of Team A (YES side) winning
            # Elo agrees if within 10% of PM price
            elo_agrees = abs(elo_prob - yes_price) < 0.10

        confidence = min(1.0, match_info.get("match_score", 0) / 1.5)

        return {
            "edge_cents": round(edge_cents, 4),
            "edge_pct": round(edge_pct, 4),
            "sportsbook_prob": round(sb_yes_prob, 4),
            "sharp_prob": round(sharp_yes_prob, 4),
            "polymarket_prob": round(yes_price, 4),
            "confidence": round(confidence, 3),
            "n_books": event.get("n_books", 0),
            "line_movement": round(line_movement, 4),
            "line_direction": line_direction,
            "elo_prob": round(elo_prob, 4) if elo_prob is not None else None,
            "elo_agrees": elo_agrees,
            "home_team": event["home_team"],
            "away_team": event["away_team"],
            "poly_is_home": poly_a_is_home,
        }

    # ─── LINE MOVEMENT DETECTION ──────────────────────────────

    def _get_line_movement(self, event_id):
        """
        Detect line movement since first snapshot.

        Returns {
            "magnitude": float,     # absolute change in home probability
            "direction": str,       # "HOME_UP", "HOME_DOWN", "STABLE"
            "opening_prob": float,  # first recorded probability
            "current_prob": float,  # latest probability
            "n_snapshots": int,
        } or None.
        """
        with self._lock:
            snapshots = self._line_snapshots.get(event_id, [])
            if len(snapshots) < 2:
                return None

            opening_home = snapshots[0][1]
            current_home = snapshots[-1][1]

        magnitude = abs(current_home - opening_home)
        if magnitude < 0.005:
            direction = "STABLE"
        elif current_home > opening_home:
            direction = "HOME_UP"
        else:
            direction = "HOME_DOWN"

        return {
            "magnitude": magnitude,
            "direction": direction,
            "opening_prob": opening_home,
            "current_prob": current_home,
            "n_snapshots": len(snapshots),
        }

    # ─── STATS & UTILITY ──────────────────────────────────────

    def stats(self):
        """Return engine status for dashboard."""
        return {
            "fetches": self.n_fetches,
            "errors": self.n_errors,
            "cached_events": self.n_events_cached,
            "last_fetch": self.last_fetch_ts,
            "requests_remaining": self.requests_remaining,
            "api_key_set": bool(self.api_key),
            "sports_cached": list(self._cache.keys()),
            "elo_teams": self.elo.stats()["total_teams"],
        }


# ═══════════════════════════════════════════════════════════════
# TEST / CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    api_key = os.getenv("THE_ODDS_API_KEY", "")
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        if not api_key:
            print("Set THE_ODDS_API_KEY env var to test API connectivity")
            sys.exit(1)

        engine = OddsEngine(api_key=api_key)
        print("Fetching NBA odds...")
        engine._fetch_odds("basketball_nba")

        print(f"\nCached {engine.n_events_cached} events")
        with engine._lock:
            for sport_key, events in engine._cache.items():
                for ev in events[:5]:
                    print(f"  {ev['home_team']} vs {ev['away_team']}: "
                          f"home={ev['consensus_home']:.3f} away={ev['consensus_away']:.3f} "
                          f"({ev['n_books']} books)")

        # Test edge calculation
        print("\nEdge tests:")
        tests = [
            ("Lightning vs Maple Leafs", 0.55, 0.45),
            ("Celtics vs Lakers", 0.65, 0.35),
        ]
        for title, yes_p, no_p in tests:
            edge = engine.get_edge(title, yes_p, no_p)
            if edge:
                print(f"  {title}: edge={edge['edge_cents']:+.3f} "
                      f"SB={edge['sportsbook_prob']:.3f} PM={edge['polymarket_prob']:.3f} "
                      f"conf={edge['confidence']:.2f}")
            else:
                print(f"  {title}: no match (odds may not be cached yet)")

    else:
        # Quick smoke test without API
        engine = OddsEngine()
        print("OddsEngine initialized (no API key — offline mode)")
        print(f"Elo stats: {engine.elo.stats()}")

        # Test Elo-only edge (no sportsbook data)
        parsed = match_title("Lightning vs Maple Leafs")
        if parsed:
            a_key = parsed["team_a"]["key"]
            b_key = parsed["team_b"]["key"]
            sport = parsed["sport"]
            pred = engine.elo.predict(sport, a_key, b_key)
            print(f"\nElo prediction for Lightning vs Maple Leafs:")
            print(f"  Lightning: {pred['a_prob']*100:.1f}% ({pred['a_elo']:.0f})")
            print(f"  Maple Leafs: {pred['b_prob']*100:.1f}% ({pred['b_elo']:.0f})")
