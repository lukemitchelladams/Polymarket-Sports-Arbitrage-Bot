"""
elo_ratings.py — Simple Elo rating model for sports teams.

Seeded from approximate 2025-26 season standings.
Updated live as the bot detects game resolutions.
Stored persistently in elo_ratings.json.

Elo formula:
  Expected = 1 / (1 + 10^((opp_rating - my_rating) / 400))
  New_rating = Old_rating + K * (Result - Expected)

  K = 32 (standard), Starting = 1500
  Seed from win%: 1500 + 400 * (win_pct - 0.5)
"""

import json
import os
import time
import logging

log = logging.getLogger("arb")

# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════

ELO_K = 32                  # standard K-factor
ELO_BASE = 1500             # starting rating
ELO_HOME_ADVANTAGE = 50     # home team gets +50 Elo boost in predictions
ELO_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "elo_ratings.json")

# ═══════════════════════════════════════════════════════════════
# INITIAL SEED DATA — approximate 2025-26 records
# Format: {team_canonical_key: (wins, losses)}
# These get converted to Elo via: 1500 + 400 * (W/(W+L) - 0.5)
#
# Sources: approximate mid-season records as of early March 2026.
# Values don't need to be exact — they converge after ~20 games.
# ═══════════════════════════════════════════════════════════════

NBA_SEED = {
    # Eastern Conference (approx March 2026)
    "celtics": (48, 16), "cavaliers": (48, 17), "knicks": (42, 22),
    "bucks": (38, 26), "pacers": (36, 28), "magic": (35, 29),
    "hawks": (33, 31), "pistons": (33, 31), "heat": (31, 33),
    "bulls": (30, 34), "76ers": (28, 36), "raptors": (24, 40),
    "nets": (22, 42), "hornets": (20, 44), "wizards": (16, 48),
    # Western Conference
    "thunder": (50, 14), "rockets": (42, 22), "grizzlies": (40, 24),
    "warriors": (38, 26), "nuggets": (37, 27), "timberwolves": (36, 28),
    "lakers": (36, 28), "clippers": (34, 30), "kings": (34, 30),
    "mavericks": (33, 31), "suns": (32, 32), "spurs": (30, 34),
    "trail blazers": (24, 40), "pelicans": (22, 42), "jazz": (18, 46),
}

NHL_SEED = {
    # Eastern Conference (approx March 2026)
    "panthers": (42, 18), "hurricanes": (40, 20), "capitals": (39, 21),
    "maple leafs": (38, 22), "devils": (37, 23), "rangers": (36, 24),
    "lightning": (36, 24), "senators": (34, 26), "bruins": (32, 28),
    "penguins": (30, 30), "islanders": (28, 32), "blue jackets": (27, 33),
    "canadiens": (26, 34), "flyers": (25, 35), "red wings": (24, 36),
    "sabres": (22, 38),
    # Western Conference
    "jets": (46, 14), "avalanche": (40, 20), "stars": (38, 22),
    "wild": (37, 23), "golden knights": (36, 24), "oilers": (35, 25),
    "kings_nhl": (34, 26), "canucks": (32, 28), "flames": (31, 29),
    "predators": (28, 32), "blues": (27, 33), "kraken": (26, 34),
    "ducks": (24, 36), "blackhawks": (22, 38), "sharks": (20, 40),
    "coyotes": (18, 42),
}

MLB_SEED = {
    # Using 2025 final standings as seed (2026 season hasn't started by March)
    # AL East
    "yankees": (90, 72), "orioles": (88, 74), "red sox": (82, 80),
    "blue jays": (78, 84), "rays": (76, 86),
    # AL Central
    "guardians": (92, 70), "royals": (86, 76), "twins": (82, 80),
    "tigers": (80, 82), "white sox": (62, 100),
    # AL West
    "astros": (88, 74), "mariners": (82, 80), "rangers_mlb": (78, 84),
    "angels": (72, 90), "athletics": (68, 94),
    # NL East
    "phillies": (92, 70), "braves": (88, 74), "mets": (86, 76),
    "marlins": (70, 92), "nationals": (68, 94),
    # NL Central
    "brewers": (90, 72), "cubs": (84, 78), "cardinals": (80, 82),
    "reds": (78, 84), "pirates": (72, 90),
    # NL West
    "dodgers": (96, 66), "padres": (88, 74), "diamondbacks": (84, 78),
    "giants": (76, 86), "rockies": (62, 100),
}

NFL_SEED = {
    # Using 2025-26 season records (regular season)
    "chiefs": (13, 4), "bills": (13, 4), "ravens": (12, 5),
    "lions": (12, 5), "eagles": (12, 5), "vikings": (14, 3),
    "packers": (11, 6), "texans": (10, 7), "steelers": (10, 7),
    "chargers": (10, 7), "broncos": (10, 7), "commanders": (10, 7),
    "buccaneers": (10, 7), "rams": (10, 7), "seahawks": (9, 8),
    "49ers": (8, 9), "falcons": (8, 9), "bears": (8, 9),
    "colts": (7, 10), "cowboys": (7, 10), "bengals": (7, 10),
    "saints": (7, 10), "dolphins": (6, 11), "jets_nfl": (5, 12),
    "cardinals_nfl": (5, 12), "patriots": (4, 13), "jaguars": (4, 13),
    "titans": (4, 13), "browns": (3, 14), "giants_nfl": (3, 14),
    "raiders": (4, 13), "panthers_nfl": (4, 13),
}

ALL_SEEDS = {
    "nba": NBA_SEED,
    "nhl": NHL_SEED,
    "mlb": MLB_SEED,
    "nfl": NFL_SEED,
}


# ═══════════════════════════════════════════════════════════════
# ELO MODEL
# ═══════════════════════════════════════════════════════════════

class EloModel:
    """
    Maintains Elo ratings for teams across all sports.

    Public API:
    - predict(sport, team_a_key, team_b_key, home="a"|"b"|None) → {
          "a_prob": 0.63, "b_prob": 0.37,
          "a_elo": 1680, "b_elo": 1520,
      }
    - update(sport, winner_key, loser_key, draw=False)
    - get_rating(sport, team_key) → float
    - save() / load()
    """

    def __init__(self, filepath=None):
        self.filepath = filepath or ELO_FILE
        self.ratings = {}  # {sport: {team_key: float}}
        self._dirty = False
        self._last_save = 0

        if not self.load():
            self._seed_from_standings()
            self.save()

    def _seed_from_standings(self):
        """Initialize ratings from win/loss records."""
        for sport, teams in ALL_SEEDS.items():
            self.ratings[sport] = {}
            for key, (w, l) in teams.items():
                total = w + l
                if total > 0:
                    win_pct = w / total
                    elo = ELO_BASE + 400 * (win_pct - 0.5)
                else:
                    elo = ELO_BASE
                self.ratings[sport][key] = round(elo, 1)
        log.info(f"  📊 Elo: seeded {sum(len(v) for v in self.ratings.values())} teams from standings")

    def get_rating(self, sport, team_key):
        """Get current Elo rating. Returns ELO_BASE for unknown teams."""
        return self.ratings.get(sport, {}).get(team_key, ELO_BASE)

    def predict(self, sport, team_a_key, team_b_key, home=None):
        """
        Predict outcome probability.

        Args:
            sport: "nba", "nhl", etc.
            team_a_key: canonical key for team A
            team_b_key: canonical key for team B
            home: "a" if team A is home, "b" if team B is home, None if unknown

        Returns:
            {"a_prob": float, "b_prob": float, "a_elo": float, "b_elo": float}
        """
        a_elo = self.get_rating(sport, team_a_key)
        b_elo = self.get_rating(sport, team_b_key)

        # Apply home advantage
        effective_a = a_elo + (ELO_HOME_ADVANTAGE if home == "a" else 0)
        effective_b = b_elo + (ELO_HOME_ADVANTAGE if home == "b" else 0)

        # Elo expected score
        exp_a = 1.0 / (1.0 + 10 ** ((effective_b - effective_a) / 400.0))
        exp_b = 1.0 - exp_a

        return {
            "a_prob": round(exp_a, 4),
            "b_prob": round(exp_b, 4),
            "a_elo": round(a_elo, 1),
            "b_elo": round(b_elo, 1),
        }

    def update(self, sport, winner_key, loser_key, draw=False):
        """
        Update ratings after a game result.

        For arb bot: called when record_resolution() detects a game ended.
        """
        if sport not in self.ratings:
            self.ratings[sport] = {}

        w_elo = self.ratings.get(sport, {}).get(winner_key, ELO_BASE)
        l_elo = self.ratings.get(sport, {}).get(loser_key, ELO_BASE)

        # Expected scores
        exp_w = 1.0 / (1.0 + 10 ** ((l_elo - w_elo) / 400.0))
        exp_l = 1.0 - exp_w

        if draw:
            result_w = 0.5
            result_l = 0.5
        else:
            result_w = 1.0
            result_l = 0.0

        new_w = w_elo + ELO_K * (result_w - exp_w)
        new_l = l_elo + ELO_K * (result_l - exp_l)

        self.ratings[sport][winner_key] = round(new_w, 1)
        self.ratings[sport][loser_key] = round(new_l, 1)
        self._dirty = True

        # Auto-save every 5 minutes
        if time.time() - self._last_save > 300:
            self.save()

        return {
            "winner": {"old": round(w_elo, 1), "new": round(new_w, 1), "delta": round(new_w - w_elo, 1)},
            "loser": {"old": round(l_elo, 1), "new": round(new_l, 1), "delta": round(new_l - l_elo, 1)},
        }

    def save(self):
        """Persist ratings to disk."""
        try:
            with open(self.filepath, "w") as f:
                json.dump(self.ratings, f, indent=2)
            self._dirty = False
            self._last_save = time.time()
        except Exception as e:
            log.debug(f"Elo save error: {e}")

    def load(self):
        """Load ratings from disk. Returns True if loaded successfully."""
        try:
            if os.path.exists(self.filepath):
                with open(self.filepath) as f:
                    self.ratings = json.load(f)
                total = sum(len(v) for v in self.ratings.values())
                log.info(f"  📊 Elo: loaded {total} team ratings from {self.filepath}")
                return True
        except Exception as e:
            log.debug(f"Elo load error: {e}")
        return False

    def stats(self):
        """Return summary stats."""
        return {
            "sports": list(self.ratings.keys()),
            "total_teams": sum(len(v) for v in self.ratings.values()),
            "dirty": self._dirty,
        }


# ═══════════════════════════════════════════════════════════════
# TEST / CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    model = EloModel(filepath="/tmp/test_elo.json")

    print("=" * 60)
    print("ELO RATINGS MODEL — TEST")
    print("=" * 60)

    # Show some ratings
    for sport in ["nba", "nhl", "mlb", "nfl"]:
        teams = model.ratings.get(sport, {})
        if teams:
            sorted_teams = sorted(teams.items(), key=lambda x: x[1], reverse=True)
            print(f"\n  {sport.upper()} — Top 5:")
            for key, elo in sorted_teams[:5]:
                print(f"    {key:20s} {elo:.0f}")

    # Test predictions
    print("\n  PREDICTIONS:")
    tests = [
        ("nba", "celtics", "lakers", "a"),
        ("nhl", "lightning", "maple leafs", None),
        ("mlb", "dodgers", "yankees", "b"),
        ("nfl", "chiefs", "bills", "a"),
    ]
    for sport, a, b, home in tests:
        pred = model.predict(sport, a, b, home=home)
        home_tag = f" (home={home})" if home else ""
        print(f"    {a} vs {b}{home_tag}: "
              f"{a} {pred['a_prob']*100:.1f}% ({pred['a_elo']:.0f}) | "
              f"{b} {pred['b_prob']*100:.1f}% ({pred['b_elo']:.0f})")

    # Test update
    print("\n  UPDATE TEST:")
    result = model.update("nba", "lakers", "celtics")
    print(f"    Lakers beat Celtics:")
    print(f"      Lakers: {result['winner']['old']:.0f} → {result['winner']['new']:.0f} ({result['winner']['delta']:+.1f})")
    print(f"      Celtics: {result['loser']['old']:.0f} → {result['loser']['new']:.0f} ({result['loser']['delta']:+.1f})")

    print(f"\n  Stats: {model.stats()}")
    print()
