"""
team_name_mapper.py — Fuzzy matching for Polymarket ↔ sportsbook team names.

Maps abbreviated/informal Polymarket titles like "Lightning vs Maple Leafs"
to full sportsbook names like "Tampa Bay Lightning @ Toronto Maple Leafs".

Uses difflib.SequenceMatcher (no external dependencies) with a master alias
dictionary covering ~200+ teams across NBA, NHL, MLB, NFL, EPL, La Liga,
Bundesliga, Serie A, Ligue 1, MMA, and tennis.
"""

import re
from difflib import SequenceMatcher

# ═══════════════════════════════════════════════════════════════
# MASTER TEAM ALIASES — {canonical_key: {full, abbrev, alts[]}}
# canonical_key = lowercase short name used for fast lookup
# ═══════════════════════════════════════════════════════════════

NBA_TEAMS = {
    "celtics":      {"full": "Boston Celtics",          "abbrev": "BOS", "alts": ["boston"]},
    "nets":         {"full": "Brooklyn Nets",           "abbrev": "BKN", "alts": ["brooklyn"]},
    "knicks":       {"full": "New York Knicks",         "abbrev": "NYK", "alts": ["ny knicks", "new york"]},
    "76ers":        {"full": "Philadelphia 76ers",      "abbrev": "PHI", "alts": ["philadelphia", "sixers", "philly"]},
    "raptors":      {"full": "Toronto Raptors",         "abbrev": "TOR", "alts": ["toronto"]},
    "bulls":        {"full": "Chicago Bulls",           "abbrev": "CHI", "alts": ["chicago"]},
    "cavaliers":    {"full": "Cleveland Cavaliers",     "abbrev": "CLE", "alts": ["cleveland", "cavs"]},
    "pistons":      {"full": "Detroit Pistons",         "abbrev": "DET", "alts": ["detroit"]},
    "pacers":       {"full": "Indiana Pacers",          "abbrev": "IND", "alts": ["indiana"]},
    "bucks":        {"full": "Milwaukee Bucks",         "abbrev": "MIL", "alts": ["milwaukee"]},
    "hawks":        {"full": "Atlanta Hawks",           "abbrev": "ATL", "alts": ["atlanta"]},
    "hornets":      {"full": "Charlotte Hornets",       "abbrev": "CHA", "alts": ["charlotte"]},
    "heat":         {"full": "Miami Heat",              "abbrev": "MIA", "alts": ["miami"]},
    "magic":        {"full": "Orlando Magic",           "abbrev": "ORL", "alts": ["orlando"]},
    "wizards":      {"full": "Washington Wizards",      "abbrev": "WAS", "alts": ["washington"]},
    "nuggets":      {"full": "Denver Nuggets",          "abbrev": "DEN", "alts": ["denver"]},
    "timberwolves": {"full": "Minnesota Timberwolves",  "abbrev": "MIN", "alts": ["minnesota", "wolves", "twolves"]},
    "thunder":      {"full": "Oklahoma City Thunder",   "abbrev": "OKC", "alts": ["oklahoma city", "okc"]},
    "trail blazers":{"full": "Portland Trail Blazers",  "abbrev": "POR", "alts": ["portland", "blazers"]},
    "jazz":         {"full": "Utah Jazz",               "abbrev": "UTA", "alts": ["utah"]},
    "warriors":     {"full": "Golden State Warriors",   "abbrev": "GSW", "alts": ["golden state", "gs warriors"]},
    "clippers":     {"full": "LA Clippers",             "abbrev": "LAC", "alts": ["los angeles clippers", "la clippers"]},
    "lakers":       {"full": "Los Angeles Lakers",      "abbrev": "LAL", "alts": ["la lakers"]},
    "suns":         {"full": "Phoenix Suns",            "abbrev": "PHX", "alts": ["phoenix"]},
    "kings":        {"full": "Sacramento Kings",        "abbrev": "SAC", "alts": ["sacramento"]},
    "mavericks":    {"full": "Dallas Mavericks",        "abbrev": "DAL", "alts": ["dallas", "mavs"]},
    "rockets":      {"full": "Houston Rockets",         "abbrev": "HOU", "alts": ["houston"]},
    "grizzlies":    {"full": "Memphis Grizzlies",       "abbrev": "MEM", "alts": ["memphis"]},
    "pelicans":     {"full": "New Orleans Pelicans",    "abbrev": "NOP", "alts": ["new orleans", "pels"]},
    "spurs":        {"full": "San Antonio Spurs",       "abbrev": "SAS", "alts": ["san antonio"]},
}

NHL_TEAMS = {
    "bruins":       {"full": "Boston Bruins",           "abbrev": "BOS", "alts": ["boston"]},
    "sabres":       {"full": "Buffalo Sabres",          "abbrev": "BUF", "alts": ["buffalo"]},
    "red wings":    {"full": "Detroit Red Wings",       "abbrev": "DET", "alts": ["detroit"]},
    "panthers":     {"full": "Florida Panthers",        "abbrev": "FLA", "alts": ["florida"]},
    "canadiens":    {"full": "Montreal Canadiens",      "abbrev": "MTL", "alts": ["montreal", "habs"]},
    "senators":     {"full": "Ottawa Senators",         "abbrev": "OTT", "alts": ["ottawa", "sens"]},
    "lightning":    {"full": "Tampa Bay Lightning",     "abbrev": "TB",  "alts": ["tampa bay", "tampa", "tbl"]},
    "maple leafs":  {"full": "Toronto Maple Leafs",    "abbrev": "TOR", "alts": ["toronto", "leafs"]},
    "hurricanes":   {"full": "Carolina Hurricanes",     "abbrev": "CAR", "alts": ["carolina", "canes"]},
    "blue jackets": {"full": "Columbus Blue Jackets",   "abbrev": "CBJ", "alts": ["columbus"]},
    "devils":       {"full": "New Jersey Devils",       "abbrev": "NJD", "alts": ["new jersey", "nj devils"]},
    "islanders":    {"full": "New York Islanders",      "abbrev": "NYI", "alts": ["ny islanders"]},
    "rangers":      {"full": "New York Rangers",        "abbrev": "NYR", "alts": ["ny rangers"]},
    "flyers":       {"full": "Philadelphia Flyers",     "abbrev": "PHI", "alts": ["philadelphia"]},
    "penguins":     {"full": "Pittsburgh Penguins",     "abbrev": "PIT", "alts": ["pittsburgh", "pens"]},
    "capitals":     {"full": "Washington Capitals",     "abbrev": "WSH", "alts": ["washington", "caps"]},
    "blackhawks":   {"full": "Chicago Blackhawks",      "abbrev": "CHI", "alts": ["chicago"]},
    "avalanche":    {"full": "Colorado Avalanche",      "abbrev": "COL", "alts": ["colorado", "avs"]},
    "stars":        {"full": "Dallas Stars",            "abbrev": "DAL", "alts": ["dallas"]},
    "wild":         {"full": "Minnesota Wild",          "abbrev": "MIN", "alts": ["minnesota"]},
    "predators":    {"full": "Nashville Predators",     "abbrev": "NSH", "alts": ["nashville", "preds"]},
    "blues":        {"full": "St. Louis Blues",         "abbrev": "STL", "alts": ["st louis", "saint louis"]},
    "jets":         {"full": "Winnipeg Jets",           "abbrev": "WPG", "alts": ["winnipeg"]},
    "ducks":        {"full": "Anaheim Ducks",           "abbrev": "ANA", "alts": ["anaheim"]},
    "flames":       {"full": "Calgary Flames",          "abbrev": "CGY", "alts": ["calgary"]},
    "oilers":       {"full": "Edmonton Oilers",         "abbrev": "EDM", "alts": ["edmonton"]},
    "kings_nhl":    {"full": "Los Angeles Kings",       "abbrev": "LAK", "alts": ["la kings", "los angeles kings"]},
    "sharks":       {"full": "San Jose Sharks",         "abbrev": "SJS", "alts": ["san jose"]},
    "kraken":       {"full": "Seattle Kraken",          "abbrev": "SEA", "alts": ["seattle"]},
    "canucks":      {"full": "Vancouver Canucks",       "abbrev": "VAN", "alts": ["vancouver"]},
    "golden knights":{"full": "Vegas Golden Knights",   "abbrev": "VGK", "alts": ["vegas", "vgk", "golden knights"]},
    "coyotes":      {"full": "Utah Hockey Club",        "abbrev": "UTA", "alts": ["utah hc", "utah hockey"]},
}

MLB_TEAMS = {
    "yankees":      {"full": "New York Yankees",        "abbrev": "NYY", "alts": ["ny yankees"]},
    "red sox":      {"full": "Boston Red Sox",           "abbrev": "BOS", "alts": ["boston"]},
    "blue jays":    {"full": "Toronto Blue Jays",        "abbrev": "TOR", "alts": ["toronto"]},
    "orioles":      {"full": "Baltimore Orioles",        "abbrev": "BAL", "alts": ["baltimore"]},
    "rays":         {"full": "Tampa Bay Rays",           "abbrev": "TB",  "alts": ["tampa bay"]},
    "white sox":    {"full": "Chicago White Sox",        "abbrev": "CWS", "alts": ["chi white sox"]},
    "guardians":    {"full": "Cleveland Guardians",      "abbrev": "CLE", "alts": ["cleveland"]},
    "tigers":       {"full": "Detroit Tigers",           "abbrev": "DET", "alts": ["detroit"]},
    "royals":       {"full": "Kansas City Royals",       "abbrev": "KC",  "alts": ["kansas city"]},
    "twins":        {"full": "Minnesota Twins",          "abbrev": "MIN", "alts": ["minnesota"]},
    "astros":       {"full": "Houston Astros",           "abbrev": "HOU", "alts": ["houston"]},
    "angels":       {"full": "Los Angeles Angels",       "abbrev": "LAA", "alts": ["la angels", "anaheim angels"]},
    "athletics":    {"full": "Oakland Athletics",        "abbrev": "OAK", "alts": ["oakland", "a's"]},
    "mariners":     {"full": "Seattle Mariners",         "abbrev": "SEA", "alts": ["seattle"]},
    "rangers_mlb":  {"full": "Texas Rangers",            "abbrev": "TEX", "alts": ["texas"]},
    "braves":       {"full": "Atlanta Braves",           "abbrev": "ATL", "alts": ["atlanta"]},
    "marlins":      {"full": "Miami Marlins",            "abbrev": "MIA", "alts": ["miami"]},
    "mets":         {"full": "New York Mets",            "abbrev": "NYM", "alts": ["ny mets"]},
    "phillies":     {"full": "Philadelphia Phillies",    "abbrev": "PHI", "alts": ["philadelphia"]},
    "nationals":    {"full": "Washington Nationals",     "abbrev": "WSH", "alts": ["washington", "nats"]},
    "cubs":         {"full": "Chicago Cubs",             "abbrev": "CHC", "alts": ["chi cubs"]},
    "reds":         {"full": "Cincinnati Reds",          "abbrev": "CIN", "alts": ["cincinnati"]},
    "brewers":      {"full": "Milwaukee Brewers",        "abbrev": "MIL", "alts": ["milwaukee"]},
    "pirates":      {"full": "Pittsburgh Pirates",       "abbrev": "PIT", "alts": ["pittsburgh"]},
    "cardinals":    {"full": "St. Louis Cardinals",      "abbrev": "STL", "alts": ["st louis", "cards"]},
    "diamondbacks": {"full": "Arizona Diamondbacks",     "abbrev": "ARI", "alts": ["arizona", "dbacks"]},
    "rockies":      {"full": "Colorado Rockies",         "abbrev": "COL", "alts": ["colorado"]},
    "dodgers":      {"full": "Los Angeles Dodgers",      "abbrev": "LAD", "alts": ["la dodgers"]},
    "padres":       {"full": "San Diego Padres",         "abbrev": "SD",  "alts": ["san diego"]},
    "giants":       {"full": "San Francisco Giants",     "abbrev": "SF",  "alts": ["san francisco", "sf giants"]},
}

NFL_TEAMS = {
    "bills":        {"full": "Buffalo Bills",            "abbrev": "BUF", "alts": ["buffalo"]},
    "dolphins":     {"full": "Miami Dolphins",           "abbrev": "MIA", "alts": ["miami"]},
    "patriots":     {"full": "New England Patriots",     "abbrev": "NE",  "alts": ["new england", "pats"]},
    "jets_nfl":     {"full": "New York Jets",            "abbrev": "NYJ", "alts": ["ny jets"]},
    "ravens":       {"full": "Baltimore Ravens",         "abbrev": "BAL", "alts": ["baltimore"]},
    "bengals":      {"full": "Cincinnati Bengals",       "abbrev": "CIN", "alts": ["cincinnati"]},
    "browns":       {"full": "Cleveland Browns",         "abbrev": "CLE", "alts": ["cleveland"]},
    "steelers":     {"full": "Pittsburgh Steelers",      "abbrev": "PIT", "alts": ["pittsburgh"]},
    "texans":       {"full": "Houston Texans",           "abbrev": "HOU", "alts": ["houston"]},
    "colts":        {"full": "Indianapolis Colts",       "abbrev": "IND", "alts": ["indianapolis", "indy"]},
    "jaguars":      {"full": "Jacksonville Jaguars",     "abbrev": "JAX", "alts": ["jacksonville", "jags"]},
    "titans":       {"full": "Tennessee Titans",         "abbrev": "TEN", "alts": ["tennessee"]},
    "cowboys":      {"full": "Dallas Cowboys",           "abbrev": "DAL", "alts": ["dallas"]},
    "giants_nfl":   {"full": "New York Giants",          "abbrev": "NYG", "alts": ["ny giants"]},
    "eagles":       {"full": "Philadelphia Eagles",      "abbrev": "PHI", "alts": ["philadelphia"]},
    "commanders":   {"full": "Washington Commanders",    "abbrev": "WAS", "alts": ["washington"]},
    "bears":        {"full": "Chicago Bears",            "abbrev": "CHI", "alts": ["chicago"]},
    "lions":        {"full": "Detroit Lions",            "abbrev": "DET", "alts": ["detroit"]},
    "packers":      {"full": "Green Bay Packers",        "abbrev": "GB",  "alts": ["green bay"]},
    "vikings":      {"full": "Minnesota Vikings",        "abbrev": "MIN", "alts": ["minnesota"]},
    "falcons":      {"full": "Atlanta Falcons",          "abbrev": "ATL", "alts": ["atlanta"]},
    "panthers_nfl": {"full": "Carolina Panthers",        "abbrev": "CAR", "alts": ["carolina"]},
    "saints":       {"full": "New Orleans Saints",       "abbrev": "NO",  "alts": ["new orleans"]},
    "buccaneers":   {"full": "Tampa Bay Buccaneers",     "abbrev": "TB",  "alts": ["tampa bay", "bucs"]},
    "broncos":      {"full": "Denver Broncos",           "abbrev": "DEN", "alts": ["denver"]},
    "chiefs":       {"full": "Kansas City Chiefs",       "abbrev": "KC",  "alts": ["kansas city"]},
    "raiders":      {"full": "Las Vegas Raiders",        "abbrev": "LV",  "alts": ["las vegas"]},
    "chargers":     {"full": "Los Angeles Chargers",     "abbrev": "LAC", "alts": ["la chargers"]},
    "cardinals_nfl":{"full": "Arizona Cardinals",        "abbrev": "ARI", "alts": ["arizona"]},
    "rams":         {"full": "Los Angeles Rams",         "abbrev": "LAR", "alts": ["la rams"]},
    "49ers":        {"full": "San Francisco 49ers",      "abbrev": "SF",  "alts": ["san francisco", "niners"]},
    "seahawks":     {"full": "Seattle Seahawks",         "abbrev": "SEA", "alts": ["seattle"]},
}

SOCCER_TEAMS = {
    # EPL
    "arsenal":          {"full": "Arsenal",              "abbrev": "ARS", "alts": ["gunners"]},
    "aston villa":      {"full": "Aston Villa",          "abbrev": "AVL", "alts": ["villa"]},
    "bournemouth":      {"full": "AFC Bournemouth",      "abbrev": "BOU", "alts": ["cherries"]},
    "brentford":        {"full": "Brentford",            "abbrev": "BRE", "alts": ["bees"]},
    "brighton":         {"full": "Brighton & Hove Albion","abbrev": "BHA", "alts": ["brighton hove", "seagulls"]},
    "chelsea":          {"full": "Chelsea",              "abbrev": "CHE", "alts": ["blues"]},
    "crystal palace":   {"full": "Crystal Palace",       "abbrev": "CRY", "alts": ["palace", "eagles"]},
    "everton":          {"full": "Everton",              "abbrev": "EVE", "alts": ["toffees"]},
    "fulham":           {"full": "Fulham",               "abbrev": "FUL", "alts": ["cottagers"]},
    "ipswich":          {"full": "Ipswich Town",         "abbrev": "IPS", "alts": ["ipswich town"]},
    "leicester":        {"full": "Leicester City",       "abbrev": "LEI", "alts": ["foxes"]},
    "liverpool":        {"full": "Liverpool",            "abbrev": "LIV", "alts": ["reds"]},
    "manchester city":  {"full": "Manchester City",      "abbrev": "MCI", "alts": ["man city", "city", "mcfc"]},
    "manchester united":{"full": "Manchester United",    "abbrev": "MUN", "alts": ["man utd", "man united", "mufc", "united"]},
    "newcastle":        {"full": "Newcastle United",     "abbrev": "NEW", "alts": ["newcastle utd", "magpies", "toon"]},
    "nottingham forest":{"full": "Nottingham Forest",    "abbrev": "NFO", "alts": ["forest", "nffc"]},
    "southampton":      {"full": "Southampton",          "abbrev": "SOU", "alts": ["saints"]},
    "tottenham":        {"full": "Tottenham Hotspur",    "abbrev": "TOT", "alts": ["spurs", "tottenham hotspur"]},
    "west ham":         {"full": "West Ham United",      "abbrev": "WHU", "alts": ["west ham utd", "hammers"]},
    "wolves":           {"full": "Wolverhampton Wanderers","abbrev": "WOL", "alts": ["wolverhampton", "wanderers"]},
    # La Liga
    "barcelona":        {"full": "FC Barcelona",         "abbrev": "BAR", "alts": ["barca", "fcb"]},
    "real madrid":      {"full": "Real Madrid",          "abbrev": "RMA", "alts": ["madrid", "rmcf"]},
    "atletico madrid":  {"full": "Atletico Madrid",      "abbrev": "ATM", "alts": ["atletico", "atleti"]},
    "real sociedad":    {"full": "Real Sociedad",        "abbrev": "RSO", "alts": ["sociedad", "la real"]},
    "villarreal":       {"full": "Villarreal CF",        "abbrev": "VIL", "alts": ["yellow submarine"]},
    "athletic bilbao":  {"full": "Athletic Bilbao",      "abbrev": "ATH", "alts": ["athletic club", "bilbao"]},
    "real betis":       {"full": "Real Betis",           "abbrev": "BET", "alts": ["betis"]},
    "sevilla":          {"full": "Sevilla FC",           "abbrev": "SEV", "alts": ["sevilla fc"]},
    # Bundesliga
    "bayern munich":    {"full": "Bayern Munich",        "abbrev": "BAY", "alts": ["bayern", "fcb munich"]},
    "dortmund":         {"full": "Borussia Dortmund",    "abbrev": "BVB", "alts": ["borussia dortmund", "bvb"]},
    "leverkusen":       {"full": "Bayer Leverkusen",     "abbrev": "LEV", "alts": ["bayer leverkusen"]},
    "rb leipzig":       {"full": "RB Leipzig",           "abbrev": "RBL", "alts": ["leipzig"]},
    # Serie A
    "inter milan":      {"full": "Inter Milan",          "abbrev": "INT", "alts": ["inter", "internazionale"]},
    "ac milan":         {"full": "AC Milan",             "abbrev": "MIL", "alts": ["milan"]},
    "juventus":         {"full": "Juventus",             "abbrev": "JUV", "alts": ["juve"]},
    "napoli":           {"full": "SSC Napoli",           "abbrev": "NAP", "alts": ["napoli"]},
    "roma":             {"full": "AS Roma",              "abbrev": "ROM", "alts": ["as roma"]},
    "lazio":            {"full": "SS Lazio",             "abbrev": "LAZ", "alts": ["lazio"]},
    "atalanta":         {"full": "Atalanta",             "abbrev": "ATA", "alts": ["atalanta bc"]},
    # Ligue 1
    "psg":              {"full": "Paris Saint-Germain",   "abbrev": "PSG", "alts": ["paris sg", "paris"]},
    "marseille":        {"full": "Olympique Marseille",   "abbrev": "OM",  "alts": ["om", "olympique marseille"]},
    "lyon":             {"full": "Olympique Lyonnais",    "abbrev": "OL",  "alts": ["olympique lyon"]},
    "monaco":           {"full": "AS Monaco",             "abbrev": "MON", "alts": ["as monaco"]},
    "lille":            {"full": "Lille OSC",              "abbrev": "LIL", "alts": ["losc", "lille osc"]},
    # Liga MX (common on Polymarket)
    "club america":     {"full": "Club America",          "abbrev": "AME", "alts": ["america", "aguilas"]},
    "guadalajara":      {"full": "CD Guadalajara",        "abbrev": "GDL", "alts": ["chivas"]},
    "monterrey":        {"full": "CF Monterrey",          "abbrev": "MTY", "alts": ["rayados"]},
    "tigres":           {"full": "Tigres UANL",           "abbrev": "TIG", "alts": ["tigres uanl"]},
    "cruz azul":        {"full": "Cruz Azul",             "abbrev": "CAZ", "alts": ["maquina"]},
    "toluca":           {"full": "Deportivo Toluca",      "abbrev": "TOL", "alts": ["diablos"]},
    "pumas":            {"full": "Pumas UNAM",            "abbrev": "PUM", "alts": ["pumas unam"]},
    "santos laguna":    {"full": "Santos Laguna",         "abbrev": "SAN", "alts": ["santos"]},
    "leon":             {"full": "Club Leon",             "abbrev": "LEO", "alts": ["leon fc"]},
    "queretaro":        {"full": "Queretaro FC",          "abbrev": "QUE", "alts": ["gallos"]},
    "atlas":            {"full": "Atlas FC",              "abbrev": "ATL", "alts": ["atlas"]},
    "pachuca":          {"full": "CF Pachuca",            "abbrev": "PAC", "alts": ["tuzos"]},
    "necaxa":           {"full": "Club Necaxa",           "abbrev": "NEC", "alts": ["rayos"]},
    "mazatlan":         {"full": "Mazatlan FC",           "abbrev": "MAZ", "alts": ["canoneros"]},
    "puebla":           {"full": "Club Puebla",           "abbrev": "PUE", "alts": ["franja"]},
    "juarez":           {"full": "FC Juarez",             "abbrev": "JUA", "alts": ["bravos"]},
    "tijuana":          {"full": "Club Tijuana",          "abbrev": "TIJ", "alts": ["xolos"]},
    "san luis":         {"full": "Atletico San Luis",     "abbrev": "SLP", "alts": ["san luis"]},
}

# ═══════════════════════════════════════════════════════════════
# BUILD REVERSE LOOKUP — every alias/name/abbrev → (sport, canonical_key)
# ═══════════════════════════════════════════════════════════════

ALL_SPORT_DICTS = {
    "nba": NBA_TEAMS,
    "nhl": NHL_TEAMS,
    "mlb": MLB_TEAMS,
    "nfl": NFL_TEAMS,
    "soccer": SOCCER_TEAMS,
}

_reverse_lookup = {}  # lowercase_token → [(sport, canonical_key, match_type)]

def _build_reverse():
    for sport, teams in ALL_SPORT_DICTS.items():
        for key, info in teams.items():
            # Canonical key
            _reverse_lookup.setdefault(key.lower(), []).append((sport, key, "canonical"))
            # Full name (lowercase)
            _reverse_lookup.setdefault(info["full"].lower(), []).append((sport, key, "full"))
            # Abbreviation
            _reverse_lookup.setdefault(info["abbrev"].lower(), []).append((sport, key, "abbrev"))
            # Alternates
            for alt in info.get("alts", []):
                _reverse_lookup.setdefault(alt.lower(), []).append((sport, key, "alt"))

_build_reverse()


# ═══════════════════════════════════════════════════════════════
# TITLE PARSING — extract team names from Polymarket titles
# ═══════════════════════════════════════════════════════════════

# Patterns for extracting teams from Polymarket titles
# "Lightning vs. Maple Leafs"
# "Lightning vs Maple Leafs: O/U 6.5"
# "Lightning vs. Maple Leafs: Spread -1.5"
# "Will the Lightning beat the Maple Leafs?"
_VS_PATTERN = re.compile(
    r"^(?:Will\s+(?:the\s+)?)?(.+?)\s+(?:vs\.?|v\.?|@)\s+(.+?)(?:\s*[:\-]\s*.+)?$",
    re.IGNORECASE
)

# Clean suffixes from team names
_SUFFIX_CLEAN = re.compile(r"\s*(FC|CF|SC|AFC|United|City|Town|Club)\s*$", re.IGNORECASE)


def extract_teams_from_title(title):
    """
    Extract two team names from a Polymarket title.
    Returns (team_a, team_b) or (None, None) if can't parse.
    """
    if not title:
        return None, None

    # Strip common suffixes like "O/U 6.5", "Spread -1.5"
    clean = re.sub(r"\s*:\s*(O/U|Over/Under|Spread|Total)\s*[\d.+-]+.*$", "", title, flags=re.I)
    # Strip "Will X beat Y?" format
    clean = re.sub(r"^Will\s+(?:the\s+)?", "", clean, flags=re.I)
    clean = re.sub(r"\s*\?$", "", clean)
    # Strip "beat" format
    clean = re.sub(r"\s+beat\s+(?:the\s+)?", " vs ", clean, flags=re.I)

    m = _VS_PATTERN.match(clean)
    if m:
        a = m.group(1).strip()
        b = m.group(2).strip()
        # Clean "the" prefix
        a = re.sub(r"^the\s+", "", a, flags=re.I)
        b = re.sub(r"^the\s+", "", b, flags=re.I)
        return a, b

    return None, None


# ═══════════════════════════════════════════════════════════════
# FUZZY MATCHING ENGINE
# ═══════════════════════════════════════════════════════════════

def _fuzzy_ratio(s1, s2):
    """Return 0.0-1.0 similarity between two lowercase strings."""
    return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()


def _match_single_team(name):
    """
    Match a single team name to our alias database.
    Returns (sport, canonical_key, confidence) or (None, None, 0).
    """
    if not name:
        return None, None, 0

    name_lower = name.lower().strip()

    # 1. Exact lookup
    if name_lower in _reverse_lookup:
        entries = _reverse_lookup[name_lower]
        sport, key, mtype = entries[0]
        conf = 1.0 if mtype in ("canonical", "full") else 0.95
        return sport, key, conf

    # 2. Substring check — is name_lower contained in any full name?
    best_sport, best_key, best_conf = None, None, 0
    for sport, teams in ALL_SPORT_DICTS.items():
        for key, info in teams.items():
            full_lower = info["full"].lower()

            # Check if our token is a suffix of the full name
            # "Lightning" in "Tampa Bay Lightning" → good
            if name_lower in full_lower or full_lower.endswith(name_lower):
                conf = 0.90 if full_lower.endswith(name_lower) else 0.85
                if conf > best_conf:
                    best_sport, best_key, best_conf = sport, key, conf

            # Check abbreviation
            if name_lower == info["abbrev"].lower():
                if 0.95 > best_conf:
                    best_sport, best_key, best_conf = sport, key, 0.95

            # Check alternates for substring
            for alt in info.get("alts", []):
                if name_lower == alt.lower() or alt.lower() in name_lower or name_lower in alt.lower():
                    conf = 0.88
                    if conf > best_conf:
                        best_sport, best_key, best_conf = sport, key, conf

    if best_conf >= 0.80:
        return best_sport, best_key, best_conf

    # 3. Fuzzy match — last resort
    for sport, teams in ALL_SPORT_DICTS.items():
        for key, info in teams.items():
            # Try canonical key
            r = _fuzzy_ratio(name_lower, key)
            if r > best_conf:
                best_sport, best_key, best_conf = sport, key, r

            # Try full name
            r = _fuzzy_ratio(name_lower, info["full"].lower())
            if r > best_conf:
                best_sport, best_key, best_conf = sport, key, r

            # Try alts
            for alt in info.get("alts", []):
                r = _fuzzy_ratio(name_lower, alt.lower())
                if r > best_conf:
                    best_sport, best_key, best_conf = sport, key, r

    return best_sport, best_key, best_conf


def match_title(poly_title):
    """
    Match a Polymarket title to sportsbook teams.

    Returns {
        "team_a": {"name": str, "sport": str, "key": str, "confidence": float},
        "team_b": {"name": str, "sport": str, "key": str, "confidence": float},
        "overall_confidence": float,
        "sport": str,
    } or None if can't match.
    """
    team_a_raw, team_b_raw = extract_teams_from_title(poly_title)
    if not team_a_raw or not team_b_raw:
        return None

    sport_a, key_a, conf_a = _match_single_team(team_a_raw)
    sport_b, key_b, conf_b = _match_single_team(team_b_raw)

    if not sport_a or not sport_b:
        return None

    # Boost confidence if both teams are from the same sport
    if sport_a == sport_b:
        conf_a = min(1.0, conf_a + 0.03)
        conf_b = min(1.0, conf_b + 0.03)

    overall = min(conf_a, conf_b)

    return {
        "team_a": {
            "name": team_a_raw,
            "sport": sport_a,
            "key": key_a,
            "full": ALL_SPORT_DICTS.get(sport_a, {}).get(key_a, {}).get("full", team_a_raw),
            "confidence": round(conf_a, 3),
        },
        "team_b": {
            "name": team_b_raw,
            "sport": sport_b,
            "key": key_b,
            "full": ALL_SPORT_DICTS.get(sport_b, {}).get(key_b, {}).get("full", team_b_raw),
            "confidence": round(conf_b, 3),
        },
        "overall_confidence": round(overall, 3),
        "sport": sport_a if sport_a == sport_b else "mixed",
    }


def match_to_sportsbook(poly_title, sb_home, sb_away):
    """
    Match Polymarket title teams to specific sportsbook home/away teams.

    Returns {
        "home_is": "a" or "b",  # which poly team is the sportsbook home team
        "confidence": float,
    } or None.
    """
    parsed = match_title(poly_title)
    if not parsed:
        return None

    a_full = parsed["team_a"]["full"].lower()
    b_full = parsed["team_b"]["full"].lower()
    sb_home_l = sb_home.lower()
    sb_away_l = sb_away.lower()

    # Score each mapping
    score_a_home = max(_fuzzy_ratio(a_full, sb_home_l), _fuzzy_ratio(parsed["team_a"]["name"].lower(), sb_home_l))
    score_b_home = max(_fuzzy_ratio(b_full, sb_home_l), _fuzzy_ratio(parsed["team_b"]["name"].lower(), sb_home_l))

    if score_a_home > score_b_home:
        return {"home_is": "a", "confidence": round(score_a_home, 3)}
    else:
        return {"home_is": "b", "confidence": round(score_b_home, 3)}


# ═══════════════════════════════════════════════════════════════
# TEST / CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    test_titles = [
        "Lightning vs. Maple Leafs",
        "Lightning vs. Maple Leafs: O/U 6.5",
        "Celtics vs. Lakers",
        "Yankees vs Red Sox",
        "Liverpool vs Manchester City",
        "Real Madrid vs Barcelona",
        "Querétaro FC vs. CF Monterrey",
        "Bayern Munich vs Borussia Dortmund",
        "North Carolina Tar Heels vs. Duke Blue Devils",
        "Packers vs. Bears",
        "PSG vs Marseille",
        "Tigres vs Club America",
        "Inter Milan vs AC Milan",
        "TB Lightning vs TOR Maple Leafs",
    ]

    print("=" * 70)
    print("TEAM NAME MATCHER — TEST RESULTS")
    print("=" * 70)
    for title in test_titles:
        result = match_title(title)
        if result:
            a = result["team_a"]
            b = result["team_b"]
            print(f"\n  {title}")
            print(f"    Team A: {a['name']} → {a['full']} ({a['sport']}) conf={a['confidence']:.2f}")
            print(f"    Team B: {b['name']} → {b['full']} ({b['sport']}) conf={b['confidence']:.2f}")
            print(f"    Overall: {result['overall_confidence']:.2f} | Sport: {result['sport']}")
        else:
            print(f"\n  {title}")
            print(f"    ❌ NO MATCH")
    print()
