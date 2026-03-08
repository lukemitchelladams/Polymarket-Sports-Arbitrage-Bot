#!/usr/bin/env python3
"""
PolyArb v10 — PURE ARB SPORTS TRADING ENGINE
═══════════════════════════════════════════════════════════════
Based on RN1's actual strategy ($5.2M lifetime):
  - Buys BOTH YES and NO when combined < $0.99 → guaranteed profit
  - Eats through entire order book on profitable arbs (50-100+ trades/market)
  - Keeps rescanning and building arb positions every cycle
  - If no arb exists, does NOTHING — no speculative one-sided bets

ARCHITECTURE:
  ARB ENGINE (runs every 50ms = 20x/second):
    Phase 1: BUILD existing arbs — re-buy more matched pairs
    Phase 2: SCAN all markets — find new arbs, enter both sides
    
  CROSS-MARKET SCANNER (runs every cycle):
    Triads: Win A + Win B + Draw (soccer) < $1.97 = guaranteed $2 payout
    Duos: Win A + Win B (basketball) < $0.97 = guaranteed $1 payout

  NO SPECULATIVE BETS. NO ONE-SIDED POSITIONS. ARBS ONLY.
  If no arb exists → bot sits idle (correct behavior).
  Every action verified through ROI gate before execution.

Usage:
  python3.11 arb_v10.py --bankroll 1000 --fresh                # dry run
  python3.11 arb_v10.py --bankroll 1000 --fresh --live          # real money
  python3.11 arb_v10.py --bankroll 1000 --fresh --scan          # scan only
"""

import os, sys, json, time, re, signal, logging, math, hashlib, threading, random
from datetime import datetime, timezone
from collections import defaultdict, deque
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    from game_scores import GameScoreFetcher
    _game_score_fetcher = GameScoreFetcher()
    _game_scores_available = True
except ImportError:
    _game_score_fetcher = None
    _game_scores_available = False
# Odds engine loaded after keys.env (deferred init)
_odds_engine = None
_odds_available = False
import requests as _req

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════
KEYS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "keys.env")
if os.path.exists(KEYS_FILE):
    with open(KEYS_FILE) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

# Initialize odds engine AFTER keys.env is loaded
try:
    from odds_engine import OddsEngine
    _odds_engine = OddsEngine(api_key=os.getenv("THE_ODDS_API_KEY", ""))
    _odds_available = True
except Exception:
    _odds_engine = None
    _odds_available = False

PRIVATE_KEY = os.getenv("PK", "")
PROXY_WALLET = os.getenv("PROXY_WALLET", "")
CLOB        = "https://clob.polymarket.com"
GAMMA       = "https://gamma-api.polymarket.com"

# ─── Bankroll ───
BANKROLL        = float(os.getenv("BANKROLL", "1000"))
DRY_RUN         = True
MAX_DEPLOY_PCT  = 0.92       # max 92% deployed at once
MAX_PER_EVENT   = 0.08       # max 8% bankroll per event/match group
MAX_PER_MARKET  = 0.05       # max 5% bankroll per individual market

# ─── ARB-SPECIFIC LIMITS ───
# SAFETY: Keep these LOW while testing. Increase only after confirming fills are tracked correctly.
ARB_MAX_PER_MARKET = 0.02    # 2% bankroll per arb market ($20 @ $1000 bankroll)
ARB_MAX_PER_EVENT  = 0.05    # 5% bankroll per arb event ($50 @ $1000 bankroll)
ARB_HAMMER_PCT     = 0.03    # max 3% of bankroll per cycle
ARB_INITIAL_MAX    = 10.00   # max $10 per side on FIRST entry — small while testing
                             # Phase 1 Path B (already-locked positions) uses full arb_room to eat the book

# ─── Market Filtering ───
# SPORTS ONLY — non-sports markets (crypto, politics, etc.) filtered out
# Sports have known resolution times + live-event volatility for accumulation
MAX_DAYS_OUT = 30  # 30 days max — anything resolving within a month is fair game

# ─── Arb Lock (Pillar 5: LOCK GUARANTEED PROFIT) ───
# BINARY ARB: same market, NO_avg + YES_ask < threshold → lock it
ARB_LOCK_THRESHOLD = 0.97    # combined cost must be under this (3% spread before slippage)
ARB_LOCK_MIN_SPREAD = 0.03   # minimum 3% guaranteed profit at detection time
POLYMARKET_FEE_PCT = 0.00   # Fee is on PROFIT not payout — threshold accounts for fees

# ─── SLIPPAGE BUFFER (fixes fill problem: book moves 10-20ms between detection & order) ───
# RN1 paid 0.640 for YES while we placed at 0.620 (book price) and got 0 fills.
# Adding buffer ensures our FOK order is AT or ABOVE the ask → instant fill.
# With 0.95 threshold + 1.5¢ slippage per side = fills land at ~0.97-0.98 worst case.
# Real margin of 2-5¢ per pair, PLUS hammer/trim upside on top.
# Polymarket CLOB fills at maker's price (ask), not limit, so slippage is ceiling not cost.
ARB_SLIPPAGE = 0.015  # add 1.5¢ above detected ask — ensures fills while threshold provides margin

# ─── FRESH VALIDATION & DEPTH (Pillar 5b: verify before execution) ───
ARB_FRESH_CACHE_SECS = 0.5    # max age of fresh validation cache (500ms)
ARB_MIN_DEPTH_SHARES = 10     # min shares at profitable prices to proceed with arb
ARB_DYNAMIC_SLIP_MIN = 0.005  # min slippage buffer above fill level (0.5¢)
ARB_DYNAMIC_SLIP_MAX = 0.02   # max slippage buffer (2¢)

# ─── LIVE EVENT TRACKER (RN1's core strategy — reverse-engineered) ───
# RN1 doesn't find "instant arbs" — he buys BOTH sides of live binary markets
# during their respective dips. In live sports, volatility guarantees both sides dip.
# Key insight: buy YES when it dips + buy NO when it dips → combined avg < $1.00 = arb.
# The 0.40-0.60 price range = closest games = most lead changes = biggest dips.
LIVE_ENABLED = True           # ON by default — this is the primary strategy
LIVE_TARGET = 0.92            # buy if potential_combined < this (8% arb target)
LIVE_COMPLETE_THRESHOLD = 0.95   # complete existing arb if combined still profitable (matches ARB_LOCK)
LIVE_WARMUP_SECS = 30         # watch market for 30s before scoring (faster startup after restart)
LIVE_MAX_PENDING = 3           # max single-sided positions waiting for 2nd side (keep tight while bankroll is low)
LIVE_MAX_PENDING_OVERRIDE = 150 # score >= this ignores LIVE_MAX_PENDING (extremely rare override)
LIVE_MIN_SCORE_ENTRY = 200     # minimum score threshold for new entries
LIVE_SCORE_SUSTAIN_CYCLES = 3  # must hold >= MIN_SCORE for this many consecutive cycles before buying
LIVE_BUY_SIZE = 10.00         # $ per accumulation buy (max $10 per entry)
LIVE_MIN_BUY = 4.00           # minimum $ per buy
LIVE_MAX_BUY = 10.00          # maximum $ per entry
LIVE_MAX_PER_MARKET = 10.00   # max $ per market in accumulation (TESTING: keep low)
LIVE_MAX_ACCUM_TOTAL = 100.00 # max TOTAL $ in all accumulation positions combined
LIVE_MAX_ONESIDED_TOTAL = 120.00 # max TOTAL $ in ALL one-sided positions (bot+wallet) — risk cap
                                  # raised: hammer strategy offsets risk on winners
LIVE_DIP_COOLDOWN_SECS = 300  # 5 min cooldown per market between DIP buys (stop hammering)
LIVE_MIN_PRICE = 0.02          # never buy below 2¢ (completions can go this low)
LIVE_MAX_PRICE = 0.96          # never buy above 96¢
LIVE_MIN_ENTRY_PRICE = 0.35    # INITIAL entry min — don't buy cheap underdog sides (<35¢)

# ─── DUAL TRAILING STOP (momentum-timed entries) ───
# Inspired by the Rust trailing bot but applied as a TIMING signal, not a standalone strategy.
# Instead of buying blindly when the score is high, we wait for:
#   1. Side A bounces UP from its session low (price >= low + TRAILING_DELTA) → momentum shift detected
#   2. Side B (now dropping as Side A rises) eventually STOPS dropping — bounces from ITS low
#   3. When Side B bounces, the dip is OVER → buy both sides now (combined is at its cheapest)
# This catches the BOTTOM of the combined price curve, not the middle of a one-way move.
TRAILING_DELTA = 0.03          # 3¢ bounce from session low triggers "bounce detected"
TRAILING_BOUNCE_EXPIRY = 120   # bounce signal expires after 2 min (stale = ignore)
TRAILING_REQUIRED = True       # require dual trailing confirmation for new entries
                                # Completions bypass this — only applies to first buy

# ─── TIME-ESCALATING HEDGE URGENCY ───
# When we have a one-sided position, urgency increases over time.
# Instead of the blunt "hold 8 min then stale-exit if losing", we progressively
# relax entry criteria to find the second side FASTER.
# Inspired by the Rust bot's multi-stage hedging: 2-min → 4-min → 10-min windows.
HEDGE_URGENCY_STAGE1_SECS = 180   # 3 min: relax trailing gate (FIRST_BOUNCE enough)
HEDGE_URGENCY_STAGE2_SECS = 300   # 5 min: lower DIP threshold by 2¢ (wider net)
HEDGE_URGENCY_STAGE3_SECS = 420   # 7 min: skip trailing gate entirely + widen DIP by 4¢
# ─── EXTERNAL ODDS INTELLIGENCE ───
# Bonus points for sportsbook edge, sharp line movement, Elo confirmation.
ODDS_EDGE_MIN_CENTS = 0.05    # ignore edges smaller than 5¢
ODDS_EDGE_MAX_BONUS = 40      # max bonus from sportsbook mispricing
ODDS_MOVEMENT_THRESHOLD = 0.02 # 2% line shift = sharp money signal
ODDS_MOVEMENT_MAX_BONUS = 20  # max bonus from line movement
ODDS_ELO_BONUS = 10           # bonus if Elo model confirms PM price
ODDS_NO_DATA_PENALTY = 5      # penalty when no external data available

LIVE_COMPETITIVE_LO = 0.30     # prefer games where prices are in this range...
LIVE_COMPETITIVE_HI = 0.70     # ...to this (closest, most volatile games)
LIVE_MIN_VOLATILITY = 0.005    # minimum price movement to consider market "live"
LIVE_DIP_THRESHOLD = 0.03      # buy when price is within 3¢ of session low
# ─── STALE POSITION EXIT — cut losers, free slots ───
# You bought one side during a live game, waiting for the other side to dip.
# After 30min with no 2nd-side opportunity:
#   bid < 0.60 → you're on the losing side, game is getting away. Sell now, free slot.
#   bid >= 0.60 → you're on the winning side. Hold — resolves to $1.00 and you profit.
# Markets resolve to exactly $0 or $1. No in-between after game ends.
LIVE_SLOT_FREE_SECS = 300        # 5 min — position stops counting against slot cap (still tries to complete)
LIVE_STALE_TIMEOUT_SECS = 480    # 8 minutes with no 2nd-side match → evaluate exit (was 10, tighter now)
LIVE_STALE_SELL_BELOW = 0.55     # sell if bid < 55¢ (losing side, cut losses)
LIVE_STALE_STILL_ACTIVE = 0.03   # if price moved > 3% in last 5min, game still volatile → hold
# STOP-LOSS: DISABLED — live sports swing too wildly, was panic-selling mid-game dips
# Dead detection + 10-min stale exit handle the real cases
LIVE_DEAD_THRESHOLD = 0.02       # position's held side trading below this = game decided, mark dead
# ─── SMART COMPLETION — momentum-aware + "let it ride" ───
# Don't catch a falling knife: if the completion side is still crashing, WAIT.
# Let it ride: if completion side is below this, first side is so likely to win
# that outright resolution profit > locking a thin arb.
COMPLETION_STILL_FALLING_VEL = -0.0008  # if 2nd side velocity more negative than this → still falling, wait
COMPLETION_LET_RIDE_PRICE = 0.15        # if 2nd side price < this, don't complete — let winner ride
COMPLETION_LET_RIDE_MIN_AGE = 180       # only let-it-ride after 3 min (avoid skipping early arb chances)
COMPLETION_PATIENCE_MAX_SECS = 420      # 7 min max patience — after this, complete anyway (don't get stuck)
# ─── WINNER HAMMER — double down on positions that are winning big ───
# When a one-sided position's held side pushes past the threshold,
# the game is going our way. Add more $ to ride the winner to resolution ($1.00).
# At 85¢ → pays $1.00 = 17.6% return, and implied probability is 85%+ of winning.
# Only hammer WITH positive momentum (price still rising, not just spiked and reversing).
HAMMER_THRESHOLD = 0.75           # held side bid must be >= this to START qualifying
HAMMER_SUSTAIN_SECS = 240         # must stay above threshold for 4 min before hammering (game must be decided)
HAMMER_SIZE = 15.00               # $ to add per hammer — covers hedge losses + builds profit
HAMMER_COOLDOWN_SECS = 120        # 2 min between hammers (unused now — one shot only)
HAMMER_MAX_PER_POSITION = 15.00   # max total $ hammered into any single position — ONE TIME ONLY
HAMMER_MIN_VEL = 0.0002           # minimum positive velocity to confirm momentum (don't buy dead spikes)
HAMMER_MAX_PRICE = 0.93           # buy up to 93¢ — above this, slippage eats the profit margin

# ─── SKIM — small buys on winning side to capture extra profit before hammer ───
# When one side stays above .70 for 7+ min, add a small $1-2 position on the winning
# side. This is "skimming the fat" — capturing value before the full hammer fires.
# Much smaller than hammer ($1-2 vs $12), but can repeat every few minutes.
SKIM_ENABLED = True
SKIM_THRESHOLD = 0.70             # side must be >= this bid to qualify
SKIM_SUSTAIN_SECS = 420           # must stay above threshold for 7 minutes
SKIM_SIZE = 2.00                  # $ per skim buy
SKIM_COOLDOWN_SECS = 300          # 5 min between skims on same position
SKIM_MAX_PER_POSITION = 8.00     # max total $ skimmed per position (4 skims max)
SKIM_MAX_PRICE = 0.75             # don't buy at/above 75¢ — that's hammer territory

# ─── LOSS TRIM — sell 50% of shares when a side collapses ───
# When a held side drops below .27 for 5+ min, trim 50% of shares to cut losses.
# This prevents a total wipeout on positions that are clearly losing.
TRIM_ENABLED = True
TRIM_THRESHOLD = 0.30             # side must be below this bid to trigger trim
TRIM_SUSTAIN_SECS = 240           # must stay below threshold for 4 min
TRIM_PCT = 0.75                   # sell this fraction of shares
TRIM_COOLDOWN_SECS = 600          # 10 min between trims on same position (don't panic-sell)

# ─── HEDGE RECOVERY — buy cheap matched pairs to dilute hedged losses ───
# When a hedged position (both sides held, GP < 0) has a market combined price
# lower than our average combined, buy more pairs to dilute the loss.
# If enough cheap pairs are added, position auto-transitions to "locked" (GP > 0).
HEDGE_RECOVERY_MAX_SPEND = 15.00   # max $ to spend per hedged position per cycle
HEDGE_RECOVERY_MIN_IMPROVEMENT = 0.01  # combined must be at least 1¢ cheaper than avg

# Sport-type priority scores (higher = more desirable for live accumulation)
# Thin liquidity = bigger dips = better for accumulation. O/U, spread, esports all thin.
LIVE_SPORT_SCORES = {"match": 10, "ou": 12, "win": 10, "draw": 8,
                     "spread": 12, "mma": 10, "btts": 10, "half": 11,
                     "golf": 8, "motor": 8, "other": 11, "esports": 11}

# ─── BLOCKED MARKET TYPES — classify() results the bot will NEVER enter ───
# classify() returns "esports" ONLY for match winners (spreads/O-U get caught earlier
# in classify and return "ou"/"spread" instead). So blocking "esports" blocks match
# winners while keeping esports spreads and totals.
BLOCKED_MARKET_TYPES = {
    "esports",   # esports match winners — too volatile, no volume
}

# Legacy aliases for old accumulation code (completion logic reused)
ACCUM_ENABLED = LIVE_ENABLED
ACCUM_COMPLETE_THRESHOLD = LIVE_COMPLETE_THRESHOLD
ACCUM_MAX_OPEN = LIVE_MAX_PENDING
ACCUM_WARMUP_SECS = LIVE_WARMUP_SECS
LIVE_MAX_POSITIONS = LIVE_MAX_PENDING  # backward compat

# ─── TRIAD ARB (LIVE): 3 outcomes of SAME game (Win A NO + Win B NO + Draw NO)
# Exactly ONE outcome wins → 2 NOs pay $1 each = $2 payout. Buy all 3 NO tokens.
# If combined NO cost < $2 → guaranteed profit. Soccer moneylines are perfect for this.
# RN1's edge: lock the triad at $1.97, then when goals shift momentum, accumulate
# more shares of the cheap legs to build the profit margin.
TRIAD_NO_THRESHOLD = 1.96    # combined NO cost must be < this ($2 payout → 2%+ profit)
TRIAD_ENTRY_THRESHOLD = 1.98 # start accumulating legs when combined < this
TRIAD_MIN_SCORE = 100        # minimum score to start a triad (additive with regular scoring)

# ─── Rate Limits ───
MAX_ORDERS_MIN  = 300        # was 60 — Polymarket CLOB handles much more. RN1 does 5-10 orders/sec
MAX_NEW_MKTS_HR = 300        # was 150 — more markets = more arb opportunities

# ═══════════════════════════════════════════════════════════════
# MARKET CLASSIFICATION — STRICT SPORTS GATE
# ═══════════════════════════════════════════════════════════════

def is_sports_market(title, end_date_str=""):
    """
    STRICT sports gate — ONLY sports markets pass through.
    Must match at least one positive sports pattern. No default-accept.

    Covers: soccer, basketball (NBA/NCAAB/intl), tennis, baseball (MLB),
    hockey (NHL), football (NFL/CFB), MMA/UFC/boxing, esports, cricket,
    rugby, golf, F1/NASCAR, volleyball, handball, table tennis, darts, etc.
    """
    if not title: return False
    tl = title.lower()

    # ─── REJECT FUTURES / OUTRIGHTS — these are NOT live match markets ───
    # "Will USA win the 2026 FIFA World Cup" = futures bet, resolves in months
    # "Will X win the championship/title/tournament" = season-long, not a match
    # These match league names but are NOT tradeable live events
    FUTURES_KW = [
        'win the 20', 'win 20',          # "win the 2026 World Cup", "win 2026 championship"
        'champion', 'championship',       # season-long outrights
        'season playoff', 'season play-off', # "win the LCK 2026 season playoffs"
        'win the nba', 'win the nfl', 'win the mlb', 'win the nhl',
        'win the world cup', 'win the cup',
        'win the premier', 'win the serie', 'win the bundesliga', 'win the la liga',
        'win the champions league', 'win the ucl', 'win the europa',
        'win the liga mx', 'win the copa',
        'win the lck', 'win the lcs', 'win the lec', 'win the lpl',  # esports outrights
        'win the vct', 'win the cdl', 'win the owl',
        'win the pga', 'win the masters', 'win the open',
        'mvp', 'rookie of the year', 'defensive player',
        'ballon d\'or', 'golden boot', 'golden glove',
        'win the title', 'win the league', 'win the tournament',
        'be relegated', 'relegation', 'promoted to',
        'make the playoffs', 'make playoffs', 'win the division',
        'reach the final', 'win the final',
        'medal at', 'gold medal', 'total medals',
        'top scorer', 'leading scorer', 'scoring title',
    ]
    if any(kw in tl for kw in FUTURES_KW):
        return False

    # ─── REJECT NON-SPORTS — politics, entertainment, crypto, etc. ───
    # These can match league abbreviations inside words (e.g. "coNFLict")
    REJECT_KW = [
        'conflict', 'ceasefire', 'strike iran', 'invade', 'invasion',
        'nuclear', 'sanctions', 'embassy', 'tariff', 'legislation',
        'best picture', 'academy award', 'oscar', 'emmy', 'grammy',
        'golden globe award', 'box office', 'opening weekend',
        'bitcoin', 'crypto', 'ethereum', 'solana', 'token',
        'election', 'president', 'congress', 'senate', 'governor',
        'democrat', 'republican', 'trump', 'biden',
        'stock', 'market cap', 'interest rate', 'inflation', 'gdp',
        'elon musk', 'openai', 'chatgpt', 'spacex', 'launch',
        'pope', 'vatican', 'arrested', 'convicted', 'prison',
        'album', 'spotify', 'tiktok', 'youtube', 'follower',
        'earthquake', 'hurricane', 'tornado', 'weather',
    ]
    if any(kw in tl for kw in REJECT_KW):
        return False

    # ─── POSITIVE patterns — must match at least one to be sports ───

    # Universal sports patterns
    has_vs = bool(re.search(r'\bvs\.?\b', tl))
    has_win_date = bool(re.match(r'will .+ win on \d{4}-\d{2}-\d{2}', tl))
    has_ou = bool(re.search(r'o/u\s+\d', tl)) or 'over/under' in tl
    has_draw = 'end in a draw' in tl or 'draw?' in tl
    has_btts = 'both teams to score' in tl
    has_spread = bool(re.search(r'spread[:.]?\s*[+(−-]?\s*\d', tl))
    has_game_set = bool(re.search(r'(game \d+ winner|map \d+ winner|set \d+ winner|set \d+ score)', tl))

    # Sport-specific keywords (team names don't help — match on STRUCTURE)
    has_win_lose = bool(re.search(r'\b(win|beat|defeat)\b', tl)) and bool(re.search(r'\d{4}-\d{2}-\d{2}|\btonight\b|\btoday\b|\bgame \d', tl))
    has_total_pts = bool(re.search(r'total (points|goals|runs|sets|games|aces|corners|cards)', tl))
    has_handicap = bool(re.search(r'(handicap|asian handicap|spread|money\s?line|moneyline)', tl))
    has_score = bool(re.search(r'(score (first|over|under)|clean sheet|shutout|hat trick|first (goal|td|basket|blood))', tl))
    has_half_quarter = bool(re.search(r'(first half|second half|1st half|2nd half|first quarter|1st quarter|halftime|half-time)', tl))
    has_innings = bool(re.search(r'(first inning|1st inning|total innings|run line)', tl))
    has_round = bool(re.search(r'(round \d+|go the distance|by (ko|tko|submission|decision|knockout|points|stoppage))', tl))
    has_match_result = bool(re.search(r'(match (winner|result)|to (qualify|advance)|series (winner|lead))', tl))

    # Esports
    has_esports = any(w in tl for w in [
        'counter-strike','dota','valorant','lol:','league of legends',
        'bo3','bo5','bo2','rocket league','overwatch','call of duty',
        'rainbow six','starcraft','mobile legends','pubg','cs2','cs:go',
        'apex legends','free fire','smite','halo','fortnite','mlbb',
        'honor of kings','king pro league','esl ','blast ','iem ','pgl ',
    ])

    # Tennis-specific
    has_tennis = bool(re.search(r'(total (aces|double faults|tiebreaks|breaks)|retire in|straight sets|match to go|'
                                r'tiebreak in|bagel |breadstick |love set)', tl))

    # Golf-specific
    has_golf = bool(re.search(r'(make the cut|miss the cut|hole[- ]in[- ]one|top \d+ finish|tournament winner)', tl))

    # Motorsport
    has_motor = bool(re.search(r'(pole position|fastest lap|podium finish|race winner|dnf |pit stop)', tl))

    # Cricket
    has_cricket = bool(re.search(r'(total (wickets|sixes|fours|run outs)|century in|most (runs|wickets)|toss winner)', tl))

    # League / tournament names in title (strong sports signal)
    # IMPORTANT: use \b word boundaries on short abbreviations to prevent
    # substring matches (e.g. "nfl" inside "conflict", "f1" in movie "F1")
    has_league = bool(re.search(
        # ─── Basketball ───
        r'(\bnba\b|\bncaab\b|\bncaam\b|\bncaaw\b|\bcwbb\b|\bwnba\b|big east|big ten|big 12|big twelve|'
        r'sec tournament|acc tournament|pac-|march madness|final four|sweet sixteen|elite eight|'
        r'euroleague|eurocup|\bfiba\b|basketball cl|'
        # ─── Football (American) ───
        r'\bnfl\b|\bcfb\b|\bxfl\b|\busfl\b|super bowl|college football|'
        # ─── Soccer ───
        r'\bepl\b|premier league|la liga|bundesliga|serie a|ligue 1|'
        r'liga mx|liga portugal|primeira liga|eredivisie|süper lig|ekstraklasa|'
        r'champions league|\bucl\b|europa league|\buel\b|conference league|\buecl\b|'
        r'copa libertadores|copa sudamericana|copa america|copa mx|'
        r'concacaf|conmebol|\bmls\b|\busl\b|\bnwsl\b|'
        r'j[- ]?league|k[- ]?league|a[- ]?league|indian super league|saudi pro|'
        r'scottish premiership|league one|league two|'
        r'copa del rey|dfb[- ]?pokal|coppa italia|coupe de france|carabao|\bfa cup\b|'
        r'club world cup|nations league|'
        # ─── Tennis ───
        r'\batp\b|\bwta\b|\bitf\b|australian open|french open|roland garros|wimbledon|us open tennis|'
        r'indian wells|miami open|monte carlo|madrid open|rome open|cincinnati|'
        r'davis cup|billie jean king|challenger |'
        # ─── Hockey ───
        r'\bnhl\b|\bkhl\b|\bshl\b|\bahl\b|\bechl\b|stanley cup|'
        # ─── Baseball ───
        r'\bmlb\b|\bnpb\b|\bkbo\b|world series|spring training|'
        # ─── MMA / Boxing ───
        r'\bufc\b|bellator|one championship|\bpfl\b|boxing |\bwbc\b|\bwba\b|\bibf\b|\bwbo\b|'
        # ─── Golf ───
        r'\bpga\b|\blpga\b|dp world|masters tournament|the open championship|ryder cup|'
        # ─── Motorsport ───
        r'formula 1|\bf1\b grand|f1 grand|\bnascar\b|\bindycar\b|\bmotogp\b|\bwrc\b|'
        # ─── Cricket ───
        r'\bipl\b|\bbbl\b|\bpsl\b|\bcpl\b|test match|\bodi\b|\bt20\b|the ashes|'
        # ─── Rugby ───
        r'rugby|six nations|super rugby|'
        # ─── Esports leagues ───
        r'\blcs\b|\blec\b|\blck\b|\blpl\b|\bvct\b|\bcdl\b|\bowl\b|\brlcs\b|'
        # ─── Other ───
        r'world cup|euro 20\d\d|olympics|asian games|'
        r'\bncaa\b|darts|snooker|handball)', tl))

    if (has_vs or has_win_date or has_ou or has_draw or has_btts or has_spread
            or has_game_set or has_win_lose or has_total_pts or has_handicap
            or has_score or has_half_quarter or has_innings or has_round
            or has_match_result or has_esports or has_tennis or has_golf
            or has_motor or has_cricket or has_league):
        return True

    # No positive pattern matched → NOT sports
    return False

def classify(title):
    """Classify a sports market into type. Only called AFTER is_sports_market passes."""
    if not title: return "other"
    tl = title.lower()
    if 'end in a draw' in tl or 'draw?' in tl: return "draw"
    if re.search(r'o/u\s+\d', tl) or 'over/under' in tl: return "ou"
    if re.search(r'total (points|goals|runs|sets|games|aces|corners|cards)', tl): return "ou"
    if 'both teams to score' in tl: return "btts"
    if tl.startswith('spread:') or tl.startswith('spread ') or re.search(r'spread[:.]?\s*[+(−-]?\s*\d', tl) or 'handicap' in tl: return "spread"
    if re.search(r'(by (ko|tko|submission|decision|knockout|points|stoppage)|go the distance|round \d)', tl): return "mma"
    if any(w in tl for w in ['counter-strike','dota','valorant','lol:',
        'league of legends','bo3','bo5','bo2','call of duty','rocket league',
        'rainbow six','overwatch','starcraft','mobile legends','pubg',
        'cs2','cs:go','apex legends','free fire','smite','halo','mlbb',
        'honor of kings','king pro league','esl ','blast ','iem ','pgl ']): return "esports"
    if re.match(r'will .+ win on \d{4}-\d{2}-\d{2}', tl, re.I): return "win"
    if re.search(r'\bvs\.?\b', tl): return "match"
    if re.search(r'(game \d+ winner|map \d+ winner|set \d+ winner)', tl): return "match"
    if re.search(r'(first half|second half|1st half|2nd half|first quarter|halftime)', tl): return "half"
    if re.search(r'(make the cut|miss the cut|top \d+ finish|tournament winner)', tl): return "golf"
    if re.search(r'(pole position|fastest lap|race winner|podium)', tl): return "motor"
    return "other"


def detect_sport(title):
    """Detect the SPORT from a market title. Returns: baseball, soccer, basketball,
    hockey, football, tennis, mma, cricket, golf, motor, esports, or 'other'.
    Used for sport-specific completion timing."""
    if not title:
        return "other"
    tl = title.lower()

    # ─── Baseball ───
    if re.search(r'\bmlb\b|\bnpb\b|\bkbo\b|world series|spring training', tl):
        return "baseball"
    # Common MLB team/matchup patterns: "Pirates vs Tigers", "Yankees vs Red Sox"
    _baseball_teams = [
        'yankees','red sox','blue jays','orioles','rays','twins','guardians',
        'white sox','tigers','royals','astros','rangers','mariners','angels',
        'athletics','braves','mets','phillies','marlins','nationals','brewers',
        'cubs','cardinals','reds','pirates','pirates','dodgers','padres',
        'giants','diamondbacks','rockies','d-backs',
    ]
    if any(t in tl for t in _baseball_teams):
        return "baseball"

    # ─── Soccer ───
    if re.search(
        r'\bepl\b|premier league|la liga|bundesliga|serie a|ligue 1|'
        r'liga mx|liga portugal|primeira liga|eredivisie|süper lig|ekstraklasa|'
        r'champions league|\bucl\b|europa league|\buel\b|conference league|\buecl\b|'
        r'copa libertadores|copa sudamericana|copa america|copa mx|'
        r'concacaf|conmebol|\bmls\b|\busl\b|\bnwsl\b|'
        r'j[- ]?league|k[- ]?league|a[- ]?league|indian super league|saudi pro|'
        r'scottish premiership|copa del rey|dfb[- ]?pokal|coppa italia|coupe de france|'
        r'carabao|\bfa cup\b|club world cup|nations league', tl):
        return "soccer"
    # Goal-based keywords
    if 'both teams to score' in tl or re.search(r'total goals|clean sheet', tl):
        return "soccer"

    # ─── Basketball ───
    if re.search(
        r'\bnba\b|\bncaab\b|\bncaam\b|\bncaaw\b|\bcwbb\b|\bwnba\b|'
        r'big east|big ten|big 12|big twelve|sec tournament|acc tournament|'
        r'march madness|final four|sweet sixteen|elite eight|'
        r'euroleague|eurocup|\bfiba\b|basketball cl', tl):
        return "basketball"

    # ─── Hockey ───
    if re.search(r'\bnhl\b|\bkhl\b|\bshl\b|\bahl\b|\bechl\b|stanley cup', tl):
        return "hockey"

    # ─── Football (American) ───
    if re.search(r'\bnfl\b|\bcfb\b|\bxfl\b|\busfl\b|super bowl|college football', tl):
        return "football"

    # ─── Tennis ───
    if re.search(
        r'\batp\b|\bwta\b|\bitf\b|australian open|french open|roland garros|'
        r'wimbledon|us open tennis|indian wells|miami open|monte carlo|'
        r'madrid open|rome open|cincinnati|davis cup|billie jean king|challenger ', tl):
        return "tennis"

    # ─── MMA / Boxing ───
    if re.search(
        r'\bufc\b|bellator|one championship|\bpfl\b|boxing |\bwbc\b|\bwba\b|\bibf\b|\bwbo\b|'
        r'by (ko|tko|submission|decision)|go the distance', tl):
        return "mma"

    # ─── Cricket ───
    if re.search(r'\bipl\b|\bbbl\b|\bpsl\b|\bcpl\b|test match|\bodi\b|\bt20\b|the ashes', tl):
        return "cricket"

    # ─── Golf ───
    if re.search(r'\bpga\b|\blpga\b|dp world|masters tournament|the open championship|ryder cup', tl):
        return "golf"

    # ─── Motorsport ───
    if re.search(r'formula 1|\bf1\b grand|f1 grand|\bnascar\b|\bindycar\b|\bmotogp\b|\bwrc\b', tl):
        return "motor"

    # ─── Esports ───
    if re.search(
        r'counter-strike|dota|valorant|lol:|league of legends|bo3|bo5|'
        r'call of duty|rocket league|overwatch|starcraft|cs2|cs:go|'
        r'\blcs\b|\blec\b|\blck\b|\blpl\b|\bvct\b|\bcdl\b|\bowl\b|\brlcs\b', tl):
        return "esports"

    return "other"


# ─── SPORT-SPECIFIC COMPLETION TIMING ───
# Each sport has different volatility patterns:
#   Baseball: Innings cause swings every ~15 min. Each half-inning = new batter
#     lineup, hits/walks move odds 3-8¢. Be patient — wait for the other half-inning.
#   Soccer: Low-scoring (0-3 goals typical). Single goal = 15-30¢ swing.
#     Long stretches of no scoring = stable prices. Act fast when dips happen.
#   Basketball: Constant scoring, small swings (1-3¢ per possession),
#     but runs/momentum shifts of 5-10¢ happen every few minutes. Medium patience.
#   Hockey: Similar to soccer — low scoring, big swings on goals. Be patient.
#   Football: Drive-based — each drive takes 3-5 min, scoring = big swings.
#   Tennis: Set/game structure. Breaks of serve = big swings. Match-point moments.
#
# Format: (tight_secs, tight_thresh, medium_secs, medium_thresh, max_loss_cap)
#   tight_secs / tight_thresh: initial window where we only complete at ideal profit
#   medium_secs / medium_thresh: middle window, accept smaller profit
#   after medium_secs: escalate to accept losses up to max_loss_cap
SPORT_COMPLETION_TIMING = {
    # Baseball: very patient — half-innings last ~15 min, prices swing on hits/walks/outs
    # Wait for the other team's at-bat, odds often revert
    "baseball":   (600, 0.95, 900, 0.98, 5.00),   # 10 min tight, 15 min medium, then escalate

    # Soccer: goals are rare but massive. Between goals, prices are stable.
    # When a dip happens, grab it fast before it bounces back
    "soccer":     (180, 0.95, 300, 0.97, 5.00),    # 3 min tight, 5 min medium — faster than default

    # Basketball: constant small swings. Runs happen every 2-4 min.
    # Medium patience — don't chase every possession, wait for a run
    "basketball": (240, 0.95, 420, 0.98, 5.00),    # 4 min tight, 7 min medium

    # Hockey: low scoring like soccer, periods are 20 min
    # Goals cause 10-20¢ swings, be patient for reversals
    "hockey":     (360, 0.95, 600, 0.98, 5.00),    # 6 min tight, 10 min medium

    # Football: drives take 3-5 min each, scoring drives = big swings
    "football":   (300, 0.95, 480, 0.98, 5.00),    # 5 min tight, 8 min medium

    # Tennis: game-by-game swings, breaks of serve are big
    "tennis":     (240, 0.95, 420, 0.98, 5.00),    # 4 min tight, 7 min medium

    # MMA: rounds are 5 min, fights can end suddenly
    "mma":        (180, 0.95, 300, 0.98, 5.00),    # 3 min tight, 5 min medium — fights are short

    # Default for everything else (cricket, golf, motor, esports, other)
    "other":      (300, 0.95, 420, 0.98, 5.00),    # 5 min tight, 7 min medium (original defaults)
}


os.makedirs("logs", exist_ok=True)
_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = f"logs/v10_{_ts}.log"
log = logging.getLogger("v10")
log.setLevel(logging.DEBUG)
_fh = logging.FileHandler(LOG_FILE); _fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(_fh)
_sh = logging.StreamHandler(); _sh.setLevel(logging.INFO)
_sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S"))
log.addHandler(_sh)

# ═══════════════════════════════════════════════════════════════
# API LAYER
# ═══════════════════════════════════════════════════════════════
_sess_cache = {}

_http_session = None

def get_http_session():
    """Get or create HTTP session with connection pooling."""
    global _http_session
    if _http_session is None:
        _http_session = _req.Session()
        _http_session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        })
        # Connection pooling — large pool for parallel book fetches
        adapter = _req.adapters.HTTPAdapter(
            pool_connections=100,
            pool_maxsize=100,
            max_retries=0  # we handle retries ourselves
        )
        _http_session.mount("https://", adapter)
    return _http_session

def api_get(url, timeout=15, retries=2):
    """Fetch JSON from API with connection pooling and exponential backoff."""
    sess = get_http_session()
    for attempt in range(retries + 1):
        try:
            resp = sess.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except _req.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                time.sleep(2 ** attempt * 2)
                continue
            if e.response and e.response.status_code >= 500:
                time.sleep(1)
                continue
            return None
        except Exception:
            time.sleep(0.5)
            continue
    return None

# Book caching for performance
_book_cache = {}  # {token_id: (asks, bids, best_ask, ask_depth, best_bid, bid_depth, timestamp)}
BOOK_CACHE_SECS = 2  # cache full books for 2 seconds

def fetch_full_book(token_id):
    """Get FULL order book — all price levels, not just best."""
    book = api_get(f"{CLOB}/book?token_id={token_id}", timeout=8)
    if not book: return [], [], None, 0, None, 0
    asks = []
    for a in book.get("asks", []):
        try:
            p, s = float(a["price"]), float(a["size"])
            if p > 0 and s > 0:
                asks.append((p, s))
        except (KeyError, ValueError, TypeError): continue
    asks.sort(key=lambda x: x[0])  # cheapest first

    bids = []
    for b in book.get("bids", []):
        try:
            p, s = float(b["price"]), float(b["size"])
            if p > 0 and s > 0:
                bids.append((p, s))
        except (KeyError, ValueError, TypeError): continue
    bids.sort(key=lambda x: -x[0])  # most expensive first
    
    best_ask = asks[0][0] if asks else None
    ask_depth = sum(s * p for p, s in asks)
    best_bid = bids[0][0] if bids else None
    bid_depth = sum(s * p for p, s in bids)
    
    return asks, bids, best_ask, round(ask_depth, 2), best_bid, round(bid_depth, 2)

def get_cached_full_book(token_id):
    """Get cached full book or fetch fresh."""
    entry = _book_cache.get(token_id)
    if entry and (time.time() - entry[6]) < BOOK_CACHE_SECS:
        return entry[:6]
    result = fetch_full_book(token_id)
    _book_cache[token_id] = result + (time.time(),)
    return result

def fetch_book(token_id):
    """Get best ask price and total depth for a token.
    Tries WebSocket cache first (fast), falls back to HTTP."""
    ws = ws_get(token_id)
    if ws:
        return ws  # (ask, ask_depth, bid, bid_depth)
    asks, bids, best_ask, ask_depth, best_bid, bid_depth = fetch_full_book(token_id)
    return best_ask, ask_depth, best_bid, bid_depth

def batch_fetch_books(token_ids, max_workers=20):
    """Fetch order books for multiple tokens in PARALLEL using ThreadPoolExecutor.
    Returns dict: {token_id: (ask, ask_depth, bid, bid_depth)}"""
    cache = {}
    unique = list(set(t for t in token_ids if t))
    if not unique: return cache
    
    with ThreadPoolExecutor(max_workers=min(max_workers, len(unique))) as pool:
        futures = {pool.submit(fetch_book, tid): tid for tid in unique}
        for f in as_completed(futures):
            tid = futures[f]
            try:
                cache[tid] = f.result()
            except Exception as e:
                log.debug(f"batch_fetch_books error for {tid[:12]}...: {e}")
                cache[tid] = (None, 0, None, 0)
    return cache

def batch_fetch_full_books(token_ids, max_workers=20):
    """Fetch FULL order books in parallel. Returns dict: {token_id: (asks, bids, best_ask, ask_depth, best_bid, bid_depth)}"""
    cache = {}
    unique = list(set(t for t in token_ids if t))
    if not unique: return cache
    try:
        with ThreadPoolExecutor(max_workers=min(max_workers, len(unique))) as pool:
            futures = {pool.submit(fetch_full_book, tid): tid for tid in unique}
            for f in as_completed(futures, timeout=15):  # 15s max — don't hang forever
                tid = futures[f]
                try:
                    cache[tid] = f.result(timeout=10)
                except Exception as e:
                    log.debug(f"batch_fetch_full_books error for {tid[:12]}...: {e}")
                    cache[tid] = ([], [], None, 0, None, 0)
    except TimeoutError:
        log.warning(f"  ⚠️ batch_fetch_full_books TIMEOUT: got {len(cache)}/{len(unique)} books")
    return cache



def walk_book_arb(pos, no_asks, yes_asks, max_budget):
    """
    Walk through order book levels, buying matched NO+YES pairs.
    Like RN1: eat every level where combined < threshold.

    SLIPPAGE: Order prices include ARB_SLIPPAGE buffer above detected ask
    to ensure fills even when the book moves during order placement.
    Profitability is checked at REAL book prices; orders placed at price + slippage.

    Returns list of (side, order_price, size_usd, shares) orders to execute.
    """
    orders = []
    no_idx, yes_idx = 0, 0
    budget_left = max_budget
    total_no_shares = 0
    total_yes_shares = 0
    total_no_cost = 0
    total_yes_cost = 0
    no_avail = [(p, s) for p, s in no_asks]  # remaining size at each level
    yes_avail = [(p, s) for p, s in yes_asks]

    while no_idx < len(no_avail) and yes_idx < len(yes_avail) and budget_left > 2:
        no_price, no_size = no_avail[no_idx]
        yes_price, yes_size = yes_avail[yes_idx]
        combined = no_price + yes_price  # check profitability at REAL book prices

        if combined > ARB_LOCK_THRESHOLD:
            # Try advancing the cheaper side to find better prices
            if no_idx + 1 < len(no_avail) and no_avail[no_idx + 1][0] + yes_price < combined:
                no_idx += 1
                continue
            if yes_idx + 1 < len(yes_avail) and no_price + yes_avail[yes_idx + 1][0] < combined:
                yes_idx += 1
                continue
            break  # No more profitable combinations

        # ORDER prices include slippage buffer for guaranteed fills
        # CRITICAL: cap so combined order price never exceeds $0.99
        no_order_price = round(no_price + ARB_SLIPPAGE, 2)
        yes_order_price = round(yes_price + ARB_SLIPPAGE, 2)
        if no_order_price + yes_order_price > 0.99:
            # Scale slippage down proportionally
            avail = max(0, 0.99 - no_price - yes_price)
            no_order_price = round(no_price + avail * 0.5, 2)
            yes_order_price = round(yes_price + avail * 0.5, 2)
        no_order_price = min(no_order_price, 0.99)
        yes_order_price = min(yes_order_price, 0.99)

        # Budget uses REAL book prices (GTC orders fill at best available, not limit)
        pair_real_cost = no_price + yes_price
        max_pairs_budget = budget_left / pair_real_cost if pair_real_cost > 0 else 0
        max_pairs_no = no_size
        max_pairs_yes = yes_size
        pairs = min(max_pairs_budget, max_pairs_no, max_pairs_yes)
        pairs = math.floor(pairs * 100) / 100  # truncate

        if pairs < 5:  # Polymarket minimum
            if no_size <= yes_size:
                no_idx += 1
            else:
                yes_idx += 1
            continue

        # Cost at REAL book prices (what we'll actually pay)
        no_cost = round(no_price * pairs, 2)
        yes_cost = round(yes_price * pairs, 2)

        # Orders use SLIPPAGE prices as limit (ensures fill), but cost reflects real prices
        orders.append(('NO', no_order_price, no_cost, pairs))
        orders.append(('YES', yes_order_price, yes_cost, pairs))

        total_no_shares += pairs
        total_yes_shares += pairs
        total_no_cost += no_cost
        total_yes_cost += yes_cost
        budget_left -= (no_cost + yes_cost)

        # Reduce available size at this level
        no_avail[no_idx] = (no_price, no_size - pairs)
        yes_avail[yes_idx] = (yes_price, yes_size - pairs)

        if no_idx < len(no_avail) and no_avail[no_idx][1] < 5:
            no_idx += 1
        if yes_idx < len(yes_avail) and yes_avail[yes_idx][1] < 5:
            yes_idx += 1

    # Guaranteed profit at REAL book prices (GTC fills at best available, not limit)
    payout_per_share = 1.0 * (1.0 - POLYMARKET_FEE_PCT)
    guaranteed = min(total_no_shares, total_yes_shares) * payout_per_share - (total_no_cost + total_yes_cost)
    return orders, guaranteed, total_no_cost + total_yes_cost


# ─── FRESH ARB VALIDATION & DEPTH CHECKING ───────────────────────────
_arb_fresh_cache = {}  # {market_id: (timestamp, no_ask, yes_ask, no_book, yes_book)}
_arb_fresh_cache_last_clean = 0  # last cleanup timestamp


def _cleanup_arb_fresh_cache():
    """Evict stale entries to prevent unbounded memory growth."""
    global _arb_fresh_cache, _arb_fresh_cache_last_clean
    now = time.time()
    if now - _arb_fresh_cache_last_clean < 10:  # cleanup at most every 10s
        return
    _arb_fresh_cache_last_clean = now
    cutoff = now - 5.0  # evict anything older than 5 seconds
    _arb_fresh_cache = {k: v for k, v in _arb_fresh_cache.items() if v[0] > cutoff}


def validate_arb_prices_fresh(market, threshold=None):
    """Re-fetch order books RIGHT NOW and validate arb is still there.

    Returns (is_valid, no_ask, yes_ask, no_book, yes_book, reason).
    no_book / yes_book are the full asks lists [(price, size), ...].
    Caches results for ARB_FRESH_CACHE_SECS to avoid thrashing within same cycle.
    """
    _cleanup_arb_fresh_cache()  # prevent unbounded growth
    if threshold is None:
        threshold = ARB_LOCK_THRESHOLD
    mid = market.get("condition_id") or market.get("mid") or market.get("id", "")
    # Support both raw API format (tokens list) and bot internal format (tok_no/tok_yes)
    tok_no = market.get("tok_no")
    tok_yes = market.get("tok_yes")
    if not tok_no or not tok_yes:
        tokens = market.get("tokens", [])
        for t in tokens:
            if t.get("outcome", "").upper() == "NO":
                tok_no = t.get("token_id")
            elif t.get("outcome", "").upper() == "YES":
                tok_yes = t.get("token_id")
    if not tok_no or not tok_yes:
        return (False, 0, 0, [], [], "missing token IDs")

    # Check cache — avoid re-fetching within 500ms
    now = time.time()
    cached = _arb_fresh_cache.get(mid)
    if cached and (now - cached[0]) < ARB_FRESH_CACHE_SECS:
        _, no_ask, yes_ask, no_book, yes_book = cached
        combined = no_ask + yes_ask
        if combined > threshold:
            return (False, no_ask, yes_ask, no_book, yes_book,
                    f"cached combined={combined:.3f} > threshold={threshold}")
        if no_ask < 0.10 or yes_ask < 0.10:
            return (False, no_ask, yes_ask, no_book, yes_book,
                    f"cached side too cheap: NO={no_ask:.2f} YES={yes_ask:.2f}")
        return (True, no_ask, yes_ask, no_book, yes_book, "")

    # Fetch BOTH books in parallel
    books = batch_fetch_full_books([tok_no, tok_yes], max_workers=2)
    no_data = books.get(tok_no)
    yes_data = books.get(tok_yes)

    if not no_data or not yes_data:
        return (False, 0, 0, [], [], "fetch failed for one or both tokens")

    no_asks, _, no_ask, _, _, _ = no_data
    yes_asks, _, yes_ask, _, _, _ = yes_data

    # Cache for this cycle
    _arb_fresh_cache[mid] = (now, no_ask, yes_ask, no_asks, yes_asks)

    # Validate
    if no_ask <= 0 or yes_ask <= 0:
        return (False, no_ask, yes_ask, no_asks, yes_asks,
                f"no liquidity: NO_ask={no_ask} YES_ask={yes_ask}")
    if no_ask < 0.10 or yes_ask < 0.10:
        return (False, no_ask, yes_ask, no_asks, yes_asks,
                f"side too cheap: NO={no_ask:.2f} YES={yes_ask:.2f}")
    combined = no_ask + yes_ask
    if combined > threshold:
        return (False, no_ask, yes_ask, no_asks, yes_asks,
                f"combined={combined:.3f} > threshold={threshold}")
    return (True, no_ask, yes_ask, no_asks, yes_asks, "")


def validate_book_depth(asks_list, target_shares, max_price):
    """Check how many shares are available at profitable price levels.

    Walks asks from cheapest up. Stops when max_price exceeded.
    Returns (available_shares, avg_fill_price).
    """
    if not asks_list:
        return (0.0, 0.0)
    total_shares = 0.0
    total_cost = 0.0
    for price, size in asks_list:
        if price > max_price:
            break
        take = min(size, target_shares - total_shares)
        total_shares += take
        total_cost += take * price
        if total_shares >= target_shares:
            break
    avg_fill = (total_cost / total_shares) if total_shares > 0 else 0.0
    return (total_shares, avg_fill)


def calculate_dynamic_limit(asks_list, target_shares, other_side_cost,
                            threshold=None):
    """Calculate optimal limit price based on book structure.

    Walks the book to find the price level needed to fill target_shares.
    Adds slippage buffer, constrained so limit + other_side_cost <= 0.99.
    Returns (limit_price, fillable_shares).
    """
    if threshold is None:
        threshold = 0.99  # absolute max combined for profitability
    max_limit = threshold - other_side_cost  # max we can pay this side
    if max_limit <= 0:
        return (0.0, 0.0)

    if not asks_list:
        return (0.0, 0.0)

    accumulated = 0.0
    highest_needed = 0.0
    for price, size in asks_list:
        if price > max_limit:
            break
        take = min(size, target_shares - accumulated)
        accumulated += take
        highest_needed = price
        if accumulated >= target_shares:
            break

    if accumulated <= 0:
        return (0.0, 0.0)

    # Add slippage buffer to ensure fill — but cap at max_limit
    slip = max(ARB_DYNAMIC_SLIP_MIN,
               min(ARB_DYNAMIC_SLIP_MAX, highest_needed * 0.02))
    limit_price = min(round(highest_needed + slip, 3), max_limit)
    return (limit_price, accumulated)


# ─── DATA COLLECTION PIPELINE — capture game data for ML training ───
_game_data_dir = None          # path to session data directory
_game_data_file = None         # open file handle for snapshots.jsonl
_game_data_resolution_file = None  # open file handle for resolutions.jsonl
_game_data_last_snap = 0       # timestamp of last snapshot
_game_data_actions = []        # actions collected during current cycle
_game_data_resolved_mids = set()  # market IDs we've already logged as resolved
GAME_DATA_INTERVAL = 30        # seconds between snapshots

def init_game_data():
    """Initialize game data directory for this session."""
    global _game_data_dir, _game_data_file
    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    _game_data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "game_data", f"session_{ts}")
    os.makedirs(_game_data_dir, exist_ok=True)
    # Write session metadata
    meta = {
        "started": time.time(),
        "started_human": ts,
        "bankroll": BANKROLL,
        "arb_threshold": ARB_LOCK_THRESHOLD,
        "hammer_size": HAMMER_SIZE,
        "recovery_max": HEDGE_RECOVERY_MAX_SPEND,
    }
    with open(os.path.join(_game_data_dir, "metadata.json"), "w") as f:
        json.dump(meta, f, indent=2)
    _game_data_file = open(os.path.join(_game_data_dir, "snapshots.jsonl"), "a")
    _game_data_resolution_file = open(os.path.join(_game_data_dir, "resolutions.jsonl"), "a")
    log.info(f"  📊 DATA: Initialized game data → {_game_data_dir}")

# ─── STRUCTURED AUDIT LOG — append-only JSONL for every trading decision ───
# Inspired by Python Sports Bot's journal system. Every signal, risk decision,
# order, fill, and exit is logged as structured JSON for post-session analysis.
# File: game_data/session_XXXXXX/audit.jsonl
_audit_log_file = None

def init_audit_log():
    """Initialize the audit log file in the game data directory."""
    global _audit_log_file
    if _game_data_dir:
        _audit_log_file = open(os.path.join(_game_data_dir, "audit.jsonl"), "a")
        log.info(f"  📝 AUDIT: Initialized audit log → {_game_data_dir}/audit.jsonl")

def audit(event_type, **kwargs):
    """Log a structured audit event.

    Event types:
      ENTRY_SIGNAL    — market scored above threshold, candidate for entry
      ENTRY_GATE      — entry blocked by trailing gate / risk cap / slot limit
      ENTRY_BUY       — entry order placed
      COMPLETION_BUY  — 2nd side bought to lock arb
      DIP_BUY         — additional shares on other side
      HAMMER_BUY      — winner hammer buy
      SKIM_BUY        — small skim buy on winning side
      TRIM_SELL       — trim losing side
      STALE_EXIT      — stale position sold
      ARB_LOCKED      — position completed into full arb
      RESOLVED        — market resolved, position closed
      TRAILING_STATE  — trailing state change (for debugging)
    """
    if not _audit_log_file:
        return
    try:
        entry = {
            "ts": round(time.time(), 3),
            "event": event_type,
            **{k: v for k, v in kwargs.items() if v is not None}
        }
        _audit_log_file.write(json.dumps(entry) + "\n")
        _audit_log_file.flush()
    except Exception:
        pass  # never crash on audit logging

def record_resolution(mid, title, typ, winner_side, no_bid, yes_bid, pos=None):
    """Log a market resolution for threshold analysis.
    Called when we detect game-over (one side at ~0, other at ~1).
    """
    if not _game_data_resolution_file or mid in _game_data_resolved_mids:
        return
    _game_data_resolved_mids.add(mid)
    try:
        entry = {
            "ts": time.time(),
            "mid": mid,
            "title": title[:80],
            "typ": typ,
            "winner": winner_side,  # "YES" or "NO"
            "no_bid_final": round(no_bid, 4),
            "yes_bid_final": round(yes_bid, 4),
        }
        if pos:
            entry.update({
                "no_shares": round(pos.shares or 0, 2),
                "yes_shares": round(pos.yes_shares or 0, 2),
                "avg_no": round(pos.avg_price or 0, 4),
                "avg_yes": round(pos.yes_avg_price or 0, 4),
                "total_cost": round(pos.total_cost or 0, 2),
                "hammered": round(pos.total_hammered or 0, 2),
                "skimmed": round(getattr(pos, 'total_skimmed', 0), 2),
                "trimmed": round(getattr(pos, 'total_trimmed', 0), 2),
                "from_wallet": pos.from_wallet,
            })
        _game_data_resolution_file.write(json.dumps(entry) + "\n")
        _game_data_resolution_file.flush()
        log.info(f"  📊 RESOLUTION LOGGED: {title[:40]} → {winner_side} won")

        # Update Elo ratings from this result
        if _odds_available and _odds_engine:
            try:
                from team_name_mapper import match_title as _mt
                parsed = _mt(title)
                if parsed and parsed["overall_confidence"] >= 0.70 and parsed["sport"] != "mixed":
                    a_key = parsed["team_a"]["key"]
                    b_key = parsed["team_b"]["key"]
                    sport = parsed["sport"]
                    # YES = Team A wins, NO = Team B wins
                    if winner_side == "YES":
                        result = _odds_engine.elo.update(sport, a_key, b_key)
                    else:
                        result = _odds_engine.elo.update(sport, b_key, a_key)
                    if result:
                        w = result["winner"]
                        l = result["loser"]
                        log.info(f"  📊 Elo updated: winner {w['old']:.0f}→{w['new']:.0f} "
                                f"({w['delta']:+.1f}), loser {l['old']:.0f}→{l['new']:.0f} "
                                f"({l['delta']:+.1f})")
            except Exception:
                pass  # Elo update is non-critical
    except Exception as e:
        log.warning(f"  ⚠ DATA: resolution log error: {e}")

def record_game_action(action_type, details):
    """Record an action during the current cycle for the next snapshot."""
    _game_data_actions.append({
        "ts": time.time(),
        "type": action_type,
        **details
    })

def capture_snapshot(cycle_num, cycle_price_cache, portfolio_obj):
    """Capture a timestamped snapshot of all market prices and positions."""
    global _game_data_last_snap, _game_data_actions
    if not _game_data_file:
        return
    now = time.time()
    if now - _game_data_last_snap < GAME_DATA_INTERVAL:
        return  # too soon

    try:
        # Market prices from cache
        markets = {}
        for tok_id, data in cycle_price_cache.items():
            if isinstance(data, tuple) and len(data) >= 4:
                markets[tok_id] = {
                    "ask": data[0], "ask_depth": data[1],
                    "bid": data[2], "bid_depth": data[3]
                }

        # Position states — enriched for threshold analysis
        positions = []
        for mid, pos in portfolio_obj.positions.items():
            gp = pos.guaranteed_profit
            # Get bid/ask from cache for both sides
            no_data = cycle_price_cache.get(pos.tok_no, (None, 0, None, 0))
            yes_data = cycle_price_cache.get(pos.tok_yes, (None, 0, None, 0)) if pos.tok_yes else (None, 0, None, 0)
            no_ask = no_data[0] or 0; no_ask_d = no_data[1] or 0
            no_bid = no_data[2] or 0; no_bid_d = no_data[3] or 0
            yes_ask = yes_data[0] or 0; yes_ask_d = yes_data[1] or 0
            yes_bid = yes_data[2] or 0; yes_bid_d = yes_data[3] or 0
            # Momentum from tracker
            vel_no = 0.0; vel_yes = 0.0
            with _live_lock:
                t = _live_tracker.get(mid)
                if t:
                    try:
                        _, _, vel_no, _ = _get_momentum_score(t, now)
                    except Exception:
                        pass
            pos_data = {
                "mid": mid, "title": pos.title[:60], "typ": getattr(pos, 'typ', ''),
                "tok_no": pos.tok_no, "tok_yes": pos.tok_yes or "",
                # Shares & cost
                "no_shares": round(pos.shares or 0, 2),
                "yes_shares": round(pos.yes_shares or 0, 2),
                "avg_no": round(pos.avg_price or 0, 4),
                "avg_yes": round(pos.yes_avg_price or 0, 4),
                "cost": round(pos.total_cost or 0, 2),
                "gp": round(gp, 2) if gp is not None else 0,
                # Prices — bid/ask/depth for both sides
                "no_bid": round(no_bid, 4), "no_ask": round(no_ask, 4),
                "no_bid_depth": round(no_bid_d, 2), "no_ask_depth": round(no_ask_d, 2),
                "yes_bid": round(yes_bid, 4), "yes_ask": round(yes_ask, 4),
                "yes_bid_depth": round(yes_bid_d, 2), "yes_ask_depth": round(yes_ask_d, 2),
                # Spread
                "no_spread": round(no_ask - no_bid, 4) if no_ask and no_bid else 0,
                "yes_spread": round(yes_ask - yes_bid, 4) if yes_ask and yes_bid else 0,
                # Momentum
                "velocity": round(vel_no, 6),
                # State flags
                "locked": pos.is_arb_locked,
                "hedged": pos.is_two_sided and not pos.is_arb_locked,
                "from_wallet": pos.from_wallet,
                "triad_group": pos.triad_group,
                "hammered": round(pos.total_hammered or 0, 2),
                "skimmed": round(getattr(pos, 'total_skimmed', 0), 2),
                "trimmed": round(getattr(pos, 'total_trimmed', 0), 2),
            }
            positions.append(pos_data)

        # Stats
        locked_gp = sum(p["gp"] for p in positions if p["locked"])
        hedged_loss = sum(p["gp"] for p in positions if p["hedged"])

        # Game scores from ESPN
        game_states = {}
        if _game_scores_available and _game_score_fetcher:
            try:
                game_states = _game_score_fetcher.enrich_snapshot(portfolio_obj.positions)
            except Exception:
                pass

        # Tracked markets — ALL markets the bot is watching (for threshold analysis)
        # Sample every 5th snapshot to keep file size manageable
        tracked_markets = {}
        if cycle_num % 5 == 0:
            with _live_lock:
                for mid, t in _live_tracker.items():
                    mkt = t.get("mkt") or {}
                    y_now = t.get("yes_now") or 0
                    n_now = t.get("no_now") or 0
                    tracked_markets[mid] = {
                        "title": (mkt.get("question") or mkt.get("title", ""))[:60],
                        "typ": mkt.get("typ", ""),
                        "yes_now": round(y_now, 4),
                        "no_now": round(n_now, 4),
                        "yes_depth": round(t.get("yes_depth") or 0, 2),
                        "no_depth": round(t.get("no_depth") or 0, 2),
                        "yes_low": round(t.get("yes_low") or 0, 4) if t.get("yes_low", 999) < 999 else 0,
                        "no_low": round(t.get("no_low") or 0, 4) if t.get("no_low", 999) < 999 else 0,
                        "vol24": mkt.get("vol24", 0),
                        "lead_switches": t.get("lead_switches", 0),
                        "score": t.get("last_score", 0),
                    }
                    # Auto-detect resolution on ANY tracked market
                    if y_now and n_now:
                        if y_now <= 0.05 and n_now >= 0.95:
                            title = (mkt.get("question") or mkt.get("title", ""))[:60]
                            record_resolution(mid, title, mkt.get("typ", ""),
                                             "NO", n_now, y_now)
                        elif n_now <= 0.05 and y_now >= 0.95:
                            title = (mkt.get("question") or mkt.get("title", ""))[:60]
                            record_resolution(mid, title, mkt.get("typ", ""),
                                             "YES", n_now, y_now)

        snapshot = {
            "ts": now,
            "cycle": cycle_num,
            "markets": markets,
            "positions": positions,
            "tracked_markets": tracked_markets,
            "game_states": game_states,
            "actions": _game_data_actions[:],
            "stats": {
                "deployed": round(portfolio_obj.deployed(), 2),
                "locked_gp": round(locked_gp, 2),
                "hedged_loss": round(hedged_loss, 2),
                "n_positions": len(positions),
                "n_tracked": len(_live_tracker),
                "ws_arb_checks": _ws_arb_checks,
                "ws_arb_fires": _ws_arb_fires,
            }
        }
        _game_data_file.write(json.dumps(snapshot) + "\n")
        _game_data_file.flush()
        _game_data_last_snap = now
        _game_data_actions = []
    except Exception as e:
        log.warning(f"  ⚠ DATA: snapshot error: {e}")


# ─── WebSocket Price Stream (optional, falls back to HTTP) ───
_ws_cache = {}       # {token_id: (ask, ask_depth, bid, bid_depth, timestamp)}
_ws_subscribed = set()
_ws_new_tokens = []  # queue of newly discovered tokens to subscribe on the fly
_ws_thread = None
_ws_running = False
WS_STALE_SECS = 2   # consider WS data stale after 2 seconds (was 5 — too slow)

# ─── WS Event-Driven Arb Detection (v4-style push model) ───
# Maps token_id → market dict for instant arb evaluation on WS price events.
# v4 fires evaluate_market() on EVERY WS tick → locks arbs in <50ms.
# v10 was pull-only (50ms timer + batch scan + HTTP book fetch = 2-5s delay).
_ws_token_to_market = {}   # {token_id: market_dict} — built from all_markets
_ws_arb_queue = []         # [(market_dict, no_ask, yes_ask, timestamp)] — arbs detected by WS
_ws_arb_lock = threading.Lock()
_ws_arb_fired = {}         # {mid: timestamp} — cooldown to avoid re-firing same arb
_ws_arb_checks = 0         # total WS price ticks that checked for arbs
_ws_arb_fires = 0          # total WS arbs that passed threshold and were queued

# ─── LIVE EVENT TRACKER — per-market session tracking ───
# Updated on EVERY WS price tick. Tracks session lows, volatility, competitiveness.
# This is how we identify which markets have arb potential and when to buy.
_live_tracker = {}    # {mid: tracker_dict}
_live_lock = threading.Lock()

# ─── BURNED MARKETS — never re-enter after trim or stale exit ───
# When we trim or stale-exit a market, we add its mid here.
# The entry logic checks this set and skips burned markets.
# Persists for the entire bot session (resets on restart, which is fine —
# new session = fresh slate, old games are over).
_burned_markets = set()  # {mid, mid, ...}

# ─── PENDING GTC ORDER TRACKER — prevent double-buying the same token ───
# When a GTC order is placed but doesn't fill immediately, we track it here.
# Before placing another GTC for the same token, check this set.
# Cleared when: order fills (verify_positions catches it), order is cancelled
# (cancel_stale_orders), or after PENDING_GTC_EXPIRY seconds.
_pending_gtc_orders = {}  # {token_id: {"order_id": str, "ts": float, "size_usd": float}}
PENDING_GTC_EXPIRY = 300  # 5 min — matches cancel_stale_orders max age

def _update_tracker(mid, side, price, ask_depth, mkt):
    """Update live tracker for a market side. Called on every WS tick."""
    now = time.time()
    with _live_lock:
        if mid not in _live_tracker:
            _live_tracker[mid] = {
                "yes_low": 999, "no_low": 999,
                "yes_low_ts": 0, "no_low_ts": 0,
                "yes_high": 0, "no_high": 0,       # session highs (for pullback detection)
                "yes_high_ts": 0, "no_high_ts": 0,
                "yes_now": None, "no_now": None,
                "yes_depth": 0, "no_depth": 0,
                "first_seen": now,
                "yes_prices": [], "no_prices": [],  # (timestamp, price) for volatility
                "combined_floor": 999,
                "yes_recent_low": 999, "no_recent_low": 999,  # rolling 5-min lows (decay stale)
                "mkt": mkt,
                "high_score_streak": 0,  # consecutive cycles scoring >= MIN_SCORE_ENTRY
                "last_score": 0,
                "lead_side": None,        # which side is currently > 0.50
                "lead_switches": 0,       # how many times the leading side has flipped
                # ─── DUAL TRAILING STOP STATE ───
                # Tracks bounce-from-low events for each side.
                # A "bounce" = price rises TRAILING_DELTA above session low.
                # Dual trailing gate: first bounce (Side A) = momentum shift,
                # second bounce (Side B) = dip is over → BUY signal.
                "yes_bounce_ts": 0,       # timestamp when YES bounced from its low
                "no_bounce_ts": 0,        # timestamp when NO bounced from its low
                "yes_bounce_price": 0,    # price at which YES bounce was detected
                "no_bounce_price": 0,     # price at which NO bounce was detected
                "trailing_state": "WATCHING",  # WATCHING → FIRST_BOUNCE → READY
                "trailing_first_side": None,   # which side bounced first ("YES" or "NO")
                "trailing_first_ts": 0,        # when the first bounce happened
                "trailing_ready_ts": 0,        # when BOTH sides bounced (entry signal)
            }
        t = _live_tracker[mid]

        if side == "NO":
            t["no_now"] = price
            t["no_depth"] = ask_depth
            if price < t["no_low"]:
                t["no_low"] = price
                t["no_low_ts"] = now
            if price > t["no_high"]:
                t["no_high"] = price
                t["no_high_ts"] = now
            t["no_prices"].append((now, price))
            # Keep last 5 minutes of prices for volatility calc
            cutoff = now - 300
            while t["no_prices"] and t["no_prices"][0][0] < cutoff:
                t["no_prices"].pop(0)
        else:
            t["yes_now"] = price
            t["yes_depth"] = ask_depth
            if price < t["yes_low"]:
                t["yes_low"] = price
                t["yes_low_ts"] = now
            if price > t["yes_high"]:
                t["yes_high"] = price
                t["yes_high_ts"] = now
            t["yes_prices"].append((now, price))
            cutoff = now - 300
            while t["yes_prices"] and t["yes_prices"][0][0] < cutoff:
                t["yes_prices"].pop(0)

        # Update rolling 5-min recent lows from price history
        # These decay naturally as old prices fall off the 5-min window,
        # preventing stale session lows from inflating scores when the
        # game has blown open and prices will never return to those lows.
        if t["no_prices"]:
            t["no_recent_low"] = min(p for _, p in t["no_prices"])
        if t["yes_prices"]:
            t["yes_recent_low"] = min(p for _, p in t["yes_prices"])

        # Update combined floor using RECENT lows (not session-wide lows)
        if t["yes_recent_low"] < 900 and t["no_recent_low"] < 900:
            t["combined_floor"] = t["yes_recent_low"] + t["no_recent_low"]

        # Track lead switches — when the favorite side flips, both sides dip at some point
        y = t.get("yes_now")
        n = t.get("no_now")
        if y and n and y > 0.01 and n > 0.01:
            new_lead = "YES" if y > n else ("NO" if n > y else None)
            if new_lead and t["lead_side"] and new_lead != t["lead_side"]:
                t["lead_switches"] += 1
            if new_lead:
                t["lead_side"] = new_lead

        # ─── DUAL TRAILING STOP: BOUNCE DETECTION ───
        # A "bounce" = current price >= session_low + TRAILING_DELTA.
        # This means the side has stopped falling and is turning around.
        # We track bounces independently for each side, then use the
        # dual sequential pattern in _get_trailing_signal().
        _detect_bounce(t, side, price, now)

def _detect_bounce(t, side, price, now):
    """Detect when a side bounces from its session low.

    A bounce is confirmed when: price >= session_low + TRAILING_DELTA.
    This means the price dropped to session_low at some point, then recovered
    by at least TRAILING_DELTA cents — the downward momentum has STOPPED.

    We also handle bounce EXPIRY: if a bounce happened too long ago and the
    price has since made new lows, the old bounce is stale and gets cleared.
    """
    low_key = f"{side.lower()}_low"
    bounce_ts_key = f"{side.lower()}_bounce_ts"
    bounce_price_key = f"{side.lower()}_bounce_price"
    low_ts_key = f"{side.lower()}_low_ts"

    session_low = t.get(low_key, 999)
    if session_low >= 900:
        return  # no meaningful low yet

    bounce_threshold = session_low + TRAILING_DELTA

    # Check if this side just bounced
    if price >= bounce_threshold and t[bounce_ts_key] == 0:
        # First bounce detected for this side!
        t[bounce_ts_key] = now
        t[bounce_price_key] = price

    # If the low was JUST updated (new low made), clear any prior bounce —
    # the old bounce was a false signal; price went lower.
    if t.get(low_ts_key, 0) == now:
        # New low just set this tick — any previous bounce is invalidated
        if t[bounce_ts_key] > 0:
            t[bounce_ts_key] = 0
            t[bounce_price_key] = 0

    # Expire stale bounces (too old to be meaningful)
    if t[bounce_ts_key] > 0 and (now - t[bounce_ts_key]) > TRAILING_BOUNCE_EXPIRY:
        t[bounce_ts_key] = 0
        t[bounce_price_key] = 0

    # ─── UPDATE TRAILING STATE MACHINE ───
    # WATCHING: no bounces yet → wait for first bounce
    # FIRST_BOUNCE: one side bounced → wait for other side to bounce too
    # READY: both sides bounced → combined is at/near its cheapest → BUY SIGNAL
    yes_bounced = t["yes_bounce_ts"] > 0
    no_bounced = t["no_bounce_ts"] > 0

    if yes_bounced and no_bounced:
        # Both sides have bounced from their lows — the combined floor is NOW
        if t["trailing_state"] != "READY":
            t["trailing_state"] = "READY"
            t["trailing_ready_ts"] = now
    elif yes_bounced or no_bounced:
        if t["trailing_state"] == "WATCHING":
            t["trailing_state"] = "FIRST_BOUNCE"
            t["trailing_first_side"] = "YES" if yes_bounced else "NO"
            t["trailing_first_ts"] = now
        elif t["trailing_state"] == "FIRST_BOUNCE":
            # Check if the first bounce has expired — revert to WATCHING
            first_ts = t.get("trailing_first_ts", 0)
            if first_ts > 0 and (now - first_ts) > TRAILING_BOUNCE_EXPIRY:
                t["trailing_state"] = "WATCHING"
                t["trailing_first_side"] = None
                t["trailing_first_ts"] = 0
        # If we were READY and one bounce expired, downgrade back
        if t["trailing_state"] == "READY" and not (yes_bounced and no_bounced):
            t["trailing_state"] = "FIRST_BOUNCE"
            t["trailing_first_side"] = "YES" if yes_bounced else "NO"
    else:
        # No bounces active
        if t["trailing_state"] != "WATCHING":
            t["trailing_state"] = "WATCHING"
            t["trailing_first_side"] = None
            t["trailing_first_ts"] = 0
            t["trailing_ready_ts"] = 0

def _get_trailing_signal(t, now):
    """Check if the dual trailing stop signals a BUY opportunity.

    Returns (is_ready, info_str):
      - is_ready: True if both sides have bounced (entry timing confirmed)
      - info_str: human-readable description for logging

    The dual trailing pattern:
      1. Side A drops to its low, then bounces (price >= low + TRAILING_DELTA)
         → This means momentum has shifted (one side recovering)
      2. Side B (which was rising as Side A fell) now drops and bounces too
         → This means the opposite side's drop is also DONE
      3. When BOTH sides have bounced from their lows, the combined price
         is at or near its cheapest point → optimal entry timing

    This is better than:
      - Buying on momentum alone (catches only one side of the move)
      - Buying on score alone (can enter mid-drop, getting worse fills)
      - Waiting a fixed time (arbitrary, misses the actual bottom)
    """
    state = t.get("trailing_state", "WATCHING")

    if state == "READY":
        ready_age = now - t.get("trailing_ready_ts", 0)
        if ready_age > TRAILING_BOUNCE_EXPIRY:
            # Signal expired — both bounces went stale
            return False, "trailing: READY expired"

        yes_bounce = t.get("yes_bounce_price", 0)
        no_bounce = t.get("no_bounce_price", 0)
        yes_low = t.get("yes_low", 0)
        no_low = t.get("no_low", 0)
        info = (f"trailing: READY ✅ "
                f"YES low={yes_low:.3f}→bounce={yes_bounce:.3f} "
                f"NO low={no_low:.3f}→bounce={no_bounce:.3f} "
                f"combined_floor={yes_low+no_low:.3f}")
        return True, info

    elif state == "FIRST_BOUNCE":
        first = t.get("trailing_first_side", "?")
        age = now - t.get("trailing_first_ts", 0)
        waiting_for = "NO" if first == "YES" else "YES"
        info = (f"trailing: {first} bounced {age:.0f}s ago, "
                f"waiting for {waiting_for} to stop dropping")
        return False, info

    else:
        info = "trailing: WATCHING (no bounces yet)"
        return False, info

def _get_pullback(t, side, now):
    """Detect pullback-from-high for a given side.

    The Rust bot uses this as the inverse of trailing stop: track the highest
    price, buy when price dips below high - trailing_delta. This catches
    temporary dips on a surging side — the side is overall rising but just
    pulled back briefly, creating a cheaper entry.

    Returns (is_pullback, high_price, current_price, pullback_amount) or
            (False, 0, 0, 0) if no pullback detected.

    Conditions for a valid pullback:
      1. Side must have been tracked for > 60 seconds (need history)
      2. High must have been set recently (< 120s ago) — stale highs don't count
      3. Current price must be TRAILING_DELTA below the high
      4. Current price must still be in tradeable range (0.30-0.60)
      5. High must be above 0.45 (the side was actually surging, not just noise)
    """
    s = side.lower()
    high = t.get(f"{s}_high", 0)
    high_ts = t.get(f"{s}_high_ts", 0)
    current = t.get(f"{s}_now")

    if not current or high <= 0.45 or not high_ts:
        return False, 0, 0, 0

    # High must be recent (< 2 min old)
    high_age = now - high_ts
    if high_age > 120:
        return False, 0, 0, 0

    # Current price must have pulled back by at least TRAILING_DELTA
    pullback = high - current
    if pullback < TRAILING_DELTA:
        return False, 0, 0, 0

    # Current price still in tradeable range
    if current < 0.30 or current > 0.60:
        return False, 0, 0, 0

    return True, high, current, pullback

def _get_volatility(prices):
    """Calculate price volatility from a list of (timestamp, price) tuples."""
    if len(prices) < 5:
        return 0.0
    vals = [p for _, p in prices]
    avg = sum(vals) / len(vals)
    if avg <= 0:
        return 0.0
    hi = max(vals)
    lo = min(vals)
    return (hi - lo) / avg  # range as % of mean

def _get_trend_strength(prices, window_secs=60):
    """Calculate trend strength using exponential smoothing + linear regression.

    Inspired by the Rust bot's PriceTrendTracker. More nuanced than simple
    velocity — captures both the DIRECTION and CONFIDENCE of a price trend.

    Returns (slope, strength):
      - slope: linear regression slope (positive = rising, negative = falling)
      - strength: abs(slope) * sqrt(variance) — higher = more confident trend
        Values: 0 = flat/no data, 0.001-0.005 = weak, 0.005+ = strong

    The Rust bot uses this to detect early hedge opportunities:
    if strength > 0.3 (their scale) and price in hedge range → hedge now.
    We use it as a confidence multiplier for momentum scoring.
    """
    if len(prices) < 5:
        return 0.0, 0.0
    now = time.time()
    recent = [(ts, p) for ts, p in prices if now - ts <= window_secs]
    if len(recent) < 5:
        return 0.0, 0.0

    # Exponential smoothing (alpha = 0.3) to reduce noise
    alpha = 0.3
    smoothed = [recent[0][1]]
    for i in range(1, len(recent)):
        smoothed.append(alpha * recent[i][1] + (1 - alpha) * smoothed[-1])

    # Linear regression on smoothed values
    n = len(smoothed)
    x_vals = list(range(n))  # use index (evenly spaced) not timestamp
    x_mean = (n - 1) / 2.0
    y_mean = sum(smoothed) / n

    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, smoothed))
    den = sum((x - x_mean) ** 2 for x in x_vals)
    if den == 0:
        return 0.0, 0.0
    slope = num / den

    # Variance of smoothed prices
    variance = sum((y - y_mean) ** 2 for y in smoothed) / n
    strength = abs(slope) * math.sqrt(max(variance, 0))

    return slope, strength

def _get_momentum(prices, window_secs=30):
    """Calculate price momentum (velocity) over the last N seconds.
    Returns cents-per-second. Positive = price rising, negative = falling.

    This detects the "comeback" pattern: when a losing side's price is
    surging toward 0.50 (the crossover point), both sides will briefly
    be in the 0.40-0.50 range — the perfect arb window.
    """
    if len(prices) < 3:
        return 0.0
    now = time.time()
    # Get prices from the last window_secs
    recent = [(ts, p) for ts, p in prices if now - ts <= window_secs]
    if len(recent) < 3:
        return 0.0
    # Use first and last price in the window
    dt = recent[-1][0] - recent[0][0]
    if dt < 2:  # need at least 2 seconds of data
        return 0.0
    dp = recent[-1][1] - recent[0][1]
    velocity = dp / dt  # cents per second
    return velocity

def _get_momentum_score(t, now):
    """Detect momentum-based arb opportunities.

    THE PATTERN (from manual observation):
    - Game is close, one side is coming back (price rising toward 0.50)
    - When they tie/take the lead, the OTHER side drops below 0.50
    - Both sides briefly in 0.35-0.50 range = arb goldmine
    - CRITICAL: detect the surge EVEN IF it already crossed 0.50 —
      the other side is now falling into buy range.

    Returns (score_bonus, momentum_side, momentum_velocity, falling_side)
    - score_bonus: 0-75 pts added to market score
    - momentum_side: "YES" or "NO" — which side is surging UP
    - momentum_velocity: cents/sec of the surge
    - falling_side: "YES" or "NO" — which side is FALLING (avoid buying this)
    """
    yes_now = t.get("yes_now")
    no_now = t.get("no_now")
    if not yes_now or not no_now:
        return 0, None, 0, None

    yes_mom = _get_momentum(t.get("yes_prices", []), window_secs=30)
    no_mom = _get_momentum(t.get("no_prices", []), window_secs=30)

    score = 0
    surge_side = None
    surge_vel = 0
    falling_side = None  # NEW: track which side is FALLING — never buy this

    # --- Detect which side is surging (EXPANDED range: 0.30 to 0.57) ---
    # Old ceiling was 0.50 — missed surges that already crossed over!
    # The Colorado State pattern: surging from 0.30 → 0.57, by the time bot
    # checks, the surge already passed 0.50 and momentum wasn't detected.
    yes_surging = (yes_mom > 0.001 and 0.30 <= yes_now <= 0.57)
    no_surging = (no_mom > 0.001 and 0.30 <= no_now <= 0.57)

    if yes_surging and no_surging:
        # Both rising — pick the stronger surge
        if yes_mom > no_mom:
            surge_side = "YES"
            surge_vel = yes_mom
        else:
            surge_side = "NO"
            surge_vel = no_mom
    elif yes_surging:
        surge_side = "YES"
        surge_vel = yes_mom
    elif no_surging:
        surge_side = "NO"
        surge_vel = no_mom

    # --- Detect falling side (NEGATIVE momentum) ---
    # When one side surges, the other FALLS. The falling side is a TRAP —
    # it looks cheap (sweet spot 0.40-0.50) but it's falling THROUGH that range.
    # Colorado State: NMX/NO was at 48¢ falling — looked like sweet spot, was a trap.
    # Enhanced with trend strength: a weakly falling side (low strength) may just be noise.
    if yes_mom < -0.001 and 0.30 <= yes_now <= 0.55:
        _, yes_fall_str = _get_trend_strength(t.get("yes_prices", []), window_secs=60)
        if yes_fall_str >= 0.001:  # confirmed downtrend, not just noise
            falling_side = "YES"
    elif no_mom < -0.001 and 0.30 <= no_now <= 0.55:
        _, no_fall_str = _get_trend_strength(t.get("no_prices", []), window_secs=60)
        if no_fall_str >= 0.001:
            falling_side = "NO"

    # --- Also check recent surge memory (60s window) ---
    # Even if 30s velocity has slowed, a surge in the last 60s is still relevant
    if not surge_side:
        yes_mom_60 = _get_momentum(t.get("yes_prices", []), window_secs=60)
        no_mom_60 = _get_momentum(t.get("no_prices", []), window_secs=60)
        yes_surging_60 = (yes_mom_60 > 0.0005 and 0.30 <= yes_now <= 0.57)
        no_surging_60 = (no_mom_60 > 0.0005 and 0.30 <= no_now <= 0.57)
        if yes_surging_60 and (not no_surging_60 or yes_mom_60 > no_mom_60):
            surge_side = "YES"
            surge_vel = yes_mom_60
        elif no_surging_60:
            surge_side = "NO"
            surge_vel = no_mom_60

    # CRITICAL: surge side can NEVER be the falling side — resolve conflict
    # This happens when 60s memory detects an old surge but 30s shows it's now falling.
    # In that case, the surge is stale — clear it and keep the falling tag.
    if surge_side and surge_side == falling_side:
        surge_side = None
        surge_vel = 0

    if not surge_side:
        # Even with no surge detected, still return falling_side info
        return 0, None, 0, falling_side

    # Score based on velocity — faster surge = more confident crossover
    if surge_vel >= 0.005:     # 0.5¢/sec = massive surge (15¢ in 30s)
        score += 50
    elif surge_vel >= 0.003:   # 0.3¢/sec = strong surge (9¢ in 30s)
        score += 40
    elif surge_vel >= 0.002:   # 0.2¢/sec = solid momentum (6¢ in 30s)
        score += 30
    elif surge_vel >= 0.001:   # 0.1¢/sec = building momentum (3¢ in 30s)
        score += 15
    elif surge_vel >= 0.0005:  # 0.05¢/sec = slow momentum from 60s window
        score += 10

    # Bonus: both sides already in the golden range (0.35-0.52)
    if 0.35 <= yes_now <= 0.52 and 0.35 <= no_now <= 0.52:
        score += 15  # BOTH sides cheap right now — this is THE moment

    # Bonus: trend strength confirmation — strong, sustained trend = more confidence
    # Uses exponential smoothing + regression for noise-resistant trend detection.
    if surge_side:
        surge_prices = t.get(f"{surge_side.lower()}_prices", [])
        _, strength = _get_trend_strength(surge_prices, window_secs=60)
        if strength >= 0.005:
            score += 10  # strong confirmed trend — high confidence surge
        elif strength >= 0.002:
            score += 5   # moderate trend — some confirmation

    # Bonus: combined current price is already low (arb is right there)
    combined_now = yes_now + no_now
    if combined_now < 0.92:
        score += 10  # combined under target RIGHT NOW
    elif combined_now < 0.95:
        score += 5

    return score, surge_side, surge_vel, falling_side

def _score_live_market(mid, t, now):
    """Score a market for live accumulation potential. Higher = better opportunity.
    Returns (score, reason_str) or (0, None) if not worth trading.

    Scoring breakdown (max ~305 theoretical, 170+ needed to buy):
      1. Combined floor    0-40 pts  (lower combined YES_low + NO_low = better)
      2. Competitiveness   0-20 pts  (closer games = more lead changes = more dips)
      3. Volatility        0-15 pts  (more price movement in last 5min)
      4. Market type       0-12 pts  (spread/O-U/esports = thinner books)
      5. Both sides dipped 0-10 pts  (confirmed BOTH sides can get cheap)
      6. Book depth       -5 to +5   (thin books = easier fills on dips)
      7. 24h volume        0-10 pts  (enough liquidity to actually fill orders)
      8. Low recency       0-10 pts  (lows hit recently = still actively dipping)
      9. Bid-ask tightness 0-10 pts  (tight spread = more efficient market)
     10. Price trend       -10 to +5  (penalize one-way drift, reward oscillation)
     11. Time close to end 0-8 pts   (matches ending soon = more urgent action)
     12. Lead switches     0-40 pts  (more lead flips = both sides dip = arb goldmine)
     13. MOMENTUM          0-85 pts  (surge velocity + trend strength confirmation)
     14. TRAILING STOP     0-25 pts  (both sides bounced from lows = optimal timing)
    """
    mkt = t.get("mkt") or {}
    typ = mkt.get("typ", "other")
    if typ in BLOCKED_MARKET_TYPES:
        return 0, None  # blocked market type (e.g. esports match winners)

    age = now - t["first_seen"]
    if age < LIVE_WARMUP_SECS:
        return 0, None  # too fresh, need more price history

    yes_now = t.get("yes_now")
    no_now = t.get("no_now")
    if not yes_now or not no_now or yes_now < 0.10 or no_now < 0.10:
        return 0, None  # no prices or one side near zero (near-resolved, not a real arb)

    combined_floor = t.get("combined_floor", 999)
    if combined_floor >= LIVE_TARGET + 0.10:
        return 0, None  # floor too high, both sides never cheap enough
    score = 0

    # 1. Combined floor score (0-40 pts) — lower = better
    if combined_floor < LIVE_TARGET:
        score += 40  # confirmed arb potential
    elif combined_floor < 0.95:
        score += 30
    elif combined_floor < 1.00:
        score += 15
    else:
        score += 5

    # 2. Competitiveness (0-20 pts) — prefer 0.40-0.60 range
    min_side = min(yes_now, no_now)
    if LIVE_COMPETITIVE_LO <= min_side <= LIVE_COMPETITIVE_HI:
        score += 20  # competitive game
    elif 0.20 <= min_side <= 0.80:
        score += 10
    else:
        score += 3

    # 3. Volatility (0-15 pts) — more movement = more dip opportunities
    vol_yes = _get_volatility(t.get("yes_prices", []))
    vol_no = _get_volatility(t.get("no_prices", []))
    vol = max(vol_yes, vol_no)
    if vol >= 0.10:
        score += 15  # very volatile
    elif vol >= 0.03:
        score += 10
    elif vol >= LIVE_MIN_VOLATILITY:
        score += 5

    # 4. Market type (0-12 pts) — spread/O/U preferred (thin liquidity)
    score += LIVE_SPORT_SCORES.get(typ, 5)

    # 5. Both sides have dipped (0-10 pts) — confirmed both can be cheap
    both_dipped = (t["yes_low"] < 900 and t["no_low"] < 900
                   and t["yes_low"] < yes_now * 0.95 and t["no_low"] < no_now * 0.95)
    if both_dipped:
        score += 10

    # 6. Book depth penalty — thicker books = harder to get dips
    avg_depth = (t.get("yes_depth", 0) + t.get("no_depth", 0)) / 2
    if avg_depth < 50:
        score += 5  # thin book, good
    elif avg_depth > 500:
        score -= 5  # thick book, less opportunity

    # 7. 24h volume (0-10 pts) — need enough activity to actually get fills
    #    PENALTY below $15k — illiquid markets = bad fills, stuck positions
    vol24 = mkt.get("volume24", 0) or mkt.get("vol24", 0)
    if vol24 >= 50000:
        score += 10  # very active market
    elif vol24 >= 15000:
        score += 7
    elif vol24 >= 5000:
        score -= 5   # low volume penalty
    elif vol24 >= 1000:
        score -= 15  # very low volume, big penalty
    else:
        score -= 30  # dead market, effectively blocks entry

    # 8. Low recency (0-10 pts) — lows hit recently = game is actively swinging
    yes_low_age = now - t.get("yes_low_ts", 0) if t.get("yes_low_ts") else 9999
    no_low_age = now - t.get("no_low_ts", 0) if t.get("no_low_ts") else 9999
    freshest_low = min(yes_low_age, no_low_age)
    if freshest_low < 60:
        score += 10  # low hit in last 60 seconds — very active
    elif freshest_low < 300:
        score += 6   # low hit in last 5 min
    elif freshest_low < 600:
        score += 3   # low hit in last 10 min
    # Older lows = stale, no bonus

    # 9. Bid-ask tightness (0-10 pts) — tighter spread = more efficient, better fills
    yes_depth = t.get("yes_depth", 0)
    no_depth = t.get("no_depth", 0)
    # Use depth as proxy for spread tightness (more $ on book = tighter spread)
    min_depth = min(yes_depth, no_depth)
    if min_depth >= 20:
        score += 10  # both sides have real liquidity
    elif min_depth >= 5:
        score += 5
    elif min_depth > 0:
        score += 2

    # 10. Price trend — penalize one-directional drift, reward oscillation
    yes_prices = t.get("yes_prices", [])
    no_prices = t.get("no_prices", [])
    if len(yes_prices) >= 10 and len(no_prices) >= 10:
        # Check if price is trending one way (bad) or oscillating (good)
        yes_vals = [p for _, p in yes_prices[-20:]]
        no_vals = [p for _, p in no_prices[-20:]]
        # Direction: compare first half avg to second half avg
        yes_mid = len(yes_vals) // 2
        no_mid = len(no_vals) // 2
        yes_early = sum(yes_vals[:yes_mid]) / yes_mid if yes_mid > 0 else 0
        yes_late = sum(yes_vals[yes_mid:]) / (len(yes_vals) - yes_mid) if len(yes_vals) > yes_mid else 0
        no_early = sum(no_vals[:no_mid]) / no_mid if no_mid > 0 else 0
        no_late = sum(no_vals[no_mid:]) / (len(no_vals) - no_mid) if len(no_vals) > no_mid else 0

        # If YES is drifting up AND NO is drifting down = game going one way = bad
        yes_drift = abs(yes_late - yes_early)
        no_drift = abs(no_late - no_early)
        max_drift = max(yes_drift, no_drift)
        if max_drift > 0.10:
            score -= 10  # strong one-way drift, game is getting decided
        elif max_drift > 0.05:
            score -= 5   # moderate drift
        elif max_drift < 0.02:
            score += 5   # very stable/oscillating — good for accumulation

    # 11. Time to close (0-8 pts) — matches ending soon = more urgent swings
    hours_left = mkt.get("hours_left", 999)
    if 0 < hours_left <= 2:
        score += 8   # ending very soon — expect rapid swings
    elif hours_left <= 6:
        score += 5
    elif hours_left <= 24:
        score += 3

    # 12. Lead switches (-20 to +40 pts) — THE key signal for arb success.
    #     If the lead keeps flipping, BOTH sides are guaranteed to dip cheap.
    #     0 switches = one team has dominated = risky, penalize hard.
    lead_switches = t.get("lead_switches", 0)
    if lead_switches >= 4:
        score += 40   # back-and-forth battle — arb heaven
    elif lead_switches >= 2:
        score += 30
    elif lead_switches >= 1:
        score += 20
    else:
        score -= 20   # no flips yet — one side dominating, don't buy blind

    # 13. MOMENTUM (0-75 pts) — THE COMEBACK DETECTOR
    #     When a losing side's price surges toward 0.50, a lead switch is imminent.
    #     Both sides will briefly be in the 0.35-0.50 range = perfect arb window.
    #     This is the #1 signal for finding sub-minute arb opportunities.
    mom_score, mom_side, mom_vel, falling_side = _get_momentum_score(t, now)
    score += mom_score
    # Store momentum data on tracker for buy logic to use
    t["_momentum_side"] = mom_side
    t["_momentum_vel"] = mom_vel
    t["_momentum_score"] = mom_score
    t["_falling_side"] = falling_side  # side with NEGATIVE momentum — avoid buying

    # 14. DUAL TRAILING STOP (0-25 pts) — TIMING CONFIRMATION
    #     When both sides have bounced from their session lows, combined price
    #     is near its floor. This is the optimal time to enter.
    trailing_ready, trailing_info = _get_trailing_signal(t, now)
    t["_trailing_ready"] = trailing_ready
    t["_trailing_info"] = trailing_info
    if trailing_ready:
        score += 25   # both sides bounced = combined near floor = perfect timing
    elif t.get("trailing_state") == "FIRST_BOUNCE":
        score += 10   # one side bounced, watching the other — partial signal

    # 15. SPORTSBOOK ODDS EDGE (0-40 pts) — mispricing vs DraftKings/FanDuel/etc
    #     If sportsbooks say 64% but Polymarket says 55¢, that's a 9¢ edge.
    #     Bigger edge = more likely a real mispricing, not just noise.
    odds_str = ""
    t["_odds_edge"] = None
    t["_odds_bonus"] = 0
    t["_odds_movement"] = 0
    t["_odds_elo_agrees"] = False
    if _odds_available and _odds_engine:
        try:
            edge_info = _odds_engine.get_edge(
                mkt.get("title", "") or mkt.get("question", ""),
                yes_now, no_now
            )
            if edge_info and edge_info.get("confidence", 0) >= 0.70:
                edge_c = abs(edge_info.get("edge_cents", 0))
                t["_odds_edge"] = edge_info.get("edge_cents", 0)

                # 15a. Mispricing bonus — bigger edge = more points
                if edge_c >= ODDS_EDGE_MIN_CENTS:
                    bonus = min(ODDS_EDGE_MAX_BONUS, int(edge_c * 400))  # 10¢ = 40 pts
                    score += bonus
                    t["_odds_bonus"] = bonus

                # 16. SHARP LINE MOVEMENT (0-20 pts) — sharp money confirms direction
                movement = edge_info.get("line_movement", 0)
                if movement >= ODDS_MOVEMENT_THRESHOLD:
                    move_bonus = min(ODDS_MOVEMENT_MAX_BONUS, int(movement * 1000))
                    score += move_bonus
                    t["_odds_movement"] = move_bonus

                # 17. ELO CONFIRMATION (0-10 pts) — team ratings agree with price
                if edge_info.get("elo_agrees", False):
                    score += ODDS_ELO_BONUS
                    t["_odds_elo_agrees"] = True

                sb_prob = edge_info.get("sportsbook_prob", 0)
                odds_str = (f" | odds={edge_info['edge_cents']:+.3f} "
                           f"SB={sb_prob:.2f} move={movement:.3f}")
            else:
                # 18. NO EXTERNAL DATA PENALTY (-5 pts) — flying blind
                score -= ODDS_NO_DATA_PENALTY
        except Exception:
            pass  # graceful degradation — odds engine errors don't block trading

    # 19. ARB DEPTH SCORE (-10 to +10 pts) — verify fillable depth at arb prices
    arb_depth_str = ""
    try:
        _no_depth_usd = t.get("no_depth", 0)   # dollar depth, not shares
        _yes_depth_usd = t.get("yes_depth", 0)
        _min_depth = min(_no_depth_usd, _yes_depth_usd)
        if _min_depth < 10:
            score -= 10  # very thin — unlikely to fill arb
            arb_depth_str = " adepth=-10"
        elif _min_depth >= 200:
            score += 10  # deep book — reliable fills
            arb_depth_str = " adepth=+10"
        # 10-200: neutral (0 pts)
    except Exception:
        pass

    # ─── Update streak counter ───
    if score >= LIVE_MIN_SCORE_ENTRY:
        t["high_score_streak"] = t.get("high_score_streak", 0) + 1
    else:
        t["high_score_streak"] = 0
    t["last_score"] = score

    trail_state = t.get("trailing_state", "?")
    reason = (f"floor={combined_floor:.3f} vol={vol:.3f} typ={typ} "
              f"depth=${avg_depth:.0f} v24=${vol24/1000:.0f}k streak={t['high_score_streak']} "
              f"leads={lead_switches} mom={mom_vel:.4f}/{mom_side or '-'} trail={trail_state}"
              f"{odds_str}{arb_depth_str}")
    return score, reason

def _export_live_tracker():
    """Export live tracker state to JSON for the dashboard."""
    try:
        now = time.time()
        export = []
        with _live_lock:
            for mid, t in _live_tracker.items():
                mkt = t.get("mkt") or {}
                # If mkt is empty (wallet positions), try to get title from portfolio
                title = mkt.get("title", "")
                typ = mkt.get("typ", "")
                if (not title or title == "?") and portfolio:
                    pos = portfolio.positions.get(mid)
                    if pos:
                        title = pos.title
                        typ = pos.typ
                if not title:
                    title = "?"
                if not typ:
                    typ = "?"
                score, reason = _score_live_market(mid, t, now)
                export.append({
                    "mid": mid,
                    "title": title[:50],
                    "typ": typ,
                    "score": score,
                    "streak": t.get("high_score_streak", 0),
                    "yes_now": t.get("yes_now"),
                    "no_now": t.get("no_now"),
                    "yes_low": t["yes_low"] if t["yes_low"] < 900 else None,
                    "no_low": t["no_low"] if t["no_low"] < 900 else None,
                    "yes_recent_low": t.get("yes_recent_low", 999) if t.get("yes_recent_low", 999) < 900 else None,
                    "no_recent_low": t.get("no_recent_low", 999) if t.get("no_recent_low", 999) < 900 else None,
                    "yes_high": t.get("yes_high", 0) or None,
                    "no_high": t.get("no_high", 0) or None,
                    "combined_floor": t["combined_floor"] if t["combined_floor"] < 900 else None,
                    "yes_depth": t.get("yes_depth", 0),
                    "no_depth": t.get("no_depth", 0),
                    "age_secs": round(now - t["first_seen"]),
                    "vol_yes": round(_get_volatility(t.get("yes_prices", [])), 4),
                    "vol_no": round(_get_volatility(t.get("no_prices", [])), 4),
                    "vol24": mkt.get("volume24", 0),
                    "momentum_side": t.get("_momentum_side"),
                    "momentum_vel": round(t.get("_momentum_vel", 0), 5),
                    "momentum_score": t.get("_momentum_score", 0),
                    "falling_side": t.get("_falling_side"),
                    "lead_switches": t.get("lead_switches", 0),
                    "triad_combined": round(t.get("_triad_combined", 0), 4) if t.get("_triad_combined") else None,
                    "triad_slug": t.get("_triad_slug"),
                    "triad_owned": t.get("_triad_owned", 0),
                    "trailing_state": t.get("trailing_state", "WATCHING"),
                    "trailing_first_side": t.get("trailing_first_side"),
                    "trailing_ready": t.get("_trailing_ready", False),
                    "odds_edge": t.get("_odds_edge"),
                    "odds_bonus": t.get("_odds_bonus", 0),
                    "odds_movement": t.get("_odds_movement", 0),
                    "odds_elo_agrees": t.get("_odds_elo_agrees", False),
                })
        # Sort by score descending, take top 50
        export.sort(key=lambda x: x["score"], reverse=True)

        # Governor state for dashboard
        open_count = 0
        if portfolio:
            open_count = sum(1 for p in portfolio.positions.values()
                             if not p.from_wallet and p.is_accumulation and not p.is_arb_locked
                             and (now - p.created) < LIVE_SLOT_FREE_SECS)
        gov_state = {
            "positions_open": open_count,
            "positions_pending": _governor_pending,
            "positions_max": GOVERNOR_MAX_POSITIONS,
        }

        ws_stats = {
            "checks": _ws_arb_checks,
            "fires": _ws_arb_fires,
        }

        # Game scores for dashboard
        game_scores_export = {}
        if _game_scores_available and _game_score_fetcher and portfolio:
            try:
                for mid, pos in portfolio.positions.items():
                    game = _game_score_fetcher.match_position(mid, pos.title, pos.typ)
                    if game and game.get("state") in ("in", "post"):
                        game_scores_export[mid] = {
                            "home": game.get("home_short") or game.get("home_name", ""),
                            "away": game.get("away_short") or game.get("away_name", ""),
                            "home_score": game.get("home_score", 0),
                            "away_score": game.get("away_score", 0),
                            "state": game.get("state", ""),
                            "period": game.get("period", 0),
                            "clock": game.get("clock", ""),
                            "detail": game.get("detail", ""),
                        }
            except Exception:
                pass

        with open("live_tracker.json", "w") as f:
            json.dump({"updated": now, "count": len(export),
                       "start_time": _start_time,
                       "mode": "DRY" if DRY_RUN else "LIVE",
                       "governor": gov_state,
                       "ws_stats": ws_stats,
                       "game_scores": game_scores_export,
                       "markets": export[:50]}, f, indent=1)
    except Exception:
        pass  # non-critical, don't crash the bot

def ws_get(token_id):
    """Get cached WS price if fresh, else None."""
    entry = _ws_cache.get(token_id)
    if entry and (time.time() - entry[4]) < WS_STALE_SECS:
        return entry[:4]  # (ask, ask_depth, bid, bid_depth)
    return None

def ws_start(token_ids):
    """Start WebSocket price stream in background thread."""
    global _ws_thread, _ws_running
    try:
        import websockets
        import asyncio
    except ImportError:
        log.warning("⚠️ websockets not installed — using HTTP polling")
        log.warning("⚠️ Install with: pip install websockets")
        return False
    
    if _ws_running:
        # Queue new tokens for live subscription by running WS connections
        new_tokens = list(set(token_ids) - _ws_subscribed)
        if new_tokens:
            _ws_subscribed.update(new_tokens)
            _ws_new_tokens.extend(new_tokens)
            log.info(f"  🔌 WS queued {len(new_tokens)} new tokens for live subscription")
        return True
    
    _ws_subscribed.update(token_ids)
    _ws_running = True
    
    WS_BATCH_SIZE = 500  # Polymarket max assets per connection

    async def _process_ws_event(evt):
        """Process a single WS event (book, price_change, or last_trade_price)."""
        event_type = evt.get("event_type", "book")
        aid = evt.get("asset_id", "")

        if event_type == "price_change":
            # price_change events wrap changes in a list
            for pc in evt.get("price_changes", []):
                pc_aid = pc.get("asset_id", "")
                if not pc_aid: continue
                best_ask = float(pc["best_ask"]) if pc.get("best_ask") else None
                best_bid = float(pc["best_bid"]) if pc.get("best_bid") else None
                # price_change doesn't include depth, keep existing depth from cache
                old = _ws_cache.get(pc_aid)
                old_ask_d = old[1] if old else 0
                old_bid_d = old[3] if old else 0
                _ws_cache[pc_aid] = (best_ask, old_ask_d, best_bid, old_bid_d, time.time())
                _ws_check_arb(pc_aid, best_ask, old_ask_d)
            return

        if not aid: return

        if event_type == "last_trade_price":
            # Update cache with trade price as reference but don't overwrite book data
            price = float(evt.get("price", 0))
            side = evt.get("side", "")
            old = _ws_cache.get(aid)
            if old:
                if side == "BUY":
                    _ws_cache[aid] = (old[0], old[1], price, old[3], time.time())
                else:
                    _ws_cache[aid] = (price, old[1], old[2], old[3], time.time())
            return

        # Default: "book" event — full order book snapshot
        bids = evt.get("bids", [])
        asks = evt.get("asks", [])

        best_ask = None; ask_depth = 0
        for a in asks:
            p, s = float(a.get("price",0)), float(a.get("size",0))
            if p > 0 and s > 0:
                if best_ask is None or p < best_ask: best_ask = p
                ask_depth += s * p

        best_bid = None; bid_depth = 0
        for b in bids:
            p, s = float(b.get("price",0)), float(b.get("size",0))
            if p > 0 and s > 0:
                if best_bid is None or p > best_bid: best_bid = p
                bid_depth += s * p

        _ws_cache[aid] = (best_ask, round(ask_depth,2),
                          best_bid, round(bid_depth,2), time.time())
        _ws_check_arb(aid, best_ask, ask_depth)

    def _ws_check_arb(aid, best_ask, ask_depth):
        """Check if this token update creates an arb opportunity + feed tracker."""
        global _ws_arb_checks, _ws_arb_fires
        if best_ask and best_ask > 0.001:
            mkt = _ws_token_to_market.get(aid)
            if mkt:
                if mkt.get("typ", "other") in BLOCKED_MARKET_TYPES:
                    return  # blocked market type (e.g. esports match winners)
                mid = mkt["mid"]
                side = "NO" if aid == mkt["tok_no"] else "YES"
                _update_tracker(mid, side, best_ask, ask_depth, mkt)
                # Get the OTHER side's price from WS cache
                other_tok = mkt.get("tok_yes") if aid == mkt["tok_no"] else mkt["tok_no"]
                other_entry = _ws_cache.get(other_tok)
                if other_entry and other_entry[0] and other_entry[0] > 0.001:
                    other_ask = other_entry[0]
                    if aid == mkt["tok_no"]:
                        no_ask_ws, yes_ask_ws = best_ask, other_ask
                    else:
                        no_ask_ws, yes_ask_ws = other_ask, best_ask
                    combined_ws = no_ask_ws + yes_ask_ws
                    spread_ws = 1.0 - combined_ws
                    _ws_arb_checks += 1  # count every combined price check
                    # Guard: both sides must be >= 10¢. If one side is near zero,
                    # it's a near-resolved market, not a real arb opportunity.
                    if min(no_ask_ws, yes_ask_ws) < 0.10:
                        pass  # skip — one-sided market, not a real arb
                    elif combined_ws <= ARB_LOCK_THRESHOLD and spread_ws >= ARB_LOCK_MIN_SPREAD:
                        now_ws = time.time()
                        last_fire = _ws_arb_fired.get(mid, 0)
                        if now_ws - last_fire > 0.3:
                            with _ws_arb_lock:
                                _ws_arb_queue.append((mkt, no_ask_ws, yes_ask_ws, now_ws))
                                _ws_arb_fired[mid] = now_ws
                                _ws_arb_fires += 1
                            log.info(f"  🚨 WS ARB QUEUED: {mkt.get('title','?')[:40]} NO@{no_ask_ws:.3f}+YES@{yes_ask_ws:.3f}={combined_ws:.3f} ({spread_ws*100:.1f}%)")

    async def _ws_connection(tokens_chunk, conn_id):
        """Run a single WS connection for up to 500 tokens."""
        import websockets
        uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        _ws_parse_ok = 0
        _ws_ping_count = 0
        my_token_count = len(tokens_chunk)
        while _ws_running:
            try:
                # Disable websockets library ping — Polymarket uses app-level text ping/pong
                async with websockets.connect(uri, ping_interval=None, ping_timeout=None) as ws:
                    # Batch subscribe — send ALL tokens in one message (max 500 per connection)
                    sub_msg = json.dumps({"type": "market", "assets_ids": list(tokens_chunk)})
                    await ws.send(sub_msg)
                    log.info(f"  🔌 WS[{conn_id}] subscribed {my_token_count} tokens in 1 batch message")

                    async for raw in ws:
                        # Handle Polymarket app-level ping/pong (plain text, NOT JSON)
                        if raw == "ping":
                            await ws.send("pong")
                            _ws_ping_count += 1
                            continue
                        if raw == "pong":
                            continue

                        try:
                            data = json.loads(raw)
                            if isinstance(data, list):
                                for evt in data:
                                    await _process_ws_event(evt)
                            else:
                                await _process_ws_event(data)
                            _ws_parse_ok += 1
                            if _ws_parse_ok % 1000 == 0:
                                log.info(f"  📡 WS[{conn_id}] {_ws_parse_ok} events processed, {_ws_ping_count} pings handled")
                        except json.JSONDecodeError:
                            # Skip non-JSON frames we don't recognize
                            pass
                        except Exception as evt_err:
                            log.debug(f"WS[{conn_id}] event error: {evt_err}")

                        # Check for newly discovered tokens to subscribe (only conn 0 handles new tokens)
                        if conn_id == 0 and _ws_new_tokens and my_token_count < WS_BATCH_SIZE:
                            batch = []
                            while _ws_new_tokens and my_token_count < WS_BATCH_SIZE:
                                batch.append(_ws_new_tokens.pop(0))
                                my_token_count += 1
                            if batch:
                                add_msg = json.dumps({"type": "market", "assets_ids": batch})
                                await ws.send(add_msg)
                                log.info(f"  🔌 WS[{conn_id}] live-subscribed {len(batch)} new tokens (total: {my_token_count})")
            except Exception as e:
                log.info(f"  🔌 WS[{conn_id}] reconnecting ({e})")
                await asyncio.sleep(2)

    async def _ws_loop():
        """Launch WS connections — 1 per 500-token chunk."""
        import asyncio
        tokens = list(_ws_subscribed)
        # Split into chunks of 500 (Polymarket max per connection)
        chunks = [tokens[i:i+WS_BATCH_SIZE] for i in range(0, len(tokens), WS_BATCH_SIZE)]
        log.info(f"  🔌 WS launching {len(chunks)} connection(s) for {len(tokens)} tokens")
        tasks = [asyncio.create_task(_ws_connection(chunk, i)) for i, chunk in enumerate(chunks)]
        await asyncio.gather(*tasks)
    
    def _run():
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_ws_loop())
    
    import threading
    _ws_thread = threading.Thread(target=_run, daemon=True)
    _ws_thread.start()
    log.info(f"🔌 WebSocket started — subscribing {len(token_ids)} tokens")
    return True

def smart_fetch(token_id):
    """Try WS cache first, fall back to HTTP."""
    ws = ws_get(token_id)
    if ws: return ws
    return fetch_book(token_id)

def smart_batch_fetch(token_ids):
    """For each token: try WS cache, batch-HTTP the rest."""
    cache = {}
    need_http = []
    for tid in set(t for t in token_ids if t):
        ws = ws_get(tid)
        if ws:
            cache[tid] = ws
        else:
            need_http.append(tid)
    
    if need_http:
        http_cache = batch_fetch_books(need_http)
        cache.update(http_cache)
    
    return cache

# ═══════════════════════════════════════════════════════════════
# RATE LIMITER
# ═══════════════════════════════════════════════════════════════
_order_ts = deque()
_new_mkt_ts = deque()

def rate_ok(is_new=False):
    now = time.time()
    while _order_ts and now - _order_ts[0] > 60: _order_ts.popleft()
    while _new_mkt_ts and now - _new_mkt_ts[0] > 3600: _new_mkt_ts.popleft()
    if len(_order_ts) >= MAX_ORDERS_MIN: return False
    if is_new and len(_new_mkt_ts) >= MAX_NEW_MKTS_HR: return False
    return True

def record_order(is_new=False):
    _order_ts.append(time.time())
    if is_new: _new_mkt_ts.append(time.time())

# ═══════════════════════════════════════════════════════════════
# MARKET CLASSIFICATION
# ═══════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════
# POSITION TRACKER
# ═══════════════════════════════════════════════════════════════
class Position:
    """Tracks a position on one market — both NO and YES sides."""
    __slots__ = ['mid','title','typ','tok_no','tok_yes','group_key',
                 'cost','shares','n_buys','n_sells','avg_price',
                 'yes_cost','yes_shares','yes_avg_price','n_yes_buys',
                 'current_ask','current_bid','current_yes_ask','current_yes_bid',
                 'last_check','last_buy','last_sell',
                 'created','peak_price','trough_price','active',
                 'spread_at_entry',
                 'last_rebal_no_price','last_rebal_time',
                 'is_accumulation','accum_side',
                 'from_wallet','triad_group',
                 'last_hammer','total_hammered',
                 'hammer_above_since','_wallet_topped_up',
                 'total_recovered','last_recovery',
                 'total_skimmed','last_skim','skim_above_since',
                 'total_trimmed','last_trim','trim_below_since','trimmed_side',
                 'event_slug','sport','_completed_this_cycle','_last_dip_buy']
    
    def __init__(self, mid, title, typ, tok_no, tok_yes="", group_key=""):
        self.mid = mid
        self.title = title
        self.typ = typ
        self.tok_no = tok_no
        self.tok_yes = tok_yes
        self.group_key = group_key or mid
        # NO side
        self.cost = 0.0
        self.shares = 0.0
        self.n_buys = 0
        self.n_sells = 0
        self.avg_price = 0.0
        # YES side
        self.yes_cost = 0.0
        self.yes_shares = 0.0
        self.yes_avg_price = 0.0
        self.n_yes_buys = 0
        # Prices
        self.current_ask = None
        self.current_bid = None
        self.current_yes_ask = None
        self.current_yes_bid = None
        self.last_check = 0.0
        self.last_buy = 0.0
        self.last_sell = 0.0
        self.created = time.time()
        self.peak_price = 0.0
        self.trough_price = 99.0
        self.active = True
        self.spread_at_entry = 0.0  # bid-ask spread % when first entered
        self.last_rebal_no_price = 0.0  # NO price at last rebalance
        self.last_rebal_time = 0.0      # when we last rebalanced
        self.is_accumulation = False     # True if this is an independent-side accumulation
        self.accum_side = ""             # "NO" or "YES" — which side we're accumulating
        self.from_wallet = False         # True if synced from wallet — DO NOT trade against this
        self.triad_group = ""            # event_slug — links 3 legs of a triad arb together
        self.last_hammer = 0.0           # timestamp of last winner hammer buy
        self.total_hammered = 0.0        # total $ added via winner hammer
        self.hammer_above_since = 0.0    # when held side first crossed above HAMMER_THRESHOLD (resets if dips below)
        self._wallet_topped_up = False   # True after wallet top-up attempted (one-shot guard)
        self.total_recovered = 0.0       # total $ spent on hedge recovery pairs
        self.last_recovery = 0.0         # timestamp of last recovery buy
        self.total_skimmed = 0.0         # total $ added via skim buys
        self.last_skim = 0.0             # timestamp of last skim buy
        self.skim_above_since = 0.0      # when held side first crossed SKIM_THRESHOLD
        self.total_trimmed = 0.0         # total $ trimmed via loss cuts
        self.last_trim = 0.0             # timestamp of last trim sell
        self.trim_below_since = 0.0      # when held side first dropped below TRIM_THRESHOLD
        self.trimmed_side = ""           # "YES" or "NO" — set after trim, blocks dip-buy/completion of that side
        self.event_slug = ""             # Polymarket event grouping — used for per-event cluster cap
        self.sport = detect_sport(title)  # auto-detect sport from title for timing config
        self._completed_this_cycle = False  # guards against double-buy in same cycle
        self._last_dip_buy = 0.0            # cooldown timer for dip buys

    def record_buy(self, price, amount, actual_shares=None):
        """Record a NO side buy. Uses actual_shares if provided (from fill),
        otherwise estimates from amount/price."""
        self.cost += amount
        new_shares = actual_shares if actual_shares is not None else (amount / price if price > 0 else 0)
        self.shares += new_shares
        self.n_buys += 1
        calc_avg = self.cost / self.shares if self.shares > 0 else price
        self.avg_price = calc_avg if calc_avg > 0 else price  # never let avg go to 0
        self.last_buy = time.time()

    def record_yes_buy(self, price, amount, actual_shares=None):
        """Record a YES side buy (for arb locking)."""
        self.yes_cost += amount
        new_shares = actual_shares if actual_shares is not None else (amount / price if price > 0 else 0)
        self.yes_shares += new_shares
        self.n_yes_buys += 1
        calc_avg = self.yes_cost / self.yes_shares if self.yes_shares > 0 else price
        self.yes_avg_price = calc_avg if calc_avg > 0 else price  # never let avg go to 0
        self.last_buy = time.time()
    
    @property
    def total_cost(self):
        """Total invested in this market (both sides).
        Handles wallet positions where cost may be 0 but shares+avg exist.
        Falls back to current bid or 0.50 estimate if avg_price is also 0."""
        no_cost = self.cost
        if no_cost <= 0 and self.shares > 0:
            if self.avg_price > 0:
                no_cost = round(self.shares * self.avg_price, 2)
            else:
                # avg_price is 0 too — use current bid or 0.50 as last resort
                est = self.current_bid if self.current_bid and self.current_bid > 0 else 0.50
                no_cost = round(self.shares * est, 2)
        yes_cost = self.yes_cost
        if yes_cost <= 0 and self.yes_shares > 0:
            if self.yes_avg_price > 0:
                yes_cost = round(self.yes_shares * self.yes_avg_price, 2)
            else:
                est = self.current_yes_bid if hasattr(self, 'current_yes_bid') and self.current_yes_bid and self.current_yes_bid > 0 else 0.50
                yes_cost = round(self.yes_shares * est, 2)
        return no_cost + yes_cost
    
    @property
    def is_arb_locked(self):
        """True arb: both sides held AND guaranteed profit is positive (after fees).
        If shares are unequal (manual sells), this returns False so
        the position gets managed normally (cuts/hedges)."""
        if self.shares <= 0 or self.yes_shares <= 0:
            return False
        payout_per_share = 1.0 * (1.0 - POLYMARKET_FEE_PCT)
        gp = min(self.shares, self.yes_shares) * payout_per_share - self.total_cost
        return gp > 0
    
    @property
    def is_two_sided(self):
        """Holds both YES and NO shares (may or may not be profitable)."""
        return self.shares > 0 and self.yes_shares > 0
    
    @property
    def guaranteed_profit(self):
        """
        Guaranteed profit regardless of outcome, AFTER Polymarket fees.
        If YES wins: yes_shares × $1 × (1 - fee) - total_cost
        If NO wins:  no_shares × $1 × (1 - fee) - total_cost
        Guaranteed = min of the two - total_cost
        Positive = locked profit. Negative = still at risk.
        """
        if not self.is_two_sided:
            return None  # one-sided, no guarantee
        payout_per_share = 1.0 * (1.0 - POLYMARKET_FEE_PCT)  # $0.98 after 2% fee
        return min(self.shares, self.yes_shares) * payout_per_share - self.total_cost
    
    @property
    def guaranteed_roi(self):
        """Guaranteed ROI % for arb positions."""
        gp = self.guaranteed_profit
        if gp is None or self.total_cost <= 0: return 0
        return gp / self.total_cost
    
    def would_improve_roi(self, action, price, amount):
        """
        THE CORE ROI CHECK: would this action increase guaranteed profit?
        
        action: 'buy_no', 'buy_yes', 'buy_matched'
        price: price per share for the action
        amount: $ to spend
        
        Returns (improves: bool, new_guaranteed: float, old_guaranteed: float)
        """
        new_shares = amount / price if price > 0 else 0
        tc = self.total_cost
        
        if action == 'buy_no':
            new_no = self.shares + new_shares
            new_yes = self.yes_shares
            new_cost = tc + amount
        elif action == 'buy_yes':
            new_no = self.shares
            new_yes = self.yes_shares + new_shares
            new_cost = tc + amount
        elif action == 'buy_matched':
            # price = NO ask, amount = total for BOTH sides
            # Split evenly between NO and YES
            half = amount / 2
            no_shares_add = half / price if price > 0 else 0
            yes_ask = self.current_yes_ask or price
            yes_shares_add = half / yes_ask if yes_ask > 0 else 0
            new_no = self.shares + no_shares_add
            new_yes = self.yes_shares + yes_shares_add
            new_cost = tc + amount
        else:
            return False, 0, 0
        
        old_gp = self.guaranteed_profit or 0
        payout_per_share = 1.0 * (1.0 - POLYMARKET_FEE_PCT)
        new_gp = min(new_no, new_yes) * payout_per_share - new_cost

        return new_gp > old_gp, new_gp, old_gp
    
    def record_sell(self, price, share_count, revenue):
        # Proportional cost reduction — compute BEFORE decrementing shares
        old_shares = self.shares
        if old_shares > 0:
            frac_sold = share_count / old_shares
            self.cost *= (1 - frac_sold)
        else:
            self.cost = 0
        self.shares = max(0, old_shares - share_count)
        self.n_sells += 1
        self.last_sell = time.time()
    
    @property
    def pnl_pct(self):
        """Current unrealized P&L % based on bid price (what you'd actually get selling).
        Bid matches what Polymarket shows. Mid-price inflates values."""
        if self.avg_price <= 0: return 0
        if self.current_bid:
            return (self.current_bid - self.avg_price) / self.avg_price
        return 0
    
    @property
    def pnl_pct_bid(self):
        """Conservative P&L % based on bid only (what we'd actually get if selling)."""
        if not self.current_bid or self.avg_price <= 0: return 0
        return (self.current_bid - self.avg_price) / self.avg_price
    
    
    @property
    def age_hours(self):
        return (time.time() - self.created) / 3600
    
    @property
    def value(self):
        """Current estimated value (both sides) using BID price.
        Bid = what you'd actually get selling. Mid-price inflates values."""
        v = 0
        if self.shares > 0:
            if self.current_bid:
                v += self.current_bid * self.shares
            else:
                v += self.cost
        if self.yes_shares > 0:
            if self.current_yes_bid:
                v += self.current_yes_bid * self.yes_shares
            else:
                v += self.yes_cost
        return v
    
    def short(self):
        return self.title[:38] if self.title else self.mid[:12]

# ═══════════════════════════════════════════════════════════════
# PORTFOLIO
# ═══════════════════════════════════════════════════════════════
class Portfolio:
    def __init__(self):
        self.positions = {}      # mid → Position
        self.cash_deployed = 0.0
        self.total_realized = 0.0
        self.total_cost = 0.0
        self.n_entries = 0
        self.n_profit_takes = 0
        self.n_loss_cuts = 0
        self.n_dip_buys = 0
        self.n_arb_locks = 0
        self.n_accum_buys = 0     # independent-side accumulation buys
        self.n_accum_completed = 0  # accumulation positions completed into arbs

    def get(self, mid): return self.positions.get(mid)
    def has(self, mid): return mid in self.positions
    
    def add_position(self, pos):
        self.positions[pos.mid] = pos
    
    def deployed(self):
        return sum(p.total_cost for p in self.positions.values() if not p.from_wallet)

    def est_value(self):
        return sum(p.value for p in self.positions.values() if not p.from_wallet)

    def active_count(self):
        return sum(1 for p in self.positions.values() if p.shares > 0 and not p.from_wallet)
    
    def group_deployed(self, gk):
        return sum(p.total_cost for p in self.positions.values() if p.group_key == gk)
    
    def market_deployed(self, mid):
        p = self.positions.get(mid)
        return p.total_cost if p else 0
    
    def room_for(self, mid, group_key):
        """How much more can we deploy on this market?"""
        total_room = BANKROLL * MAX_DEPLOY_PCT - self.deployed()
        group_room = BANKROLL * MAX_PER_EVENT - self.group_deployed(group_key)
        mkt_room = BANKROLL * MAX_PER_MARKET - self.market_deployed(mid)
        return max(0, min(total_room, group_room, mkt_room))
    
    def arb_room_for(self, mid, group_key):
        """How much more can we deploy on this ARB? Much higher limits — guaranteed profit."""
        total_room = BANKROLL * MAX_DEPLOY_PCT - self.deployed()
        group_room = BANKROLL * ARB_MAX_PER_EVENT - self.group_deployed(group_key)
        mkt_room = BANKROLL * ARB_MAX_PER_MARKET - self.market_deployed(mid)
        return max(0, min(total_room, group_room, mkt_room))

    def triad_legs(self, triad_group):
        """Get all positions that belong to a triad group."""
        if not triad_group:
            return []
        return [p for p in self.positions.values() if p.triad_group == triad_group]

    def triad_combined_no_cost(self, triad_group):
        """Total NO cost across all legs of a triad. Used for lock detection."""
        legs = self.triad_legs(triad_group)
        return sum(p.cost for p in legs)

    def triad_is_locked(self, triad_group):
        """True if triad has all 3 legs with NO shares and guaranteed profit.
        Exactly 1 outcome wins → 2 NOs pay $1 each = $2 payout."""
        legs = self.triad_legs(triad_group)
        if len(legs) < 3:
            return False
        # All legs must have NO shares
        if not all(p.shares > 0 for p in legs):
            return False
        # Min shares across all legs (matched quantity)
        min_sh = min(p.shares for p in legs)
        total_cost = sum(p.cost for p in legs)
        payout = min_sh * 2.0  # exactly 2 legs pay $1 per share = $2
        return payout > total_cost

    def triad_guaranteed_profit(self, triad_group):
        """Guaranteed profit for a completed triad (NO on all 3 legs).
        Exactly 1 outcome wins → 2 NOs pay $1 each = $2 payout."""
        legs = self.triad_legs(triad_group)
        if len(legs) < 3:
            return 0.0
        if not all(p.shares > 0 for p in legs):
            return 0.0
        min_sh = min(p.shares for p in legs)
        total_cost = sum(p.cost for p in legs)
        return min_sh * 2.0 - total_cost

    def accum_deployed(self):
        """Total $ in accumulation (unlocked one-sided) positions."""
        return sum(p.total_cost for p in self.positions.values()
                   if p.is_accumulation and not p.is_arb_locked)

    def accum_open_count(self):
        """Number of uncompleted accumulation positions (one-sided, waiting for other side).
        Only counts positions younger than LIVE_SLOT_FREE_SECS for slot-cap purposes.
        Older positions still try to complete but don't block new entries."""
        now = time.time()
        return sum(1 for p in self.positions.values()
                   if p.is_accumulation and not p.is_arb_locked
                   and (now - p.created) < LIVE_SLOT_FREE_SECS)

    # Alias: "pending" = single-sided, searching for 2nd side (slot tracking)
    accum_pending_count = accum_open_count

    def accum_can_open_new(self):
        """Can we open a new accumulation position? Only if under the cap."""
        return self.accum_open_count() < ACCUM_MAX_OPEN
    
    def sync_wallet(self):
        """
        Pillar 0: WALLET SYNC — fetch positions from data-api.polymarket.com
        Public endpoint, no auth needed. Works in dry and live mode.
        """
        log.info("🔄 Syncing wallet from Polymarket...")
        
        if not PROXY_WALLET:
            log.warning("  No PROXY_WALLET set — can't sync. Set: export PROXY_WALLET=0x...")
            return 0
        
        wallet = PROXY_WALLET.lower()
        synced = 0
        
        # data-api.polymarket.com/positions?user=WALLET — the one that works
        url = f"https://data-api.polymarket.com/positions?user={wallet}"
        data = api_get(url, timeout=20)
        
        if not data or not isinstance(data, list):
            log.warning(f"  Wallet API returned no data for {wallet[:12]}...")
            return 0
        
        log.info(f"  Found {len(data)} positions in wallet")
        
        for item in data:
            if not isinstance(item, dict): continue
            
            token_id = str(item.get("asset", ""))
            size = float(item.get("size", 0))
            if size <= 0 or not token_id: continue
            
            condition_id = str(item.get("conditionId", token_id[:16]))
            avg_price = float(item.get("avgPrice", 0))
            initial_val = float(item.get("initialValue", 0))
            current_val = float(item.get("currentValue", 0))
            cash_pnl = float(item.get("cashPnl", 0))
            
            # Skip resolved/dead positions (worth $0, already settled)
            if current_val <= 0.01 and cash_pnl <= 0:
                continue
            
            # Look up market title from Gamma API using token ID
            title = ""
            gamma_mid = ""
            is_yes_token = False  # track which side this token is
            tok_no_id = ""
            tok_yes_id = ""
            try:
                # Try lookup by CLOB token ID
                mkt_data = api_get(
                    f"{GAMMA}/markets?clob_token_ids={token_id}&limit=1", timeout=8)
                if not mkt_data or not isinstance(mkt_data, list) or len(mkt_data) == 0:
                    # Fallback: try condition_id
                    mkt_data = api_get(
                        f"{GAMMA}/markets?id={condition_id}&limit=1", timeout=8)
                
                if mkt_data and isinstance(mkt_data, list) and len(mkt_data) > 0:
                    m = mkt_data[0]
                    title = m.get("question", "") or m.get("title", "")
                    gamma_mid = m.get("id", condition_id)
                    clob_ids = m.get("clobTokenIds", [])
                    if isinstance(clob_ids, str):
                        try: clob_ids = json.loads(clob_ids)
                        except (json.JSONDecodeError, TypeError): clob_ids = []
                    if isinstance(clob_ids, list) and len(clob_ids) >= 2:
                        tok_yes_id = str(clob_ids[0])  # [0] = YES outcome
                        tok_no_id = str(clob_ids[1])   # [1] = NO outcome
                        if token_id == tok_yes_id:
                            is_yes_token = True
                        elif token_id == tok_no_id:
                            is_yes_token = False
                        else:
                            # Token doesn't match either — try outcomes
                            outcomes = m.get("outcomes", [])
                            if outcomes and len(outcomes) >= 2:
                                log.warning(f"  Token {token_id[:12]}... doesn't match clob_ids for {title[:30]}")
            except Exception as lookup_err:
                log.debug(f"  Gamma lookup error for token {token_id[:12]}...: {lookup_err}")

            if not title:
                title = f"pos_{condition_id[:12]}"
            
            # Use Gamma market ID if found, otherwise conditionId
            mid = gamma_mid if gamma_mid else condition_id
            
            # Check if we already have a position on this market
            # (could be other side of an existing position — arb!)
            if self.has(mid):
                existing = self.positions[mid]
                existing.from_wallet = True  # mark as user's existing bet
                # FIX: compute cost and typ BEFORE using them
                cost = initial_val if initial_val > 0 else (avg_price * size if avg_price > 0 else 0)
                typ = classify(title)
                if is_yes_token and existing.yes_shares <= 0:
                    # We already have NO, this is the YES side — BOTH sides from wallet
                    existing.tok_yes = token_id
                    existing.yes_shares = size
                    existing.yes_cost = cost
                    existing.yes_avg_price = avg_price if avg_price > 0 else 0.5
                    existing.n_yes_buys = 1
                    self.total_cost += cost
                    synced += 1
                    log.info(f"  📥 +YES {title[:30]:<30s} {size:.0f}sh @{avg_price:.3f} "
                            f"[from wallet — will not trade against] [{typ}]")
                elif not is_yes_token and existing.shares <= 0:
                    # We already have YES, this is the NO side
                    existing.tok_no = token_id
                    existing.shares = size
                    existing.cost = cost
                    existing.avg_price = avg_price if avg_price > 0 else 0.5
                    existing.n_buys = 1
                    self.total_cost += cost
                    synced += 1
                    log.info(f"  📥 +NO  {title[:30]:<30s} {size:.0f}sh @{avg_price:.3f} "
                            f"[from wallet — will not trade against] [{typ}]")
                else:
                    log.debug(f"  Skip duplicate {title[:30]} (same side already tracked)")
                continue
            
            # Get live price (will be batch-fetched below after all positions loaded)
            ask, bid = None, None
            
            typ = classify(title)
            cost = initial_val if initial_val > 0 else (avg_price * size if avg_price > 0 else 0)
            
            # Create position on correct side
            if is_yes_token:
                pos = Position(mid, title, typ, tok_no_id, token_id, group_key=mid)
                # Put shares on YES side
                pos.yes_shares = size
                pos.yes_cost = cost
                pos.yes_avg_price = avg_price if avg_price > 0 else 0.5
                pos.n_yes_buys = 1
                side_label = "YES"
            else:
                pos = Position(mid, title, typ, token_id, tok_yes_id, group_key=mid)
                # Put shares on NO side (original behavior)
                pos.shares = size
                pos.cost = cost
                pos.avg_price = avg_price if avg_price > 0 else 0.5
                pos.n_buys = 1
                side_label = "NO"
            
            pos.active = True
            pos.from_wallet = True  # CRITICAL: this is the user's existing bet — DO NOT arb against it

            self.add_position(pos)
            self.total_cost += cost
            synced += 1
            
            pnl_s = f"${cash_pnl:+.2f}" if cash_pnl else ""
            log.info(f"  📥 {side_label:>3s} {title[:32]:<32s} {size:.0f}sh @{avg_price:.3f} "
                    f"val=${current_val:.2f} {pnl_s} [{typ}]")
        
        if synced > 0:
            # Batch refresh all live prices in parallel (instead of one-by-one)
            all_tokens = []
            for p in self.positions.values():
                if p.tok_no: all_tokens.append(p.tok_no)
                if p.tok_yes: all_tokens.append(p.tok_yes)
            if all_tokens:
                log.info(f"  ⚡ Batch refreshing {len(all_tokens)} token prices...")
                cache = batch_fetch_books(all_tokens)
                for p in self.positions.values():
                    if p.tok_no and p.tok_no in cache:
                        a, _, b, _ = cache[p.tok_no]
                        p.current_ask = a; p.current_bid = b
                        if b and a and a > 0:
                            p.spread_at_entry = (a - b) / a
                    if p.tok_yes and p.tok_yes in cache:
                        a, _, b, _ = cache[p.tok_yes]
                        p.current_yes_ask = a; p.current_yes_bid = b
            
            # All wallet positions stay locked — user manages them manually.
            # Bot just needs to know they exist so it doesn't trade against them.
            wallet_count = sum(1 for p in self.positions.values() if p.from_wallet)
            bot_deployed = sum(p.total_cost for p in self.positions.values() if not p.from_wallet)
            log.info(f"  ✅ Synced {synced} positions (${bot_deployed:,.2f} bot-deployed)")
            log.info(f"  🛡️  {wallet_count} wallet positions LOCKED — user-managed, bot will not touch")
        else:
            log.info("  No open positions with size > 0")
        
        return synced
    
    def save(self, path="portfolio_v10.json"):
        data = {}
        for mid, p in self.positions.items():
            data[mid] = {
                "title": p.title, "typ": p.typ,
                "tok_no": p.tok_no, "tok_yes": p.tok_yes, "group_key": p.group_key,
                "cost": p.cost, "shares": p.shares,
                "n_buys": p.n_buys, "n_sells": p.n_sells, "avg_price": p.avg_price,
                "yes_cost": p.yes_cost, "yes_shares": p.yes_shares,
                "yes_avg_price": p.yes_avg_price, "n_yes_buys": p.n_yes_buys,
                "created": p.created, "active": p.active,
                "spread_at_entry": p.spread_at_entry,
                "last_rebal_no_price": p.last_rebal_no_price,
                "is_accumulation": p.is_accumulation,
                "accum_side": p.accum_side,
                "from_wallet": p.from_wallet,
                "triad_group": p.triad_group,
                "last_hammer": p.last_hammer,
                "total_hammered": p.total_hammered,
                "hammer_above_since": p.hammer_above_since,
                "total_recovered": getattr(p, 'total_recovered', 0),
                "total_skimmed": getattr(p, 'total_skimmed', 0),
                "last_skim": getattr(p, 'last_skim', 0),
                "total_trimmed": getattr(p, 'total_trimmed', 0),
                "last_trim": getattr(p, 'last_trim', 0),
                "trim_below_since": getattr(p, 'trim_below_since', 0),
                "trimmed_side": getattr(p, 'trimmed_side', ''),
                "skim_above_since": getattr(p, 'skim_above_since', 0),
                "event_slug": getattr(p, 'event_slug', ''),
                "sport": getattr(p, 'sport', ''),
            }
        with open(path, "w") as f:
            json.dump({"positions": data, "realized": self.total_realized,
                       "total_cost": self.total_cost, "entries": self.n_entries,
                       "profit_takes": self.n_profit_takes, "loss_cuts": self.n_loss_cuts,
                       "dip_buys": self.n_dip_buys, "arb_locks": self.n_arb_locks,
                       "accum_buys": self.n_accum_buys, "accum_completed": self.n_accum_completed}, f, indent=2)
    
    def load(self, path="portfolio_v10.json"):
        if not os.path.exists(path): return
        with open(path) as f:
            data = json.load(f)
        for mid, d in data.get("positions", {}).items():
            p = Position(mid, d["title"], d["typ"], d["tok_no"],
                        d.get("tok_yes", ""), group_key=d.get("group_key",""))
            p.cost = d["cost"]; p.shares = d["shares"]; p.n_buys = d["n_buys"]
            p.n_sells = d.get("n_sells",0)
            p.avg_price = d["avg_price"] if d["avg_price"] > 0 else (d["cost"] / d["shares"] if d["shares"] > 0 and d["cost"] > 0 else 0.5 if d["shares"] > 0 else 0)
            p.yes_cost = d.get("yes_cost", 0); p.yes_shares = d.get("yes_shares", 0)
            raw_yes_avg = d.get("yes_avg_price", 0)
            p.yes_avg_price = raw_yes_avg if raw_yes_avg > 0 else (p.yes_cost / p.yes_shares if p.yes_shares > 0 and p.yes_cost > 0 else 0.5 if p.yes_shares > 0 else 0)
            p.n_yes_buys = d.get("n_yes_buys", 0)
            p.created = d.get("created", time.time()); p.active = d.get("active", True)
            p.spread_at_entry = d.get("spread_at_entry", 0.0)
            p.last_rebal_no_price = d.get("last_rebal_no_price", 0.0)
            p.is_accumulation = d.get("is_accumulation", False)
            p.accum_side = d.get("accum_side", "")
            p.from_wallet = d.get("from_wallet", False)
            p.triad_group = d.get("triad_group", "")
            p.last_hammer = d.get("last_hammer", 0.0)
            p.total_hammered = d.get("total_hammered", 0.0)
            p.hammer_above_since = d.get("hammer_above_since", 0.0)
            p.total_recovered = d.get("total_recovered", 0.0)
            p.total_skimmed = d.get("total_skimmed", 0.0)
            p.last_skim = d.get("last_skim", 0.0)
            p.total_trimmed = d.get("total_trimmed", 0.0)
            p.last_trim = d.get("last_trim", 0.0)
            p.trim_below_since = d.get("trim_below_since", 0.0)
            p.trimmed_side = d.get("trimmed_side", "")
            p.skim_above_since = d.get("skim_above_since", 0.0)
            p.event_slug = d.get("event_slug", "")
            p.sport = d.get("sport", "") or detect_sport(p.title)
            self.positions[mid] = p
        self.total_realized = data.get("realized", 0)
        self.total_cost = data.get("total_cost", 0)
        self.n_entries = data.get("entries", 0)
        self.n_profit_takes = data.get("profit_takes", 0)
        self.n_loss_cuts = data.get("loss_cuts", 0)
        self.n_dip_buys = data.get("dip_buys", 0)
        self.n_arb_locks = data.get("arb_locks", 0)
        self.n_accum_buys = data.get("accum_buys", 0)
        self.n_accum_completed = data.get("accum_completed", 0)
        locked = sum(1 for p in self.positions.values() if p.is_arb_locked)
        log.info(f"Loaded portfolio: {len(self.positions)} positions "
                 f"({locked} arb-locked), ${self.deployed():.2f} deployed")

portfolio = Portfolio()

# ═══════════════════════════════════════════════════════════════
# EXECUTION ENGINE — LIMIT-ORDER-FIRST
# ═══════════════════════════════════════════════════════════════
# RN1 places limit orders at favorable prices, not market orders.
# We only get filled when someone crosses to OUR price → we enter
# already at a profitable level relative to the spread.
#
# MODES:
#   'limit'      → GTC at mid-price (default for entries, adds)
#   'aggressive'  → GTC at ask (for arb locks where speed matters)
#   'fok'        → FOK at specified price (for urgent exits)

_clob = None

def get_clob():
    global _clob
    if _clob: return _clob
    from py_clob_client import ClobClient
    # Match v4's working config: signature_type=1 with funder=PROXY_WALLET
    _clob = ClobClient(CLOB, key=PRIVATE_KEY, chain_id=137,
                        signature_type=1, funder=PROXY_WALLET)
    _clob.set_api_creds(_clob.create_or_derive_api_creds())
    log.info(f"  CLOB connected (sig_type=1, wallet:{PROXY_WALLET[:12]}...)")
    return _clob

def calc_limit_price(bid, ask, mode="limit"):
    """Calculate the price to place our order at.
    
    'limit'      → mid-price: (bid+ask)/2, rounded to 2 decimals
                   We sit in the middle of the book; filled when someone
                   crosses to us. Entry cost ≈ mid, not ask.
    'aggressive' → at the ask (take the liquidity, fill immediately)
    'bid'        → at the bid (for selling — sit on the bid)
    
    Returns price rounded to 0.01 (Polymarket tick size).
    """
    if mode == "aggressive":
        return round(ask, 2) if ask else None
    if mode == "bid":
        return round(bid, 2) if bid else None
    # 'limit' — mid-price
    if bid and ask:
        mid = (bid + ask) / 2
        # Round to nearest 0.01 but ensure we're at or below ask
        # (don't accidentally cross the ask)
        price = round(mid, 2)
        if price >= ask:
            price = round(ask - 0.01, 2)
        if price <= 0:
            price = round(ask, 2)  # fallback
        return price
    # Fallback: if we only have ask, place slightly below
    if ask:
        return round(ask - 0.01, 2) if ask > 0.02 else round(ask, 2)
    return None

def exec_buy(token_id, price, size_usd, label="", mode="limit", bid=None, ask=None,
             is_completion=False):
    """Buy shares. ALL modes now use GTC limit orders for price certainty.

    Args:
        token_id: token to buy
        price: the limit price to place at (pre-calculated by caller)
        size_usd: dollar amount to spend
        label: log label
        mode: 'limit' (GTC at mid), 'aggressive' (GTC at ask — fills if book has it)
        bid/ask: for logging context only
        is_completion: True if buying 2nd side to lock an arb (bypasses spend governor)

    Returns (order_id, shares_filled, actual_price).
    """
    if not price or price <= 0: return None, 0, 0

    # ─── PENDING GTC GUARD — don't double-buy if a GTC is already open ───
    # If we have a pending GTC for this exact token, skip unless it's expired.
    if token_id in _pending_gtc_orders:
        pending = _pending_gtc_orders[token_id]
        age = time.time() - pending.get("ts", 0)
        if age < PENDING_GTC_EXPIRY:
            log.debug(f"  ⚠️ PENDING GTC exists for token {token_id[:12]}... "
                     f"({age:.0f}s old, ${pending.get('size_usd', 0):.2f}) — skipping buy | {label}")
            return None, 0, 0
        else:
            # Expired — clean up
            del _pending_gtc_orders[token_id]

    # ─── HARD DEPLOYMENT CAP — prevent runaway buying ───
    max_deploy = BANKROLL * MAX_DEPLOY_PCT
    if portfolio and portfolio.total_cost >= max_deploy:
        log.warning(f"  🛑 HARD CAP: total deployed ${portfolio.total_cost:.2f} >= ${max_deploy:.2f} — refusing buy | {label}")
        return None, 0, 0

    # ─── GLOBAL GOVERNOR — position cap + rolling spend window ───
    size_usd = _governor_allow_buy(size_usd, is_completion=is_completion)
    if size_usd < 1:
        return None, 0, 0

    # CRITICAL: Polymarket requires maker_amount (shares*price) ≤ 2 decimals.
    # Integer shares × 2-decimal price always produces ≤ 2 decimal cost.
    price = round(price, 2)  # ensure 2-decimal price tick
    shares = math.floor(size_usd / price)  # integer shares → clean maker_amount
    if shares < 5:
        _governor_refund(size_usd)  # Governor approved but too few shares → refund
        return None, 0, 0  # Polymarket minimum is 5 shares

    if DRY_RUN:
        tag = "LMT" if mode == "limit" else "FAK"
        spread_s = ""
        if bid and ask:
            spread_s = f" [bid={bid:.3f} ask={ask:.3f}]"
        log.info(f"  📗 BUY {tag} @{price:.2f} ${size_usd:.2f} ({shares:.0f}sh){spread_s} | {label}")
        _governor_record(size_usd)
        return f"dry_{int(time.time()*1000)}", shares, price

    try:
        from py_clob_client.order_builder.constants import BUY
        from py_clob_client.clob_types import OrderArgs, MarketOrderArgs, OrderType
        c = get_clob()

        # ORDER TYPE STRATEGY:
        # "aggressive" = FAK (Fill-And-Kill) — for hammers, skims, arb entries.
        #   Market order: fills whatever liquidity is available, kills the rest.
        #   Partial fills OK — getting 40 of 80 shares is better than 0.
        #   FOK was rejecting entire orders when full size wasn't available.
        # "limit" = GTC — for accumulation, near-arb seeding. Sits in book waiting for fills.
        if mode == "aggressive":
            fak_args = MarketOrderArgs(
                token_id=token_id,
                amount=round(size_usd, 2),
                side=BUY
            )
            signed = c.create_market_order(fak_args)
            resp = c.post_order(signed, OrderType.FAK)
        else:
            args = OrderArgs(
                token_id=token_id,
                price=price,
                size=float(shares),
                side=BUY
            )
            signed = c.create_order(args)
            resp = c.post_order(signed, OrderType.GTC)

        # Log FULL response for debugging
        log.debug(f"  API resp: {json.dumps(resp) if resp else 'None'}")

        # Check for API error in response — order failed
        error_msg = ""
        if resp and isinstance(resp, dict):
            error_msg = resp.get("errorMsg", "") or resp.get("error", "")
        if error_msg:
            log.error(f"  ❌ ORDER ERROR: {error_msg} | {label}")
            _governor_refund(size_usd)  # Order failed → refund governor
            return None, 0, 0

        if resp and resp.get("orderID"):
            tag = "LMT" if mode == "limit" else "FAK"
            status = resp.get("status", "unknown")

            if status in ("CANCELLED", "EXPIRED", "REJECTED"):
                log.warning(f"  ⚠️ Order {status}: {label} | resp={resp}")
                _governor_refund(size_usd)  # Order rejected → refund governor
                return resp["orderID"], 0, 0

            # GTC order: check how many shares actually matched immediately
            actual_shares = 0
            actual_cost = 0

            # Try to parse fill info from response
            # The CLOB API may return: takingAmount, makingAmount, matchedAmount
            # API sometimes returns empty strings instead of null — guard all float() calls
            def _safe_float(v):
                """Convert API value to float, returning 0 for empty/None/garbage."""
                if v is None or v == "" or v == "0":
                    return 0.0
                try:
                    return float(v)
                except (ValueError, TypeError):
                    return 0.0

            # CRITICAL: For BUY orders in Polymarket CLOB:
            #   makingAmount = USDC spent (what WE provide)
            #   takingAmount = shares received (what WE get)
            # These were SWAPPED in previous code — caused massive underreporting.
            making = _safe_float(resp.get("makingAmount"))  # USDC we spent
            taking = _safe_float(resp.get("takingAmount"))  # shares we received
            matched = _safe_float(resp.get("matchedAmount") or
                                  resp.get("filled") or resp.get("filledSize"))

            # ACTUAL FILL PRICE: When FAK fills, it fills at the maker's price
            # (the resting order's price), which can be BETTER than our limit.
            # Track the real fill price so callers record accurate cost basis.
            actual_fill_price = price  # default: assume fill at our order price

            if taking > 0:
                # Best case: API tells us exactly how many shares we got
                actual_shares = taking
                actual_cost = making if making > 0 else round(taking * price, 2)
                # Compute real fill price from actual data
                if making > 0 and taking > 0:
                    actual_fill_price = round(making / taking, 4)
                log.info(f"  📗 LIVE BUY {tag} @{price:.2f} "
                        f"filled={actual_shares:.1f}sh ${actual_cost:.2f}"
                        f"{f' (real@{actual_fill_price:.3f})' if actual_fill_price != price else ''}"
                        f" | {label}")
            elif making > 0:
                # Have cost but not shares — derive shares from cost
                actual_cost = making
                actual_shares = making / price
                log.info(f"  📗 LIVE BUY {tag} @{price:.2f} "
                        f"filled={actual_shares:.1f}sh ${actual_cost:.2f} | {label}")
            elif matched > 0:
                # matchedAmount — try to figure out if it's USD or shares
                if matched > shares * 2:
                    actual_cost = matched
                    actual_shares = matched / price
                else:
                    actual_shares = matched
                    actual_cost = round(matched * price, 2)
                log.info(f"  📗 LIVE BUY {tag} @{price:.2f} "
                        f"matched={actual_shares:.1f}sh ${actual_cost:.2f} | {label}")
            else:
                # No fill data in initial response.
                actual_shares = 0
                actual_cost = 0
                if mode == "aggressive":
                    # FOK with 'delayed' status — VERIFY via get_order() API.
                    # Polymarket returns "delayed" when the matching engine hasn't
                    # returned fill data yet. The order may have actually filled.
                    # We MUST check, otherwise we'll keep buying the same position.
                    order_id = resp["orderID"]
                    log.info(f"  📗 LIVE BUY {tag} @{price:.3f} ${size_usd:.2f} "
                            f"({shares:.0f}sh) [DELAYED — verifying fill...] | {label}")
                    try:
                        # Retry loop: Polymarket may return null if order is still processing
                        # At high prices (75¢+), matching takes longer — need more patience.
                        order_data = None
                        for _retry_i, _retry_delay in enumerate([1.0, 2.0, 3.0, 4.0]):
                            time.sleep(_retry_delay)
                            order_data = c.get_order(order_id)
                            try:
                                _od_dump = json.dumps(order_data, default=str) if order_data else "None"
                            except Exception:
                                _od_dump = repr(order_data)
                            log.info(f"  🔎 ORDER VERIFY attempt {_retry_i+1}: {_od_dump[:500]}")
                            if order_data and isinstance(order_data, dict):
                                break  # got a real response
                            log.info(f"  ⏳ get_order returned {type(order_data).__name__}, retrying...")

                        if order_data and isinstance(order_data, dict):
                            # OpenOrder fields from Polymarket CLOB API:
                            #   status        — "MATCHED", "CANCELLED", "EXPIRED", "LIVE"
                            #   size_matched  — shares filled (string, e.g. "16.5")
                            #   original_size — shares ordered (string)
                            #   price         — order price (string)
                            #   side          — "BUY" or "SELL"
                            od_status = order_data.get("status", "")
                            od_size_matched = _safe_float(order_data.get("size_matched", 0))
                            od_original_size = _safe_float(order_data.get("original_size", 0))
                            od_price = _safe_float(order_data.get("price", 0))

                            if od_status == "MATCHED" or od_size_matched > 0:
                                # FILLED — use actual fill data, but cap at what we requested
                                actual_shares = od_size_matched if od_size_matched > 0 else shares
                                if actual_shares > shares * 1.05:
                                    # Accept real shares — capping causes position mismatch
                                    # with on-chain balance. Position sync will reconcile.
                                    log.warning(f"  ⚠️ OVERFILL: got {actual_shares:.1f}sh but "
                                               f"requested {shares:.0f}sh — using REAL amount | {label}")
                                # od_price from get_order() is unreliable — it can
                                # return the match/market price, not our limit. For FAK
                                # orders the fill price CANNOT exceed our limit, so cap it.
                                # For GTC-like orders, also cap at limit as a safety measure.
                                if od_price > 0 and od_price <= price * 1.02:
                                    fill_price = od_price  # plausible — use it
                                else:
                                    fill_price = price     # use our limit as fill price
                                    if od_price > 0:
                                        log.warning(f"  ⚠️ od_price={od_price:.3f} >> limit={price:.3f}"
                                                   f" — using limit as fill price | {label}")
                                actual_fill_price = fill_price
                                actual_cost = round(actual_shares * fill_price, 2)
                                log.info(f"  ✅ VERIFIED FILL: {actual_shares:.1f}sh "
                                        f"${actual_cost:.2f} @{fill_price:.3f} (status={od_status}, "
                                        f"size_matched={od_size_matched}) | {label}")
                            elif od_status in ("CANCELLED", "EXPIRED"):
                                # FOK was rejected — definitively NOT filled
                                log.info(f"  ❌ VERIFIED NO FILL: status={od_status} | {label}")
                                _governor_refund(size_usd)
                                return order_id, 0, 0
                            else:
                                # Any other status (LIVE, empty, unknown) — assume filled
                                # because Polymarket returned success:true + delayed, meaning
                                # the order was accepted. Conservative = count it.
                                log.warning(f"  ⚠️ AMBIGUOUS STATUS '{od_status}' "
                                           f"size_matched={od_size_matched} — assuming filled | {label}")
                                actual_shares = od_size_matched if od_size_matched > 0 else shares
                                actual_cost = round(actual_shares * price, 2)
                        else:
                            # get_order() returned None/non-dict after all retries.
                            # Cannot verify the fill — treat as NOT filled to avoid ghost positions.
                            # If it actually filled, wallet sync will pick it up next restart.
                            log.warning(f"  ❌ get_order returned None after 4 retries — "
                                       f"treating as NO FILL {shares:.0f}sh ${size_usd:.2f} | {label}")
                            _governor_refund(size_usd)
                            return order_id, 0, 0
                    except Exception as ve:
                        # Verification failed — treat as no fill to avoid ghost positions
                        log.warning(f"  ❌ ORDER VERIFY FAILED: {ve} — treating as NO FILL | {label}")
                        _governor_refund(size_usd)
                        return order_id, 0, 0
                else:
                    log.info(f"  📗 LIVE BUY {tag} @{price:.3f} ${size_usd:.2f} "
                            f"({shares:.0f}sh) [GTC PENDING in book] | {label}")
                    # Track pending GTC to prevent double-buying this token
                    _pending_gtc_orders[token_id] = {
                        "order_id": resp["orderID"],
                        "ts": time.time(),
                        "size_usd": size_usd,
                        "price": price,
                    }
                    # GTC: don't refund — order may fill later. verify_positions() handles it.

            _governor_record(actual_cost if actual_cost > 0 else size_usd)
            # Return ACTUAL fill price — callers should use this for cost basis,
            # not the ask price they detected. FAK fills at maker's price which
            # can be cheaper than the limit. Accurate cost → accurate profit calc.
            return resp["orderID"], actual_shares, actual_fill_price
        else:
            log.warning(f"  ⚠️ No orderID in response: {resp} | {label}")
            _governor_refund(size_usd)  # No order placed → refund governor
    except Exception as e:
        err_str = str(e)
        if "not enough balance" in err_str or "not enough allowance" in err_str:
            log.warning(f"  ⚠️ BUY FAILED — insufficient balance: {label}")
            _governor_refund(size_usd)  # Failed → refund governor
            return "NO_BALANCE", 0, 0
        log.error(f"  BUY ERR: {e}")
        _governor_refund(size_usd)  # Exception → refund governor
    return None, 0, 0

def exec_sell(token_id, price, shares, label="", mode="limit"):
    """Sell shares.

    Args:
        price: the price to sell at
        shares: number of shares to sell
        mode: 'limit' (GTC at bid) or 'aggressive' (FAK — fill what's available, kill rest)
    """
    if not price or price <= 0: return None, 0, 0
    # CRITICAL: Polymarket requires maker_amount ≤ 2 decimals.
    # Integer shares × 2-decimal price = clean amounts.
    price = round(price, 2)
    shares = math.floor(shares)  # integer shares
    if shares < 5: return None, 0, 0  # Polymarket minimum
    revenue = round(shares * price, 2)

    order_style = "FAK" if mode == "aggressive" else "GTC"
    if DRY_RUN:
        log.info(f"  📕 SELL {order_style} @{price:.2f} {shares:.0f}sh (${revenue:.2f}) | {label}")
        return f"dry_{int(time.time()*1000)}", revenue, price

    try:
        from py_clob_client.order_builder.constants import SELL
        from py_clob_client.clob_types import OrderArgs, MarketOrderArgs, OrderType
        c = get_clob()
        if mode == "aggressive":
            # FAK (Fill-And-Kill) — sell whatever liquidity is available.
            # Partial fills OK: selling 30 of 50 trimmed shares still recovers value.
            fak_args = MarketOrderArgs(
                token_id=token_id,
                amount=round(revenue, 2),
                side=SELL
            )
            signed = c.create_market_order(fak_args)
            otype = OrderType.FAK
        else:
            args = OrderArgs(
                token_id=token_id,
                price=price,
                size=float(shares),
                side=SELL
            )
            signed = c.create_order(args)
            otype = OrderType.GTC
        resp = c.post_order(signed, otype)
        log.debug(f"  SELL resp: {json.dumps(resp) if resp else 'None'}")
        if resp and resp.get("orderID"):
            status = resp.get("status", "unknown")
            if status in ("CANCELLED", "EXPIRED", "REJECTED"):
                log.warning(f"  ⚠️ SELL {status}: {label}")
                return resp["orderID"], 0, 0

            # Parse actual fill for FAK partial fills
            def _sf(v):
                if v is None or v == "" or v == "0": return 0.0
                try: return float(v)
                except (ValueError, TypeError): return 0.0

            actual_revenue = revenue  # default: assume full fill
            taking = _sf(resp.get("takingAmount"))   # USDC received (for SELL)
            making = _sf(resp.get("makingAmount"))    # shares sold
            matched = _sf(resp.get("matchedAmount") or resp.get("filled") or resp.get("filledSize"))

            if taking > 0:
                actual_revenue = taking
            elif making > 0 and making < shares:
                # Partial fill — making = shares actually sold
                actual_revenue = round(making * price, 2)
            elif matched > 0 and matched < revenue:
                actual_revenue = matched

            if actual_revenue < revenue:
                log.info(f"  📕 LIVE SELL {order_style} @{price:.3f} "
                        f"PARTIAL {actual_revenue:.2f}/{revenue:.2f} | {label}")
            else:
                log.info(f"  📕 LIVE SELL {order_style} @{price:.3f} {shares:.0f}sh | {label}")
            return resp["orderID"], actual_revenue, price
    except Exception as e:
        err_str = str(e)
        if "not enough balance" in err_str or "not enough allowance" in err_str:
            log.warning(f"  ⚠️ SELL FAILED — no shares on-chain: {label}")
            return "NO_BALANCE", 0, 0
        log.error(f"  SELL ERR: {e}")
    return None, 0, 0

# ═══════════════════════════════════════════════════════════════
# STALE ORDER CANCELLATION — clean up unfilled GTC orders
# ═══════════════════════════════════════════════════════════════
def cancel_stale_orders(max_age_minutes=2):
    """Cancel GTC orders that have been sitting unfilled for too long.

    Unfilled one-sided orders from arb attempts are dangerous:
    - If only NO filled and YES is pending, we have one-sided risk
    - If the arb window closes, we're stuck with a speculative position

    Called every 30s alongside verify_positions().
    """
    if DRY_RUN:
        return 0

    try:
        c = get_clob()
        # Get all open orders
        open_orders = c.get_orders()  # returns list of open orders
        if not open_orders:
            return 0

        now = time.time()
        cancelled = 0

        for order in open_orders:
            if not isinstance(order, dict):
                continue

            order_id = order.get("id", "") or order.get("orderID", "")
            if not order_id:
                continue

            # Check if order is stale (created > max_age_minutes ago)
            created = order.get("createdAt", "") or order.get("timestamp", "")
            if not created:
                continue

            try:
                if isinstance(created, (int, float)):
                    order_age_min = (now - created) / 60
                else:
                    from datetime import datetime as _dt
                    ct = _dt.fromisoformat(str(created).replace("Z", "+00:00"))
                    order_age_min = (now - ct.timestamp()) / 60
            except (ValueError, TypeError):
                continue

            # Only cancel orders older than threshold that have 0 fills
            filled = float(order.get("filledSize", 0) or order.get("sizeFilled", 0) or 0)
            if order_age_min > max_age_minutes and filled <= 0:
                try:
                    c.cancel(order_id)
                    token_id = order.get("asset_id", "") or order.get("tokenID", "")
                    price = order.get("price", "?")
                    # Clean up pending GTC tracker so the token isn't blocked
                    if token_id and token_id in _pending_gtc_orders:
                        del _pending_gtc_orders[token_id]
                    log.info(f"  🗑️ Cancelled stale order: {order_id[:12]}... "
                            f"@{price} ({order_age_min:.0f}m old, 0 fills)")
                    cancelled += 1
                except Exception as ce:
                    log.debug(f"  Cancel error for {order_id[:12]}...: {ce}")

        if cancelled > 0:
            log.info(f"  🗑️ Cancelled {cancelled} stale unfilled orders")
        return cancelled
    except Exception as e:
        log.debug(f"  Stale order cleanup error: {e}")
        return 0

# ═══════════════════════════════════════════════════════════════
# POST-TRADE VERIFICATION — sync real fills from wallet
# ═══════════════════════════════════════════════════════════════
def verify_positions():
    """After trading, check wallet for actual share counts.
    Corrects any mismatches between what we THINK we have and reality.
    Called every 30s in the main loop.
    """
    if not PROXY_WALLET:
        return

    wallet = PROXY_WALLET.lower()
    url = f"https://data-api.polymarket.com/positions?user={wallet}"
    data = api_get(url, timeout=15)
    if not data or not isinstance(data, list):
        return

    # Build lookup: token_id → (size, avgPrice, initialValue)
    wallet_positions = {}
    for item in data:
        if not isinstance(item, dict): continue
        token_id = str(item.get("asset", ""))
        size = float(item.get("size", 0))
        if token_id and size > 0:
            wallet_positions[token_id] = {
                "size": size,
                "avgPrice": float(item.get("avgPrice", 0)),
                "initialValue": float(item.get("initialValue", 0)),
                "currentValue": float(item.get("currentValue", 0)),
            }

    corrections = 0
    for mid, pos in portfolio.positions.items():
        # Check NO side
        if pos.tok_no and pos.tok_no in wallet_positions:
            real = wallet_positions[pos.tok_no]
            real_shares = real["size"]
            real_avg = real["avgPrice"]
            real_cost = real["initialValue"] if real["initialValue"] > 0 else (real_avg * real_shares)

            if abs(real_shares - pos.shares) > 1:  # >1 share difference
                log.warning(f"  🔧 CORRECTION {pos.short()} NO: "
                           f"tracked={pos.shares:.1f}sh@{pos.avg_price:.3f} "
                           f"→ actual={real_shares:.1f}sh@{real_avg:.3f}")
                old_cost = pos.cost
                pos.shares = real_shares
                pos.avg_price = real_avg if real_avg > 0 else (pos.avg_price if pos.avg_price > 0 else 0.5)
                pos.cost = real_cost if real_cost > 0 else (pos.avg_price * real_shares)
                portfolio.total_cost += (pos.cost - old_cost)
                corrections += 1

        # Check YES side
        if pos.tok_yes and pos.tok_yes in wallet_positions:
            real = wallet_positions[pos.tok_yes]
            real_shares = real["size"]
            real_avg = real["avgPrice"]
            real_cost = real["initialValue"] if real["initialValue"] > 0 else (real_avg * real_shares)

            if abs(real_shares - pos.yes_shares) > 1:
                log.warning(f"  🔧 CORRECTION {pos.short()} YES: "
                           f"tracked={pos.yes_shares:.1f}sh@{pos.yes_avg_price:.3f} "
                           f"→ actual={real_shares:.1f}sh@{real_avg:.3f}")
                old_cost = pos.yes_cost
                pos.yes_shares = real_shares
                pos.yes_avg_price = real_avg if real_avg > 0 else (pos.yes_avg_price if pos.yes_avg_price > 0 else 0.5)
                pos.yes_cost = real_cost if real_cost > 0 else (pos.yes_avg_price * real_shares)
                portfolio.total_cost += (real_cost - old_cost)
                corrections += 1

    if corrections > 0:
        log.info(f"  🔧 Corrected {corrections} position mismatches from wallet data")

    # ─── RESOLVED POSITION CLEANUP ───
    # Detect positions where the market has resolved:
    #   1. Wallet shows 0 shares for BOTH sides → market settled, shares redeemed or lost
    #   2. One side went to 0 in wallet but we still track shares → losing side resolved to $0
    # Log the resolution and zero out the position so zombie cleanup removes it.
    resolved_count = 0
    for mid, pos in list(portfolio.positions.items()):
        if pos.shares <= 0 and pos.yes_shares <= 0:
            continue  # already empty — zombie cleanup handles this

        no_in_wallet = pos.tok_no in wallet_positions if pos.tok_no else False
        yes_in_wallet = pos.tok_yes in wallet_positions if pos.tok_yes else False

        # Both sides gone from wallet = market fully resolved & redeemed (or never had shares)
        both_gone = (not no_in_wallet and not yes_in_wallet)
        # Must have had shares at some point to count as resolved (not a brand-new empty position)
        had_shares = (pos.shares > 0 or pos.yes_shares > 0)

        if both_gone and had_shares:
            # Check market status via Gamma to confirm it's actually closed
            # (avoid false positives from API hiccups)
            try:
                mkt_data = api_get(f"{GAMMA}/markets/{mid}?limit=1", timeout=8)
                if mkt_data and isinstance(mkt_data, dict):
                    is_closed = mkt_data.get("closed", False)
                    is_active = mkt_data.get("active", True)
                    if not is_closed and is_active:
                        continue  # market still active — API might be lagging
                elif mkt_data and isinstance(mkt_data, list) and len(mkt_data) > 0:
                    m = mkt_data[0]
                    if not m.get("closed", False) and m.get("active", True):
                        continue
            except Exception:
                continue  # can't confirm — skip this cycle

            # Market is confirmed closed + no shares in wallet = resolved
            no_val = round(pos.shares * pos.avg_price, 2) if pos.shares > 0 else 0
            yes_val = round(pos.yes_shares * pos.yes_avg_price, 2) if pos.yes_shares > 0 else 0
            log.info(f"  🏁 RESOLVED: {pos.title[:50]} — "
                    f"NO={pos.shares:.0f}sh(${no_val:.2f}) "
                    f"YES={pos.yes_shares:.0f}sh(${yes_val:.2f}) "
                    f"→ zeroed out (shares redeemed or resolved to $0)")

            # Record resolution for analytics
            # Determine winner: side that was worth more at entry is likely winner
            if pos.shares > 0 and pos.yes_shares > 0:
                winner = "YES" if pos.yes_avg_price > pos.avg_price else "NO"
            elif pos.yes_shares > 0:
                winner = "YES"
            elif pos.shares > 0:
                winner = "NO"
            else:
                winner = "UNKNOWN"
            record_resolution(mid, pos.title, getattr(pos, 'typ', ''),
                             winner, 0, 0, pos)

            # Zero out — zombie cleanup will remove it next cycle
            portfolio.total_cost -= (pos.cost + pos.yes_cost)
            pos.shares = 0
            pos.yes_shares = 0
            pos.cost = 0
            pos.yes_cost = 0
            pos.active = False
            resolved_count += 1

        # Single side disappeared: losing side resolved to $0
        # Only flag if we tracked shares and they're gone from wallet
        elif pos.shares > 0 and pos.tok_no and not no_in_wallet:
            # NO shares gone from wallet but we tracked them
            # Could be: (a) resolved to $0, (b) sold and verify_positions already zeroed
            # Only act if position is old enough (>30 min) to avoid racing with sells
            age = time.time() - pos.created
            if age > 1800:  # 30 min
                log.info(f"  🏁 RESOLVED SIDE: {pos.title[:40]} NO={pos.shares:.0f}sh "
                        f"→ gone from wallet (resolved to $0 or redeemed)")
                portfolio.total_cost -= pos.cost
                pos.shares = 0
                pos.avg_price = 0
                pos.cost = 0
                resolved_count += 1

        elif pos.yes_shares > 0 and pos.tok_yes and not yes_in_wallet:
            age = time.time() - pos.created
            if age > 1800:
                log.info(f"  🏁 RESOLVED SIDE: {pos.title[:40]} YES={pos.yes_shares:.0f}sh "
                        f"→ gone from wallet (resolved to $0 or redeemed)")
                portfolio.total_cost -= pos.yes_cost
                pos.yes_shares = 0
                pos.yes_avg_price = 0
                pos.yes_cost = 0
                resolved_count += 1

    if resolved_count > 0:
        log.info(f"  🏁 Cleaned up {resolved_count} resolved position(s)")
        portfolio.save()

# ═══════════════════════════════════════════════════════════════
# MARKET DISCOVERY
# ═══════════════════════════════════════════════════════════════
def discover_markets():
    """Fetch active sports markets — v4 PROVEN approach.
    Orders by 24h volume (sports naturally rank high) and
    filters by endDate (within MAX_DAYS_OUT days = upcoming matches).
    NO TAG FILTERING — Gamma tags are unreliable."""
    log.info("🔍 Discovering markets...")
    markets = []
    seen_ids = set()
    t0 = time.time()
    rejected = {"no_clob": 0, "not_sports": 0, "too_far": 0, "low_vol": 0,
                "no_end_date": 0, "accepted": 0}
    now_utc = datetime.now(timezone.utc)
    
    # EXPANDED discovery: fetch up to 2000 markets (10 pages), keep up to 500
    # RN1 finds 200+ markets — we need to go deep into the volume list
    # to catch tennis qualifiers, lower-tier soccer, challenger events etc.
    for offset in range(0, 2000, 200):
        url = (f"{GAMMA}/markets?limit=200&offset={offset}"
               f"&active=true&closed=false"
               f"&order=volume24hr&ascending=false")

        data = api_get(url, timeout=15)
        if not data or not isinstance(data, list):
            log.warning(f"  offset={offset}: no data")
            if offset > 0: break  # no more pages
            continue

        log.info(f"  Fetched {len(data)} markets (offset={offset})")
        if len(data) < 50:
            # Less than 50 results = we've exhausted available markets
            pass  # still process them, but will break after

        for m in data:
            mid = m.get("id", "")
            if mid in seen_ids: continue
            if not m.get("active") or m.get("closed"): continue
            seen_ids.add(mid)

            # End date filter: must close within MAX_DAYS_OUT days
            end_str = m.get("endDate") or m.get("end_date_iso") or ""
            max_hours = MAX_DAYS_OUT * 24  # use config (default 7 days = 168h)
            hours_left = max_hours  # default: assume far out if no date
            if end_str:
                try:
                    end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                    hours_left = (end_dt - now_utc).total_seconds() / 3600
                    if hours_left < -1:
                        rejected["too_far"] += 1
                        continue
                    if hours_left > max_hours:
                        # Check if title contains a date that's within range
                        # Polymarket often sets endDate to season end, not match date
                        # "Will X win on 2026-03-03" → match is TODAY even if endDate is months out
                        title_tmp = m.get("question", "") or m.get("title", "")
                        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', title_tmp)
                        if date_match:
                            try:
                                match_dt = datetime.fromisoformat(date_match.group(1) + "T23:59:59+00:00")
                                match_hours = (match_dt - now_utc).total_seconds() / 3600
                                if match_hours <= max_hours and match_hours >= -24:
                                    hours_left = match_hours  # use match date, not endDate
                                else:
                                    rejected["too_far"] += 1
                                    continue
                            except (ValueError, TypeError):
                                rejected["too_far"] += 1
                                continue
                        else:
                            # No date in title — check if it looks like an event market
                            # Sports keywords = clearly an event, accept regardless of volume
                            tl_tmp = title_tmp.lower()
                            if ('vs' in tl_tmp or 'end in a draw' in tl_tmp
                                    or 'o/u' in tl_tmp or ' win on ' in tl_tmp
                                    or ' beat ' in tl_tmp or ' win ' in tl_tmp
                                    or 'over/under' in tl_tmp or 'spread:' in tl_tmp):
                                hours_left = 48  # assume near-term event
                            else:
                                rejected["too_far"] += 1
                                continue
                except (ValueError, TypeError):
                    pass  # unparseable date — keep market with default hours_left

            # CLOB tokens
            clob_ids = m.get("clobTokenIds", [])
            if isinstance(clob_ids, str):
                try: clob_ids = json.loads(clob_ids)
                except (json.JSONDecodeError, TypeError): clob_ids = []
            if not isinstance(clob_ids, list) or len(clob_ids) < 2:
                rejected["no_clob"] += 1
                continue

            # Volume filter: lowered from 100→25 to catch tennis/challenger markets
            # RN1 hits fresh low-volume markets where arbs are juiciest
            vol24 = float(m.get("volume24hr") or m.get("volume_24h") or 0)
            if vol24 < 5:  # was 25 — low-volume markets have juiciest mispricing
                rejected["low_vol"] += 1
                continue

            title = m.get("question", "") or m.get("title", "")

            # STRICT SPORTS GATE — only sports markets allowed
            if not is_sports_market(title, end_str):
                rejected["not_sports"] += 1
                continue

            typ = classify(title)
            rejected["accepted"] += 1

            markets.append({
                "mid": mid, "title": title, "typ": typ,
                "is_sports": True,
                "tok_yes": str(clob_ids[0]), "tok_no": str(clob_ids[1]),
                "volume": float(m.get("volume", 0) or 0),
                "volume24": vol24,
                "liquidity": float(m.get("liquidity", 0) or 0),
                "group": m.get("groupItemTitle", "") or m.get("conditionId", "") or mid,
                "event_slug": m.get("event_slug", m.get("groupSlug", "")),
                "hours_left": hours_left,
            })

        time.sleep(0.15)  # slightly more polite per page

        # Stop if we got less than a full page (no more data)
        if len(data) < 50:
            break
    
    elapsed = time.time() - t0
    from collections import Counter
    tc = Counter(m["typ"] for m in markets)
    log.info(f"  ✅ {len(markets):,} sports markets in {elapsed:.1f}s | {dict(tc)}")
    log.info(f"  Filter: accepted={rejected['accepted']} too_far={rejected['too_far']} "
             f"not_sports={rejected['not_sports']} low_vol={rejected['low_vol']} "
             f"no_clob={rejected['no_clob']}")

    if markets:
        # ─── EVENT EXPANSION: find ALL sibling markets for discovered matches ───
        # When we find "Will Team A win", also grab O/U 1.5, O/U 2.5, draw, BTTS, spread etc.
        # RN1 trades 6-8 markets per match — we need siblings from events API.
        markets = expand_event_siblings(markets, seen_ids, now_utc)

        # Sort by soonest ending first — live/ending games get capital first
        markets.sort(key=lambda m: m["hours_left"])
        for m in markets[:8]:
            h = m["hours_left"]
            time_s = f"LIVE" if h <= 0 else (f"{h:.1f}h" if h < 24 else f"{h/24:.0f}d")
            log.info(f"    ✓ {m['typ']:<8s} {time_s:>5s}  vol24=${m['volume24']:>8,.0f}  {m['title'][:45]}")
    else:
        log.error("  ⚠ STILL 0 MARKETS — endDate filter may be too strict")
        log.error("  Retrying WITHOUT endDate filter...")

        # Emergency fallback: skip endDate, just use is_sports_market
        for offset in [0, 200]:
            url = (f"{GAMMA}/markets?limit=200&offset={offset}"
                   f"&active=true&closed=false"
                   f"&order=volume24hr&ascending=false")
            data = api_get(url, timeout=15)
            if not data: continue
            for m in data:
                mid = m.get("id", "")
                if mid in seen_ids: continue
                seen_ids.add(mid)
                clob_ids = m.get("clobTokenIds", [])
                if isinstance(clob_ids, str):
                    try: clob_ids = json.loads(clob_ids)
                    except: clob_ids = []
                if not isinstance(clob_ids, list) or len(clob_ids) < 2: continue
                title = m.get("question", "") or m.get("title", "")
                if not is_sports_market(title, ""):  continue
                typ = classify(title)
                markets.append({
                    "mid": mid, "title": title, "typ": typ,
                    "tok_yes": str(clob_ids[0]), "tok_no": str(clob_ids[1]),
                    "volume": float(m.get("volume", 0) or 0),
                    "volume24": float(m.get("volume24hr") or 0),
                    "liquidity": float(m.get("liquidity", 0) or 0),
                    "group": m.get("groupItemTitle", "") or m.get("conditionId", "") or mid,
                    "event_slug": m.get("event_slug", m.get("groupSlug", "")),
                    "hours_left": 48,  # unknown end date, put at back of queue
                })
            time.sleep(0.1)

        # Sort fallback markets too
        markets.sort(key=lambda m: m["hours_left"])
        log.info(f"  Fallback found {len(markets)} markets (no endDate filter)")
        for m in markets[:5]:
            log.info(f"    ✓ {m['typ']:<8s} {m['title'][:55]}")

    return markets


def expand_event_siblings(markets, seen_ids, now_utc):
    """EVENT EXPANSION — RN1's edge: trade ALL markets per match, not just one.

    When we find "Will Vélez win on 2026-03-02" via volume-based discovery,
    use the event_slug to fetch ALL sibling markets for that event:
    Win A, Win B, Draw, O/U 1.5, O/U 2.5, O/U 3.5, BTTS, Spread, etc.

    This catches low-volume siblings (like O/U 3.5) that never appear in
    the top-by-volume pages but may have juicy arbs because fewer bots watch them.
    """
    # Collect unique event slugs we already found
    slugs = set()
    for m in markets:
        slug = m.get("event_slug", "")
        if slug and len(slug) > 3:
            slugs.add(slug)

    if not slugs:
        return markets

    log.info(f"  🔗 Expanding {len(slugs)} events for sibling markets...")

    siblings_added = 0
    # Batch fetch events — do 10 at a time to avoid huge URLs
    slug_list = list(slugs)

    for slug in slug_list[:50]:  # cap at 50 events to avoid API spam
        try:
            # Try event_slug first (Gamma's grouping field), fall back to slug
            url = f"{GAMMA}/markets?event_slug={slug}&active=true&closed=false&limit=50"
            data = api_get(url, timeout=10)
            if not data or not isinstance(data, list) or len(data) == 0:
                # Fallback: try the events endpoint which groups all markets per event
                url = f"{GAMMA}/events/{slug}?active=true&closed=false"
                evt_data = api_get(url, timeout=10)
                if evt_data and isinstance(evt_data, dict):
                    data = evt_data.get("markets", [])
                elif not data:
                    continue
            if not data or not isinstance(data, list):
                continue

            for m in data:
                mid = m.get("id", "")
                if mid in seen_ids:
                    continue
                if not m.get("active") or m.get("closed"):
                    continue
                seen_ids.add(mid)

                clob_ids = m.get("clobTokenIds", [])
                if isinstance(clob_ids, str):
                    try: clob_ids = json.loads(clob_ids)
                    except (json.JSONDecodeError, TypeError): clob_ids = []
                if not isinstance(clob_ids, list) or len(clob_ids) < 2:
                    continue

                title = m.get("question", "") or m.get("title", "")
                if not is_sports_market(title, ""):
                    continue

                # Parse end date for this sibling — same smart logic
                end_str = m.get("endDate") or m.get("end_date_iso") or ""
                max_hours_sib = MAX_DAYS_OUT * 24
                hours_left = max_hours_sib
                if end_str:
                    try:
                        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                        hours_left = (end_dt - now_utc).total_seconds() / 3600
                        if hours_left < -1:
                            continue
                        if hours_left > max_hours_sib:
                            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', title)
                            if date_match:
                                try:
                                    match_dt = datetime.fromisoformat(date_match.group(1) + "T23:59:59+00:00")
                                    match_hours = (match_dt - now_utc).total_seconds() / 3600
                                    if match_hours <= max_hours_sib and match_hours >= -24:
                                        hours_left = match_hours
                                    else:
                                        continue
                                except (ValueError, TypeError):
                                    continue
                            else:
                                tl_sib = title.lower()
                                if ('vs' in tl_sib or 'end in a draw' in tl_sib
                                        or 'o/u' in tl_sib or ' win on ' in tl_sib
                                        or ' beat ' in tl_sib or ' win ' in tl_sib
                                        or 'over/under' in tl_sib or 'spread:' in tl_sib):
                                    hours_left = 48
                                else:
                                    continue
                    except (ValueError, TypeError):
                        pass

                typ = classify(title)
                vol24 = float(m.get("volume24hr") or m.get("volume_24h") or 0)

                markets.append({
                    "mid": mid, "title": title, "typ": typ,
                    "is_sports": is_sports_market(title, ""),
                    "tok_yes": str(clob_ids[0]), "tok_no": str(clob_ids[1]),
                    "volume": float(m.get("volume", 0) or 0),
                    "volume24": vol24,
                    "liquidity": float(m.get("liquidity", 0) or 0),
                    "group": m.get("groupItemTitle", "") or m.get("conditionId", "") or mid,
                    "event_slug": m.get("event_slug", m.get("groupSlug", slug)),
                    "hours_left": hours_left,
                })
                siblings_added += 1

            time.sleep(0.1)  # polite rate
        except Exception as e:
            log.debug(f"  Event expansion error for {slug}: {e}")

    if siblings_added > 0:
        log.info(f"  🔗 Expanded: +{siblings_added} sibling markets "
                 f"(total: {len(markets)})")

    return markets

args_scan_mode = False  # set by main() — disables trading in scan-only mode

# ═══════════════════════════════════════════════════════════════
# GLOBAL TRADE GOVERNOR — the ONLY thing that decides if a buy can fire.
# Lives inside exec_buy / exec_batch_buy so NO code path can bypass it.
# TIME-BASED, not cycle-based (arb_engine runs every 50ms, cycle resets are useless).
# ═══════════════════════════════════════════════════════════════
GOVERNOR_MAX_POSITIONS = LIVE_MAX_PENDING  # hard cap on open positions (3)
_governor_pending = 0                # positions approved but not yet in portfolio
                                     # (prevents same-cycle multi-buy race condition)

def _governor_allow_buy(size_usd, is_completion=False):
    """Gate for buys. Only enforces position cap + per-trade max.
    No spend window — speed matters, buy when the opportunity is there.

    is_completion=True: buying the 2nd side to LOCK an arb.
      → Skips position cap (we WANT to complete arbs).
      → Still caps per-trade at LIVE_MAX_BUY.
    """
    global _governor_pending

    if is_completion:
        # Completions need to match existing shares — no $ cap, just let it through
        return size_usd

    approved = min(size_usd, LIVE_MAX_BUY)

    # Position cap — only count FRESH positions (< 5 min old) against the cap
    if portfolio:
        now = time.time()
        open_count = sum(1 for p in portfolio.positions.values()
                         if not p.from_wallet and p.is_accumulation and not p.is_arb_locked
                         and (now - p.created) < LIVE_SLOT_FREE_SECS)
    else:
        open_count = 0
    total_open = open_count + _governor_pending
    if total_open >= GOVERNOR_MAX_POSITIONS:
        return 0

    _governor_pending += 1
    log.info(f"  🔒 GOVERNOR: approved ${approved:.2f} "
             f"(pos={total_open + 1}/{GOVERNOR_MAX_POSITIONS})")

    return approved

def _governor_record(amount):
    """No-op — kept for call-site compatibility."""
    pass

def _governor_position_added():
    """Call when a position is actually added to portfolio.
    Decrements pending counter since portfolio now tracks it."""
    global _governor_pending
    if _governor_pending > 0:
        _governor_pending -= 1

def _governor_refund(amount):
    """Refund pending counter when a buy fails."""
    global _governor_pending
    if _governor_pending > 0:
        _governor_pending -= 1
        log.info(f"  🔓 GOVERNOR REFUND: ${amount:.2f} returned "
                 f"(pending={_governor_pending})")

def _pending_slots_full():
    """Check if we've hit the max open position limit (only fresh positions count)."""
    if not portfolio:
        return _governor_pending >= GOVERNOR_MAX_POSITIONS
    now = time.time()
    open_count = sum(1 for p in portfolio.positions.values()
                     if not p.from_wallet and p.is_accumulation and not p.is_arb_locked
                     and (now - p.created) < LIVE_SLOT_FREE_SECS)
    return (open_count + _governor_pending) >= GOVERNOR_MAX_POSITIONS

def arb_engine(all_markets):
    """DEDICATED ARB ENGINE — runs EVERY cycle (~50ms), ZERO sleeps.
    
    CORE profit generator. Models RN1's behavior:
    RN1 hammers 50-150+ trades per market, eating entire order book.
    
    PHASE 1: BUILD EXISTING ARBS
      Path A: Unmatched NO → buy YES to LOCK (ROI: would_improve_roi check)
      Path B: Already locked → add matched pairs (ROI: would_improve_roi + pair_cost < 1.0)
    
    PHASE 2: SCAN ALL MARKETS for new arbs
      ROI: combined < 0.98 = guaranteed 2%+ profit per pair
    
    ALL arb actions verified through ROI gate before execution.
    Returns: number of arb trades executed this cycle
    """
    trades = 0
    cycle_price_cache = {}  # shared price cache for this cycle (Phase 1 + 2)

    # ═══ PHASE 0: INSTANT WS-TRIGGERED ARBS (v4-style push model) ═══
    # Process any arbs detected by the WS event handler.
    # These fire at best-ask WITHOUT full book fetch — speed beats depth.
    # v4 locks arbs in <50ms this way. Full book walk happens in Phase 1 next cycle.
    ws_arbs = []
    with _ws_arb_lock:
        ws_arbs = list(_ws_arb_queue)
        _ws_arb_queue.clear()

    for mkt, no_ask_ws, yes_ask_ws, ts in ws_arbs:
        mid = mkt["mid"]
        # BLOCKED market types (e.g. esports match winners)
        if mkt.get("typ", "other") in BLOCKED_MARKET_TYPES:
            continue
        # Guard: both sides must be >= 10¢ — if one side is near zero it's
        # a near-resolved market, not a real arb (just looks cheap on paper)
        if min(no_ask_ws, yes_ask_ws) < 0.10:
            continue
        # Skip if already in portfolio or wallet-blocked
        if portfolio.has(mid):
            continue
        if mid in _burned_markets:
            continue  # trimmed/stale-exited — don't re-enter
        # O/U & SPREAD LOPSIDED GUARD: skip if one side > 65¢ (game already shifted)
        if mkt.get("typ", "") in ("ou", "spread") and max(no_ask_ws, yes_ask_ws) > 0.65:
            continue
        # ─── GLOBAL PENDING CAP — no new positions if slots full ───
        if _pending_slots_full():
            break
        if not rate_ok(is_new=True):
            break

        arb_room = portfolio.arb_room_for(mid, mkt.get("group", mid))
        if arb_room < 2:
            continue

        combined_ws = no_ask_ws + yes_ask_ws
        spread_ws = 1.0 - combined_ws
        age_ms = (time.time() - ts) * 1000

        log.info(f"  ⚡ WS-ARB DETECTED! {mkt['title'][:40]} "
                f"NO@{no_ask_ws:.3f}+YES@{yes_ask_ws:.3f}={combined_ws:.3f} "
                f"({spread_ws*100:.1f}%) [age={age_ms:.0f}ms]")

        # FIRE IMMEDIATELY at best-ask + slippage — no full book fetch!
        # This is how v4 locks arbs. Full book eating happens next cycle in Phase 1.
        # CRITICAL: ensure order prices NEVER sum above $0.99 — otherwise we overpay.
        # Distribute slippage proportionally but cap total.
        no_order_p = round(no_ask_ws + ARB_SLIPPAGE, 2)
        yes_order_p = round(yes_ask_ws + ARB_SLIPPAGE, 2)
        order_combined = no_order_p + yes_order_p
        if order_combined > 0.99:
            # Scale back both prices proportionally to fit under 0.99
            scale = 0.99 / order_combined
            no_order_p = round(no_ask_ws + ARB_SLIPPAGE * scale, 2)
            yes_order_p = round(yes_ask_ws + ARB_SLIPPAGE * scale, 2)
            # Final safety cap
            if no_order_p + yes_order_p > 0.99:
                no_order_p = round(0.99 - yes_order_p, 2)
        no_order_p = min(no_order_p, 0.99)
        yes_order_p = min(yes_order_p, 0.99)

        # Size: cap at ARB_INITIAL_MAX per side on first entry.
        # Confirm both sides fill before scaling up (Phase 1 handles scaling next cycle).
        budget_per_side = min(arb_room / 2, ARB_INITIAL_MAX)
        pair_cost = no_ask_ws + yes_ask_ws
        max_pairs = budget_per_side / max(no_ask_ws, yes_ask_ws) if pair_cost > 0 else 0
        no_shares = math.floor(max_pairs * 100) / 100
        yes_shares = no_shares  # matched shares
        if no_shares < 5:
            continue

        no_cost = round(no_order_p * no_shares, 2)   # cost at ORDER price (includes slippage)
        yes_cost = round(yes_order_p * yes_shares, 2)

        # Build batch order for BOTH sides in ONE API call — pass MATCHED target_shares
        # so exec_batch_buy doesn't recalculate different share counts per side
        batch_list = [
            (mkt["tok_no"], no_order_p, no_cost, f"⚡WS-NO|@{no_ask_ws:.3f}|{mkt['title'][:20]}", no_shares),
            (mkt["tok_yes"], yes_order_p, yes_cost, f"⚡WS-YES|@{yes_ask_ws:.3f}|{mkt['title'][:20]}", yes_shares),
        ]
        results = exec_batch_buy(batch_list, label_prefix=f"WS-ARB|{mkt['title'][:20]}")

        pos = Position(mid, mkt["title"], mkt["typ"], mkt["tok_no"],
                     mkt["tok_yes"], mkt.get("group", mid))
        total_spent = 0
        for i, (oid, sh, ap) in enumerate(results):
            if not oid or oid == "NO_BALANCE":
                continue
            if sh > 0:
                real_price = no_ask_ws if i == 0 else yes_ask_ws
                actual_cost = round(sh * real_price, 2)
                if i == 0:
                    pos.record_buy(real_price, actual_cost, actual_shares=sh)
                else:
                    pos.record_yes_buy(real_price, actual_cost, actual_shares=sh)
                total_spent += actual_cost
                trades += 1

        if total_spent > 0:
            pos.current_ask = no_ask_ws
            pos.current_bid = None
            portfolio.add_position(pos)
            _governor_position_added()
            portfolio.n_entries += 1
            portfolio.total_cost += total_spent
            record_order(is_new=True)
            if pos.shares > 0 and pos.yes_shares > 0:
                gp = min(pos.shares, pos.yes_shares) * (1.0 - POLYMARKET_FEE_PCT) - pos.total_cost
                if gp > 0:
                    log.info(f"  ⚡ WS-ARB LOCKED! {mkt['title'][:40]} "
                            f"${total_spent:.2f} spent → ${gp:.2f} guaranteed ({spread_ws*100:.1f}%)")
                    portfolio.n_arb_locks += 1
                else:
                    log.warning(f"  ⚠️ WS-ARB HEDGE (GP negative): {mkt['title'][:40]} "
                               f"${total_spent:.2f} spent → GP=${gp:.2f} (NOT locking)")
                    # Mark as accumulation so it can be improved or exited
                    pos.is_accumulation = True
                    pos.accum_side = "NO" if pos.shares > pos.yes_shares else "YES"
            else:
                # One side only — mark as accumulation so Step A completion can finish it
                pos.is_accumulation = True
                pos.accum_side = "NO" if pos.shares > 0 and pos.yes_shares == 0 else "YES"
                log.info(f"  ⚡ WS-ARB PARTIAL: {mkt['title'][:40]} ${total_spent:.2f} spent "
                        f"(one side pending, marked accum {pos.accum_side} for completion)")

    # ─── Reset per-cycle flags ───
    for _mid, _pos in portfolio.positions.items():
        _pos._completed_this_cycle = False

    # ═══ PHASE 1: BUILD / LOCK EXISTING POSITIONS ═══
    # Get all positions that have a YES token (arb candidates)
    # Include bot positions + wallet positions that are ALREADY two-sided (hedged).
    # One-sided wallet positions are excluded — bot must NEVER buy the opposite
    # side of a user's directional bet. Two-sided wallet positions CAN benefit
    # from Step R recovery (buy cheap matched pairs to dilute avg).
    arb_positions = [(mid, pos) for mid, pos in portfolio.positions.items()
                     if (pos.shares > 0 or pos.yes_shares > 0) and pos.tok_yes
                     and (not pos.from_wallet or pos.is_two_sided)]

    if arb_positions:
        # Batch fetch ALL prices in one parallel call
        arb_tokens = []
        for mid, pos in arb_positions:
            arb_tokens.append(pos.tok_no)
            arb_tokens.append(pos.tok_yes)
        arb_cache = smart_batch_fetch(arb_tokens)
        cycle_price_cache.update(arb_cache)  # share with Phase 2 + 3

        # Pre-fetch FULL order books in parallel for locked positions (Path B)
        # AND hedged positions (Step R recovery) — need books for both
        full_book_tokens = []
        for mid, pos in arb_positions:
            if pos.is_arb_locked and portfolio.arb_room_for(mid, pos.group_key) >= 2:
                full_book_tokens.append(pos.tok_no)
                full_book_tokens.append(pos.tok_yes)
            elif pos.is_two_sided and not pos.is_arb_locked:
                # Hedged position — fetch books for recovery buying
                full_book_tokens.append(pos.tok_no)
                full_book_tokens.append(pos.tok_yes)
        full_book_cache = batch_fetch_full_books(full_book_tokens) if full_book_tokens else {}

        for mid, pos in arb_positions:
            no_ask, _, no_bid, _ = arb_cache.get(pos.tok_no, (None, 0, None, 0))
            yes_ask, _, yes_bid, _ = arb_cache.get(pos.tok_yes, (None, 0, None, 0))
            if not no_ask or not yes_ask or yes_ask <= 0.001: continue
            
            # Update position cache
            pos.current_ask = no_ask
            pos.current_bid = no_bid
            pos.current_yes_ask = yes_ask
            pos.current_yes_bid = yes_bid
            
            # ── PATH A: NOT YET LOCKED — buy other side to create arb ──
            # SKIP hammered/trimmed positions — uneven shares are INTENTIONAL.
            # Hammer adds extra shares on the winning side; trim sells losing shares.
            # Trying to "even out" by buying the other side undoes the whole strategy.
            # Detect by total_hammered, total_trimmed, OR significant share imbalance.
            _share_imbalance = abs(pos.shares - pos.yes_shares)
            _is_shaped = (pos.total_hammered > 0
                         or getattr(pos, 'total_trimmed', 0) > 0
                         or (pos.is_two_sided and _share_imbalance >= 5))
            # Skip wallet positions in Path A — don't complete/even one-sided wallet bets.
            # Wallet hedged positions get improved via Step R recovery instead.
            # BUT wallet locked arbs CAN benefit from Path B (adding matched pairs).
            _skip_path_a = pos.from_wallet or _is_shaped
            if not pos.is_arb_locked and not _skip_path_a:
                # DIRECTION 1: Have NO shares → buy YES to lock
                if pos.avg_price > 0 and pos.shares > pos.yes_shares:
                    combined = pos.avg_price + yes_ask
                    spread_pct = 1.0 - combined
                    if combined <= ARB_LOCK_THRESHOLD and spread_pct >= ARB_LOCK_MIN_SPREAD:
                        unmatched = max(0, pos.shares - pos.yes_shares)
                        if unmatched > 0:
                            # ─── FRESH VALIDATION before completion ───
                            _m_data = _ws_token_to_market.get(pos.tok_yes, {})
                            _yb = None  # fresh YES book (if available)
                            if _m_data:
                                _fv, _fn, _fy, _nb, _yb, _fr = validate_arb_prices_fresh(_m_data)
                                if _fy > 0:
                                    yes_ask = _fy  # use fresh price
                                    combined = pos.avg_price + yes_ask
                                    spread_pct = 1.0 - combined
                                    if combined > ARB_LOCK_THRESHOLD or spread_pct < ARB_LOCK_MIN_SPREAD:
                                        log.info(f"  ⏳ DEFER PathA-D1: fresh combined={combined:.3f} "
                                                f"> threshold | {pos.short()}")
                                        continue  # skip this cycle, try next
                                    # Check depth for YES side
                                    if _yb:
                                        _da, _df = validate_book_depth(_yb, unmatched,
                                                                       yes_ask + ARB_DYNAMIC_SLIP_MAX)
                                        if _da < ARB_MIN_DEPTH_SHARES:
                                            log.info(f"  ⏳ DEFER PathA-D1: YES depth={_da:.0f}sh "
                                                    f"< {ARB_MIN_DEPTH_SHARES} | {pos.short()}")
                                            continue
                            room = portfolio.arb_room_for(mid, pos.group_key)
                            # Cap order price so combined never exceeds $0.99
                            max_yes_p = round(0.99 - pos.avg_price, 2)
                            # Dynamic limit pricing if we have fresh book data
                            if _m_data and _yb:
                                yes_order_price, _ = calculate_dynamic_limit(
                                    _yb, unmatched, pos.avg_price)
                                yes_order_price = min(yes_order_price, max_yes_p, 0.99) if yes_order_price > 0 \
                                    else min(round(yes_ask + ARB_SLIPPAGE, 2), max_yes_p, 0.99)
                            else:
                                yes_order_price = min(round(yes_ask + ARB_SLIPPAGE, 2), max_yes_p, 0.99)
                            target = min(unmatched, room / yes_ask if yes_ask > 0 else 0)
                            if target >= 1:
                                yes_cost = round(yes_ask * target, 2)
                                improves, _, _ = pos.would_improve_roi('buy_yes', yes_ask, yes_cost)
                                if improves and yes_cost >= 1 and rate_ok():
                                    label = f"🔒ARB-LOCK-YES|{spread_pct*100:.1f}%|slip+{ARB_SLIPPAGE}|{pos.short()}"
                                    order_cost = round(yes_order_price * target, 2)
                                    oid, sh, fill_p = exec_buy(pos.tok_yes, yes_order_price, order_cost,
                                                       label, mode="aggressive", is_completion=True)
                                    if oid and oid != "NO_BALANCE" and sh > 0:
                                        real_fill = fill_p if fill_p > 0 else yes_ask
                                        # SAFETY: if real fill price is way above our limit,
                                        # the DELAYED path may have returned a bad price.
                                        # Cap fill price at our order limit + small tolerance.
                                        if real_fill > yes_order_price * 1.10:
                                            log.warning(f"  ⚠️ FILL PRICE SANITY: fill={real_fill:.3f} "
                                                       f">> limit={yes_order_price:.3f} — using limit | {label}")
                                            real_fill = yes_order_price
                                        actual_cost = round(sh * real_fill, 2)
                                        pos.record_yes_buy(real_fill, actual_cost, actual_shares=sh)
                                        gp = pos.guaranteed_profit or 0
                                        combined_actual = pos.avg_price + real_fill
                                        if gp > 0:
                                            log.info(f"  🔒 LOCKED! {pos.short()} "
                                                    f"NO@{pos.avg_price:.3f}+YES@{real_fill:.3f}(limit={yes_order_price:.3f})="
                                                    f"{combined_actual:.3f} → ${gp:.2f} guaranteed")
                                            portfolio.n_arb_locks += 1
                                            if pos.is_accumulation:
                                                pos.is_accumulation = False
                                                portfolio.n_accum_completed += 1
                                                log.info(f"  🎯 ACCUM→ARB! {pos.short()} promoted to locked arb")
                                        else:
                                            log.warning(f"  ⚠️ HEDGE (not arb): {pos.short()} "
                                                       f"NO@{pos.avg_price:.3f}+YES@{real_fill:.3f}={combined_actual:.3f} "
                                                       f"→ GP=${gp:.2f} (negative, NOT promoting to locked)")
                                            # Don't promote to locked — leave as accumulation/hedge
                                        portfolio.total_cost += actual_cost
                                        record_order()
                                        trades += 1

                # DIRECTION 2: Have YES shares → buy NO to lock
                # (Previously missing — bot could buy YES first but never complete by buying NO)
                elif pos.yes_avg_price > 0 and pos.yes_shares > pos.shares:
                    combined = no_ask + pos.yes_avg_price
                    spread_pct = 1.0 - combined
                    if combined <= ARB_LOCK_THRESHOLD and spread_pct >= ARB_LOCK_MIN_SPREAD:
                        unmatched = max(0, pos.yes_shares - pos.shares)
                        if unmatched > 0:
                            # ─── FRESH VALIDATION before completion ───
                            _m_data2 = _ws_token_to_market.get(pos.tok_no, {})
                            _nb2 = None  # fresh NO book (if available)
                            if _m_data2:
                                _fv2, _fn2, _fy2, _nb2, _yb2, _fr2 = validate_arb_prices_fresh(_m_data2)
                                if _fn2 > 0:
                                    no_ask = _fn2  # use fresh price
                                    combined = no_ask + pos.yes_avg_price
                                    spread_pct = 1.0 - combined
                                    if combined > ARB_LOCK_THRESHOLD or spread_pct < ARB_LOCK_MIN_SPREAD:
                                        log.info(f"  ⏳ DEFER PathA-D2: fresh combined={combined:.3f} "
                                                f"> threshold | {pos.short()}")
                                        continue
                                    # Check depth for NO side
                                    if _nb2:
                                        _da2, _df2 = validate_book_depth(_nb2, unmatched,
                                                                         no_ask + ARB_DYNAMIC_SLIP_MAX)
                                        if _da2 < ARB_MIN_DEPTH_SHARES:
                                            log.info(f"  ⏳ DEFER PathA-D2: NO depth={_da2:.0f}sh "
                                                    f"< {ARB_MIN_DEPTH_SHARES} | {pos.short()}")
                                            continue
                            room = portfolio.arb_room_for(mid, pos.group_key)
                            # Cap order price so combined never exceeds $0.99
                            max_no_p = round(0.99 - pos.yes_avg_price, 2)
                            # Dynamic limit pricing if we have fresh book data
                            if _m_data2 and _nb2:
                                no_order_price, _ = calculate_dynamic_limit(
                                    _nb2, unmatched, pos.yes_avg_price)
                                no_order_price = min(no_order_price, max_no_p, 0.99) if no_order_price > 0 \
                                    else min(round(no_ask + ARB_SLIPPAGE, 2), max_no_p, 0.99)
                            else:
                                no_order_price = min(round(no_ask + ARB_SLIPPAGE, 2), max_no_p, 0.99)
                            target = min(unmatched, room / no_ask if no_ask > 0 else 0)
                            if target >= 1:
                                no_cost = round(no_ask * target, 2)
                                improves, _, _ = pos.would_improve_roi('buy_no', no_ask, no_cost)
                                if improves and no_cost >= 1 and rate_ok():
                                    label = f"🔒ARB-LOCK-NO|{spread_pct*100:.1f}%|slip+{ARB_SLIPPAGE}|{pos.short()}"
                                    order_cost = round(no_order_price * target, 2)
                                    oid, sh, fill_p = exec_buy(pos.tok_no, no_order_price, order_cost,
                                                       label, mode="aggressive", is_completion=True)
                                    if oid and oid != "NO_BALANCE" and sh > 0:
                                        real_fill = fill_p if fill_p > 0 else no_ask
                                        # SAFETY: cap fill price at limit + tolerance
                                        if real_fill > no_order_price * 1.10:
                                            log.warning(f"  ⚠️ FILL PRICE SANITY: fill={real_fill:.3f} "
                                                       f">> limit={no_order_price:.3f} — using limit | {label}")
                                            real_fill = no_order_price
                                        actual_cost = round(sh * real_fill, 2)
                                        pos.record_buy(real_fill, actual_cost, actual_shares=sh)
                                        gp = pos.guaranteed_profit or 0
                                        combined_actual = real_fill + pos.yes_avg_price
                                        if gp > 0:
                                            log.info(f"  🔒 LOCKED! {pos.short()} "
                                                    f"YES@{pos.yes_avg_price:.3f}+NO@{real_fill:.3f}(limit={no_order_price:.3f})="
                                                    f"{combined_actual:.3f} → ${gp:.2f} guaranteed")
                                            portfolio.n_arb_locks += 1
                                            if pos.is_accumulation:
                                                pos.is_accumulation = False
                                                portfolio.n_accum_completed += 1
                                                log.info(f"  🎯 ACCUM→ARB! {pos.short()} promoted to locked arb")
                                        else:
                                            log.warning(f"  ⚠️ HEDGE (not arb): {pos.short()} "
                                                       f"YES@{pos.yes_avg_price:.3f}+NO@{real_fill:.3f}={combined_actual:.3f} "
                                                       f"→ GP=${gp:.2f} (negative, NOT promoting to locked)")
                                        portfolio.total_cost += actual_cost
                                        record_order()
                                        trades += 1

            # ── PATH B: ALREADY LOCKED — ADD MATCHED PAIRS (RN1-style) ──
            elif pos.is_arb_locked:
                # SKIP hammered positions — the imbalance is INTENTIONAL.
                # Hammer added extra shares on the winning side; Path B would
                # re-even them out by buying the losing side, undoing the hammer.
                # ALSO skip trimmed positions — trim cut losses for a reason.
                # Re-buying pairs on a trimmed position compounds the loss.
                if pos.total_hammered > 0 or getattr(pos, 'total_trimmed', 0) > 0:
                    continue
                pair_cost = no_ask + yes_ask
                if pair_cost < 1.0:  # still profitable at current prices
                    room = portfolio.arb_room_for(mid, pos.group_key)
                    if room >= 2:
                        # Use batch-fetched full books (pre-fetched above)
                        no_full_data = full_book_cache.get(pos.tok_no)
                        yes_full_data = full_book_cache.get(pos.tok_yes)
                        no_asks_full = no_full_data[0] if no_full_data else []
                        yes_asks_full = yes_full_data[0] if yes_full_data else []

                        if no_asks_full and yes_asks_full:
                            # Walk the book — find every profitable level
                            walk_orders, walk_gp, walk_cost = walk_book_arb(
                                pos, no_asks_full, yes_asks_full, room)

                            if walk_orders and walk_gp > 0.10:
                                # Batch all walk orders into ONE API call
                                batch_list = []
                                batch_sides = []
                                for side, price, cost_usd, shares in walk_orders:
                                    if not rate_ok(): break
                                    token = pos.tok_no if side == 'NO' else pos.tok_yes
                                    label = f"🔒EAT-{side}|@{price:.3f}|{pos.short()}"
                                    # cost_usd = real cost; price = slippage limit
                                    real_price = round(cost_usd / shares, 4) if shares > 0 else price
                                    order_cost = round(price * shares, 2)  # cost at order price
                                    batch_list.append((token, price, order_cost, label, shares))
                                    batch_sides.append((side, price, cost_usd, real_price))

                                results = exec_batch_buy(batch_list, is_completion=True)
                                cycle_trades = 0
                                no_filled = 0
                                yes_filled = 0
                                for i, (oid, sh, ap) in enumerate(results):
                                    if oid and oid != "NO_BALANCE":
                                        side, price, cost_usd, real_price = batch_sides[i]
                                        if side == 'NO':
                                            pos.record_buy(real_price, cost_usd)
                                            no_filled += 1
                                        else:
                                            pos.record_yes_buy(real_price, cost_usd)
                                            yes_filled += 1
                                        portfolio.total_cost += cost_usd
                                        record_order()
                                        trades += 1
                                        cycle_trades += 1
                                if no_filled != yes_filled and cycle_trades > 0:
                                    log.warning(f"  ⚠️ BATCH ASYMMETRY: {pos.short()} — "
                                               f"NO filled {no_filled}, YES filled {yes_filled}. "
                                               f"Position has unmatched exposure.")

                                if cycle_trades > 0:
                                    gp = pos.guaranteed_profit or 0
                                    log.info(f"  🔒 ATE BOOK! {pos.short()} "
                                            f"{cycle_trades} orders, ${walk_cost:.2f} spent "
                                            f"→ ${gp:.2f} guaranteed")
                        else:
                            # Fallback: single-level buy at best ask + slippage
                            # Budget uses REAL prices; orders placed at ask+slippage for fill guarantee
                            no_order_p = min(round(no_ask + ARB_SLIPPAGE, 2), 0.99)
                            yes_order_p = min(round(yes_ask + ARB_SLIPPAGE, 2), 0.99)
                            pair_real_cost = no_ask + yes_ask
                            max_pairs = room / pair_real_cost if pair_real_cost > 0 else 0
                            if max_pairs >= 5:
                                batch_no_order = round(no_order_p * max_pairs, 2)
                                batch_no_real = round(no_ask * max_pairs, 2)
                                improves, _, _ = pos.would_improve_roi(
                                    'buy_matched', no_ask, batch_no_real * 2)
                                if improves and rate_ok():
                                    oid_no, sh_no, _ = exec_buy(pos.tok_no, no_order_p,
                                        batch_no_order,
                                        f"🔒ARB+NO|slip|{pos.short()}",
                                        mode="aggressive", is_completion=True)
                                    if oid_no and sh_no > 0:
                                        no_actual_cost = round(sh_no * no_ask, 2)
                                        matched_yes_order = round(yes_order_p * sh_no, 2)
                                        oid_yes, sh_yes, _ = exec_buy(pos.tok_yes,
                                            yes_order_p, matched_yes_order,
                                            f"🔒ARB+YES|slip|{pos.short()}",
                                            mode="aggressive", is_completion=True)
                                        if oid_yes and sh_yes > 0:
                                            # BOTH sides filled — record matched pair
                                            pos.record_buy(no_ask, no_actual_cost, actual_shares=sh_no)
                                            yes_actual_cost = round(sh_yes * yes_ask, 2)
                                            pos.record_yes_buy(yes_ask, yes_actual_cost, actual_shares=sh_yes)
                                            portfolio.total_cost += no_actual_cost + yes_actual_cost
                                            record_order()
                                            trades += 1
                                        else:
                                            # YES failed — record NO but log warning about asymmetry.
                                            # Still record because shares are in wallet, but flag it.
                                            pos.record_buy(no_ask, no_actual_cost, actual_shares=sh_no)
                                            portfolio.total_cost += no_actual_cost
                                            record_order()
                                            trades += 1
                                            log.warning(f"  ⚠️ PARTIAL FILL: {pos.short()} — NO filled {sh_no} shares "
                                                       f"but YES failed. Position has unmatched NO exposure.")

        # ── STEP R: HEDGE RECOVERY — buy cheap matched pairs to dilute losses ──
        # For hedged positions (both sides held, GP < 0), if the market combined
        # price is cheaper than our average combined, buy more pairs to lower avg.
        # SKIP hammered positions — they're intentionally uneven, recovery would waste $.
        recovery_count = 0
        for mid, pos in arb_positions:
            if not (pos.is_two_sided and not pos.is_arb_locked):
                continue  # only hedged positions (two-sided but not locked)
            _ri = abs(pos.shares - pos.yes_shares)
            if pos.total_hammered > 0 or _ri >= 5:
                continue  # hammered — uneven shares are intentional, don't recover

            no_ask = cycle_price_cache.get(pos.tok_no, (None, 0, None, 0))[0]
            yes_ask = cycle_price_cache.get(pos.tok_yes, (None, 0, None, 0))[0]
            if not no_ask or not yes_ask or no_ask < 0.01 or yes_ask < 0.01:
                continue

            market_combined = no_ask + yes_ask
            # Our current average combined cost per pair
            avg_combined = pos.avg_price + pos.yes_avg_price
            if avg_combined < 0.01:
                continue  # bad data

            improvement = avg_combined - market_combined
            if improvement < HEDGE_RECOVERY_MIN_IMPROVEMENT:
                continue  # market isn't cheap enough to help

            # Calculate budget: min of recovery cap and arb room
            room = portfolio.arb_room_for(mid, pos.group_key)
            budget = min(HEDGE_RECOVERY_MAX_SPEND, room)
            if budget < 2.0:
                continue  # not enough room

            # How many matched pairs can we buy?
            pair_cost = market_combined
            max_pairs = int(budget / pair_cost) if pair_cost > 0 else 0
            if max_pairs < 1:
                continue

            # Calculate GP before recovery — ONLY proceed if recovery improves GP
            payout = 1.0 * (1.0 - POLYMARKET_FEE_PCT)
            gp_before = min(pos.shares, pos.yes_shares) * payout - pos.total_cost
            # Simulate GP after adding max_pairs matched pairs
            new_matched = min(pos.shares + max_pairs, pos.yes_shares + max_pairs)
            new_cost = pos.total_cost + round(max_pairs * market_combined, 2)
            gp_simulated = new_matched * payout - new_cost
            if gp_simulated <= gp_before:
                continue  # recovery would make GP worse or no change — skip

            # Buy NO side
            no_spend = round(max_pairs * no_ask, 2)
            oid_no, sh_no, _ = exec_buy(pos.tok_no, no_ask, no_spend,
                f"💪REC-NO|{pos.title[:25]}",
                mode="aggressive", is_completion=True)
            if not oid_no or sh_no <= 0:
                continue

            # Buy YES side (matched to NO shares bought)
            yes_spend = round(sh_no * yes_ask, 2)
            oid_yes, sh_yes, _ = exec_buy(pos.tok_yes, yes_ask, yes_spend,
                f"💪REC-YES|{pos.title[:25]}",
                mode="aggressive", is_completion=True)

            # Record buys — only record matched pairs to avoid asymmetry
            no_actual_cost = round(sh_no * no_ask, 2)
            if oid_yes and sh_yes > 0:
                # BOTH sides filled — record matched recovery pair
                pos.record_buy(no_ask, no_actual_cost, actual_shares=sh_no)
                yes_actual_cost = round(sh_yes * yes_ask, 2)
                pos.record_yes_buy(yes_ask, yes_actual_cost, actual_shares=sh_yes)
                portfolio.total_cost += no_actual_cost + yes_actual_cost
                total_spent = no_actual_cost + yes_actual_cost
            else:
                # YES failed — still record NO (shares are in wallet) but warn
                pos.record_buy(no_ask, no_actual_cost, actual_shares=sh_no)
                portfolio.total_cost += no_actual_cost
                total_spent = no_actual_cost
                log.warning(f"  ⚠️ RECOVERY PARTIAL: {pos.short()} — NO filled {sh_no} but YES failed. "
                           f"Unmatched NO exposure added.")

            pos.total_recovered = getattr(pos, 'total_recovered', 0) + total_spent
            pos.last_recovery = time.time()

            # Calculate GP after recovery
            gp_after = min(pos.shares, pos.yes_shares) * payout - pos.total_cost
            new_avg = pos.avg_price + pos.yes_avg_price

            status = "→ LOCKED!" if pos.is_arb_locked else f"GP now ${gp_after:.2f}"
            log.info(f"  💪 RECOVERY: {pos.title[:40]} avg={avg_combined:.3f}→mkt={market_combined:.3f} "
                    f"+{sh_no:.0f}pairs ${total_spent:.2f} | GP ${gp_before:.2f}→{gp_after:.2f} {status}")
            record_game_action("recovery", {
                "mid": mid, "title": pos.title[:50],
                "avg_combined": round(avg_combined, 4), "mkt_combined": round(market_combined, 4),
                "pairs": sh_no, "spent": total_spent,
                "gp_before": round(gp_before, 2), "gp_after": round(gp_after, 2),
                "now_locked": pos.is_arb_locked,
            })
            recovery_count += 1
            trades += 1

        if recovery_count:
            log.info(f"  💪 HEDGE RECOVERY: {recovery_count} positions improved this cycle")

    # ═══ PHASE 2: SCAN ALL MARKETS FOR NEW ARBS ═══
    new_markets = []  # will be reused in Phase 3 accumulation
    has_yes_count = 0
    in_portfolio_count = 0
    wallet_blocked = 0
    if not all_markets:
        # Skip Phase 2, but still run Phase 3 for accumulation completion
        pass
    else:
        # Only markets we're NOT in and that have YES tokens
        # Skip markets we're already in (includes wallet positions the bot must not touch)
        wallet_tokens = set()
        for p in portfolio.positions.values():
            if p.from_wallet:
                wallet_tokens.add(p.tok_no)
                wallet_tokens.add(p.tok_yes)

        has_yes_count = sum(1 for m in all_markets if m.get("tok_yes"))
        in_portfolio_count = sum(1 for m in all_markets if portfolio.has(m["mid"]))
        wallet_blocked = sum(1 for m in all_markets if m["tok_no"] in wallet_tokens or m.get("tok_yes") in wallet_tokens)

        new_markets = [m for m in all_markets
                       if not portfolio.has(m["mid"]) and m.get("tok_yes")
                       and m["tok_no"] not in wallet_tokens
                       and m.get("tok_yes") not in wallet_tokens]

    # ─── DEBUG: periodic scan diagnostics (every ~10s = ~200 cycles) ───
    global _p2_debug_counter
    _p2_debug_counter += 1
    _p2_show_debug = (_p2_debug_counter % 5 == 0)  # every 5 cycles

    if _p2_show_debug and all_markets:
        log.info(f"  📊 DIAG: {len(all_markets)} total markets | "
                 f"{has_yes_count} have YES tok | {in_portfolio_count} in portfolio | "
                 f"{wallet_blocked} wallet-blocked | {len(new_markets)} scannable")

    # Batch fetch new market prices (skip tokens already in cycle cache from Phase 1)
    scan_tokens = []
    for m in new_markets:
        if m["tok_no"] not in cycle_price_cache:
            scan_tokens.append(m["tok_no"])
        if m["tok_yes"] not in cycle_price_cache:
            scan_tokens.append(m["tok_yes"])
    if scan_tokens:
        fresh = smart_batch_fetch(scan_tokens)
        cycle_price_cache.update(fresh)
    scan_cache = cycle_price_cache  # use the unified cache

    # ─── FEED LIVE TRACKER from HTTP prices (not just WS) ───
    # WS may be flaky/broken — this ensures tracker gets data every scan cycle
    # Feed ALL markets (including portfolio positions) so tracker has full picture
    if LIVE_ENABLED:
        _tracker_fed = 0
        for m in all_markets:
            if not m.get("tok_yes"): continue
            no_ask_t, no_depth_t, _, _ = scan_cache.get(m["tok_no"], (None, 0, None, 0))
            yes_ask_t, yes_depth_t, _, _ = scan_cache.get(m["tok_yes"], (None, 0, None, 0))
            mid_t = m["mid"]
            if no_ask_t and no_ask_t > 0.001:
                _update_tracker(mid_t, "NO", no_ask_t, no_depth_t, m)
                _tracker_fed += 1
            if yes_ask_t and yes_ask_t > 0.001:
                _update_tracker(mid_t, "YES", yes_ask_t, yes_depth_t, m)
                _tracker_fed += 1
        if _p2_show_debug:
            log.info(f"  📡 Tracker fed {_tracker_fed} price updates from {len(all_markets)} markets")

    # Find arb opportunities — check BOTH best-ask AND full-book depth.
    # Best-ask catches obvious arbs. Full-book catches hidden arbs where
    # best asks sum to >0.98 but deeper levels have profitable pairs.
    # NEAR_ARB_THRESHOLD: fetch full books for markets within 5% of arb
    NEAR_ARB_THRESHOLD = 1.05  # combined < 1.05 → worth checking deeper levels

    arbs = []          # definite arbs at best ask
    near_arbs_mids = []  # near-arbs that need full book scan

    # DEBUG: track closest combined prices
    _debug_all_combined = []
    _debug_no_price_count = 0

    for m in new_markets:
        no_ask, _, no_bid, _ = scan_cache.get(m["tok_no"], (None, 0, None, 0))
        yes_ask, _, _, _ = scan_cache.get(m["tok_yes"], (None, 0, None, 0))
        if not no_ask or not yes_ask or yes_ask <= 0.001:
            _debug_no_price_count += 1
            continue
        combined = no_ask + yes_ask
        spread_pct = 1.0 - combined
        _debug_all_combined.append((combined, m["title"][:40], no_ask, yes_ask))
        if combined <= ARB_LOCK_THRESHOLD and spread_pct >= ARB_LOCK_MIN_SPREAD:
            arbs.append((combined, m, no_ask, no_bid, yes_ask, spread_pct))
        elif combined < NEAR_ARB_THRESHOLD:
            # Near-arb: deeper book levels might have profitable pairs
            near_arbs_mids.append(m)

    # DEBUG: log top 5 closest combined prices every ~10s
    if _p2_show_debug and _debug_all_combined:
        _debug_all_combined.sort(key=lambda x: x[0])
        log.info(f"  📊 DIAG: {len(_debug_all_combined)} priced markets, "
                 f"{_debug_no_price_count} missing prices, "
                 f"{len(arbs)} arbs, {len(near_arbs_mids)} near-arbs")
        log.info(f"  📊 TOP 5 closest combined (threshold={ARB_LOCK_THRESHOLD}):")
        for c, title, na, ya in _debug_all_combined[:5]:
            gap = c - ARB_LOCK_THRESHOLD
            status = "✅ ARB" if c <= ARB_LOCK_THRESHOLD else f"❌ +{gap:.3f} over"
            log.info(f"    {status} {title} NO@{na:.3f}+YES@{ya:.3f}={c:.3f}")
    elif _p2_show_debug and not _debug_all_combined:
        log.info(f"  📊 DIAG: 0 priced markets out of {len(new_markets)} scannable! "
                 f"{_debug_no_price_count} had no prices")

    # Sort by best margin first
    arbs.sort(key=lambda x: x[0])

    if arbs:
        log.info(f"  🔍 Phase 2: {len(new_markets)} markets scanned → {len(arbs)} arbs, "
                 f"{len(near_arbs_mids)} near-arbs")
        for c, m, na, nb, ya, sp in arbs[:3]:
            log.info(f"    ✅ {m['title'][:40]} NO@{na:.3f}+YES@{ya:.3f}={c:.3f} ({sp*100:.1f}%)")
    elif near_arbs_mids:
        log.info(f"  🔍 Phase 2: {len(new_markets)} markets → 0 arbs, {len(near_arbs_mids)} near-arbs (checking depth)")

    # ─── STEP 1: INSTANT ENTRY AT BEST-ASK (no full book fetch!) ───
    # Like v4: fire both sides at best-ask price IMMEDIATELY.
    # Full book walk for deeper levels happens in Phase 1 next cycle.
    # This cuts 2-5 seconds of latency from the critical path.
    for combined, m, no_ask, no_bid, yes_ask, spread_pct in arbs:
        # ─── GLOBAL PENDING CAP — no new positions if slots full ───
        if _pending_slots_full():
            log.info(f"  🛑 PENDING FULL — skipping Phase 2 arbs")
            break
        if not rate_ok(is_new=True):
            log.info(f"  ⚠ RATE LIMIT — skipping arb {m['title'][:30]}")
            break

        mid = m["mid"]
        # Skip if WS Phase 0 already got this one
        if portfolio.has(mid):
            continue
        if mid in _burned_markets:
            continue  # trimmed/stale-exited — don't re-enter
        # O/U & SPREAD LOPSIDED GUARD: skip if one side > 65¢ (game already shifted)
        if m.get("typ", "") in ("ou", "spread") and max(no_ask, yes_ask) > 0.65:
            continue

        arb_room = portfolio.arb_room_for(mid, m["group"])
        if arb_room < 2:
            log.info(f"  ⚠ NO ROOM (${arb_room:.2f}) — skipping {m['title'][:30]}")
            continue

        # ─── FRESH VALIDATION: re-fetch books RIGHT NOW ───
        is_valid, fresh_no, fresh_yes, no_book, yes_book, reason = \
            validate_arb_prices_fresh(m)
        if not is_valid:
            log.info(f"  ⏭️ FRESH CHECK FAIL: {reason} | {m['title'][:35]}")
            continue
        # Use fresh prices (may differ from stale scan)
        no_ask, yes_ask = fresh_no, fresh_yes
        combined = no_ask + yes_ask
        spread_pct = 1.0 - combined

        log.info(f"  🔒 INSTANT ARB: {m['title'][:40]} "
                f"NO@{no_ask:.3f}+YES@{yes_ask:.3f}={combined:.3f} ({spread_pct*100:.1f}%) [FRESH ✓]")

        # Cap first entry at ARB_INITIAL_MAX per side — confirm fills before scaling up
        budget_per_side = min(arb_room / 2, ARB_INITIAL_MAX)
        pair_cost = no_ask + yes_ask
        max_pairs = budget_per_side / max(no_ask, yes_ask) if pair_cost > 0 else 0
        target_shares = math.floor(max_pairs * 100) / 100
        if target_shares < 5:
            log.info(f"  ⚠ TOO FEW SHARES ({target_shares:.0f}) — skipping {m['title'][:30]}")
            continue

        # ─── DEPTH CHECK: verify book has shares at profitable prices ───
        no_avail, no_avg_fill = validate_book_depth(no_book, target_shares, no_ask + ARB_DYNAMIC_SLIP_MAX)
        yes_avail, yes_avg_fill = validate_book_depth(yes_book, target_shares, yes_ask + ARB_DYNAMIC_SLIP_MAX)
        if no_avail < ARB_MIN_DEPTH_SHARES or yes_avail < ARB_MIN_DEPTH_SHARES:
            log.info(f"  ⏭️ DEPTH FAIL: NO={no_avail:.0f}sh YES={yes_avail:.0f}sh "
                    f"(need {ARB_MIN_DEPTH_SHARES}) | {m['title'][:35]}")
            continue
        # Reduce target to what BOTH sides can fill equally
        target_shares = min(target_shares, no_avail, yes_avail)
        target_shares = math.floor(target_shares * 100) / 100
        if target_shares < 5:
            continue

        # ─── DYNAMIC LIMIT PRICING: adapt to book structure ───
        no_order_p, _ = calculate_dynamic_limit(no_book, target_shares, yes_ask)
        yes_order_p, _ = calculate_dynamic_limit(yes_book, target_shares, no_ask)
        if no_order_p <= 0 or yes_order_p <= 0:
            log.info(f"  ⏭️ PRICE CALC FAIL: no_lim={no_order_p:.3f} yes_lim={yes_order_p:.3f} | {m['title'][:35]}")
            continue
        # Final safety: combined order prices must be profitable
        if no_order_p + yes_order_p > 0.99:
            log.info(f"  ⏭️ COMBINED LIMIT TOO HIGH: {no_order_p:.3f}+{yes_order_p:.3f}="
                    f"{no_order_p+yes_order_p:.3f} | {m['title'][:35]}")
            continue

        no_cost = round(no_order_p * target_shares, 2)
        yes_cost = round(yes_order_p * target_shares, 2)

        # Pass MATCHED target_shares so both sides buy EXACTLY the same number of shares
        batch_list = [
            (m["tok_no"], no_order_p, no_cost, f"🔒NEW-NO|@{no_ask:.3f}|{spread_pct*100:.1f}%|{m['title'][:20]}", target_shares),
            (m["tok_yes"], yes_order_p, yes_cost, f"🔒NEW-YES|@{yes_ask:.3f}|{spread_pct*100:.1f}%|{m['title'][:20]}", target_shares),
        ]

        results = exec_batch_buy(batch_list, label_prefix=f"NEW-ARB|{m['title'][:20]}")

        pos = Position(mid, m["title"], m["typ"], m["tok_no"],
                     m["tok_yes"], m["group"])
        total_spent = 0
        cycle_trades = 0
        pending_orders = 0
        for i, (oid, sh, ap) in enumerate(results):
            if not oid or oid == "NO_BALANCE":
                continue
            real_price = no_ask if i == 0 else yes_ask
            if sh > 0:
                actual_cost = round(sh * real_price, 2)
                if i == 0:
                    pos.record_buy(real_price, actual_cost, actual_shares=sh)
                else:
                    pos.record_yes_buy(real_price, actual_cost, actual_shares=sh)
                total_spent += actual_cost
                record_order(is_new=True if cycle_trades == 0 else False)
                cycle_trades += 1
                trades += 1
            else:
                pending_orders += 1
                log.info(f"  ⏳ PENDING {'NO' if i==0 else 'YES'} @{real_price:.3f} "
                        f"[awaiting fill] | {m['title'][:30]}")

        if total_spent > 0 or pending_orders > 0:
            if pos.shares > 0 and pos.yes_shares > 0:
                payout_per_share = 1.0 * (1.0 - POLYMARKET_FEE_PCT)
                guaranteed = min(pos.shares, pos.yes_shares) * payout_per_share - pos.total_cost
                log.info(f"  🔒 NEW ARB! {m['title'][:45]}")
                log.info(f"     {cycle_trades} confirmed, {pending_orders} pending, "
                        f"${total_spent:.2f} spent → ${guaranteed:.2f} guaranteed ({spread_pct*100:.1f}%)")
            else:
                filled_side = "NO" if pos.shares > 0 else ("YES" if pos.yes_shares > 0 else "none")
                # Mark as accumulation so completion paths (Step A + Path A) can finish it
                pos.is_accumulation = True
                pos.accum_side = filled_side if filled_side != "none" else "NO"
                log.info(f"  ⏳ PARTIAL ARB: {m['title'][:45]}")
                log.info(f"     {cycle_trades} fills ({filled_side} side), "
                        f"{pending_orders} pending | ${total_spent:.2f} spent "
                        f"[marked accum {pos.accum_side} for completion]")
            pos.current_ask = no_ask
            pos.current_bid = no_bid
            if no_bid and no_ask > 0:
                pos.spread_at_entry = (no_ask - no_bid) / no_ask
            portfolio.add_position(pos)
            _governor_position_added()
            portfolio.n_entries += 1
            if pos.is_arb_locked:
                portfolio.n_arb_locks += 1
            portfolio.total_cost += total_spent
            # Subscribe new tokens to WS for instant prices next cycle
            ws_start([m["tok_no"], m["tok_yes"]])
            _ws_token_to_market[m["tok_no"]] = m
            _ws_token_to_market[m["tok_yes"]] = m
    
    # ─── STEP 2: CHECK NEAR-ARBS FOR HIDDEN DEPTH ARBS (after executing confirmed arbs) ───
    if near_arbs_mids and not _pending_slots_full() and rate_ok(is_new=True):
        near_tokens = []
        for m in near_arbs_mids[:5]:  # cap to 5 — 20 was causing API stalls
            near_tokens.append(m["tok_no"])
            near_tokens.append(m["tok_yes"])
        near_full_cache = batch_fetch_full_books(near_tokens) if near_tokens else {}

        for m in near_arbs_mids[:5]:
            no_full = near_full_cache.get(m["tok_no"])
            yes_full = near_full_cache.get(m["tok_yes"])
            if not no_full or not yes_full: continue
            no_asks_deep = no_full[0]
            yes_asks_deep = yes_full[0]
            if not no_asks_deep or not yes_asks_deep: continue

            for no_p, no_s in no_asks_deep[:5]:
                for yes_p, yes_s in yes_asks_deep[:5]:
                    deep_combined = no_p + yes_p
                    if deep_combined <= ARB_LOCK_THRESHOLD:
                        deep_spread = 1.0 - deep_combined
                        if deep_spread >= ARB_LOCK_MIN_SPREAD:
                            log.info(f"  🔬 DEEP ARB: {m['title'][:40]} "
                                     f"NO@{no_p:.3f}+YES@{yes_p:.3f}={deep_combined:.3f} "
                                     f"({deep_spread*100:.1f}%) [not visible at best ask]")
                            # Execute immediately — don't just append
                            mid = m["mid"]
                            if portfolio.has(mid): break
                            if _pending_slots_full(): break
                            room = portfolio.arb_room_for(mid, m["group"])
                            if room < 2: break
                            # ─── FRESH VALIDATION for near-arb ───
                            is_valid_deep, fn, fy, nb_fresh, yb_fresh, dr = \
                                validate_arb_prices_fresh(m)
                            if not is_valid_deep:
                                log.info(f"  ⏭️ DEEP FRESH FAIL: {dr} | {m['title'][:35]}")
                                break
                            # Use fresh books if we got them
                            if nb_fresh:
                                no_asks_deep = nb_fresh
                            if yb_fresh:
                                yes_asks_deep = yb_fresh
                            no_bid = no_full[4]
                            pos = Position(mid, m["title"], m["typ"], m["tok_no"],
                                         m["tok_yes"], m["group"])
                            walk_orders, walk_gp, walk_cost = walk_book_arb(
                                pos, no_asks_deep, yes_asks_deep, room)
                            if walk_orders and walk_gp > 0.10:
                                results = exec_batch_buy(
                                    [(m["tok_no"] if s == 'NO' else m["tok_yes"], p,
                                      round(p * sh, 2),
                                      f"🔬DEEP-{s}|{m['title'][:20]}", sh)
                                     for s, p, c, sh in walk_orders])
                                total_spent = 0
                                for i, (oid, sh, ap) in enumerate(results):
                                    if oid and sh > 0:
                                        side = walk_orders[i][0]
                                        real_p = round(walk_orders[i][2] / walk_orders[i][3], 4) if walk_orders[i][3] > 0 else walk_orders[i][1]
                                        cost = round(sh * real_p, 2)
                                        if side == 'NO':
                                            pos.record_buy(real_p, cost, actual_shares=sh)
                                        else:
                                            pos.record_yes_buy(real_p, cost, actual_shares=sh)
                                        total_spent += cost
                                        trades += 1
                                if total_spent > 0:
                                    # If only one side filled, mark as accumulation for completion
                                    if not pos.is_arb_locked:
                                        pos.is_accumulation = True
                                        pos.accum_side = "NO" if pos.shares > 0 and pos.yes_shares == 0 else "YES"
                                    portfolio.add_position(pos)
                                    _governor_position_added()
                                    portfolio.n_entries += 1
                                    portfolio.total_cost += total_spent
                                    if pos.is_arb_locked:
                                        portfolio.n_arb_locks += 1
                                    ws_start([m["tok_no"], m["tok_yes"]])
                                    record_order(is_new=True)
                            break
                else:
                    continue
                break

    # ═══ PHASE 3: INDEPENDENT SIDE ACCUMULATION (RN1's core edge) ═══
    # ═══ PHASE 3: LIVE EVENT ACCUMULATION (RN1-style) ═══
    # Buy BOTH sides of live binary markets during their respective dips.
    # In live sports, volatility guarantees both sides will dip at different times.
    # Track session lows via WS → buy when side dips near its low → combined < $1.00 = arb.
    accum_trades = 0
    if LIVE_ENABLED and not args_scan_mode:
        now_live = time.time()

        # ─── STEP A⁰: DEAD POSITION CLEANUP — game decided, free the slot ───
        # If the side we hold is trading below LIVE_DEAD_THRESHOLD, the game
        # is over (one side went to ~$0, other to ~$1). No arb possible. Mark dead.
        for mid, pos in list(portfolio.positions.items()):
            if not pos.is_accumulation or pos.is_arb_locked:
                continue
            no_ask_c, _, no_bid_c, _ = cycle_price_cache.get(pos.tok_no, (None, 0, None, 0))
            yes_ask_c, _, yes_bid_c, _ = cycle_price_cache.get(pos.tok_yes, (None, 0, None, 0))

            # Which side do we hold?
            held_bid = None
            held_side = ""
            if pos.accum_side == "NO" and pos.shares > 0:
                held_bid = no_bid_c
                held_side = "NO"
            elif pos.accum_side == "YES" and pos.yes_shares > 0:
                held_bid = yes_bid_c
                held_side = "YES"

            # Check the OTHER side too — if opponent ask >= 0.98, game is decided
            if pos.accum_side == "NO":
                other_ask = yes_ask_c  # if YES is at $0.99, NO lost
            else:
                other_ask = no_ask_c   # if NO is at $0.99, YES lost

            is_dead = False
            if held_bid is not None and held_bid < LIVE_DEAD_THRESHOLD:
                is_dead = True
                reason_str = f"{held_side} bid={held_bid:.3f} < {LIVE_DEAD_THRESHOLD}"
            elif other_ask is not None and other_ask >= 0.98:
                is_dead = True
                reason_str = f"other side ask={other_ask:.3f} >= 0.98 — game decided"
            elif held_side and held_bid is None and (other_ask is None or other_ask >= 0.98):
                # No price data for our side + other side at/near $1 or also gone
                age = now_live - pos.created
                if age > 120:  # 2 min is enough for wallet-synced positions
                    is_dead = True
                    reason_str = f"{held_side} no bid data after {age/60:.0f}min"

            if is_dead:
                # Game is decided. Our side lost. Mark as dead, free the pending slot.
                pos.is_accumulation = False
                pos.active = False
                log.info(f"  💀 DEAD POSITION: {pos.title[:40]} "
                        f"{reason_str} — game decided, freeing slot")

        # ─── STEP W: WALLET POSITION ARB SCANNER ───
        # Scan user's wallet positions for arb completion opportunities.
        # The bot won't SELL wallet positions, but it WILL buy the other side
        # if it can lock in profit. This protects the user's manual bets.
        # Example: user bought Red Wings NO at 57¢. Later Golden Knights YES
        # dips to 35¢. Bot buys YES → locked arb at 92¢ combined = 8% guaranteed.
        #
        # PRICE FETCH: wallet positions are excluded from Phase 1 price fetching,
        # so we need to fetch their prices here before checking arb opportunities.
        # IMPORTANT: Fetch ALL wallet positions (including locked) so hammer can see
        # current_bid and the live tracker gets fed for momentum checks.
        all_wallet_positions = [(mid, pos) for mid, pos in portfolio.positions.items()
                               if pos.from_wallet and pos.tok_yes and pos.tok_no]
        if all_wallet_positions:
            wallet_price_tokens = []
            for mid, pos in all_wallet_positions:
                if pos.tok_no not in cycle_price_cache:
                    wallet_price_tokens.append(pos.tok_no)
                if pos.tok_yes not in cycle_price_cache:
                    wallet_price_tokens.append(pos.tok_yes)
            if wallet_price_tokens:
                wallet_prices = smart_batch_fetch(wallet_price_tokens)
                cycle_price_cache.update(wallet_prices)

            # Update current prices on ALL wallet positions (including locked)
            # so hammer Step H can see current_bid/current_yes_bid
            for mid, pos in all_wallet_positions:
                no_data = cycle_price_cache.get(pos.tok_no, (None, 0, None, 0))
                yes_data = cycle_price_cache.get(pos.tok_yes, (None, 0, None, 0))
                if no_data[0]:
                    pos.current_ask = no_data[0]
                if no_data[2]:
                    pos.current_bid = no_data[2]
                if yes_data[0]:
                    pos.current_yes_ask = yes_data[0]
                if yes_data[2]:
                    pos.current_yes_bid = yes_data[2]

            # Feed live tracker for wallet positions — they're excluded from the
            # Phase 2 tracker feed (their prices aren't in cache at that point).
            # Without this, hammer can't get momentum data for wallet positions.
            if LIVE_ENABLED:
                for mid, pos in all_wallet_positions:
                    no_data = cycle_price_cache.get(pos.tok_no, (None, 0, None, 0))
                    yes_data = cycle_price_cache.get(pos.tok_yes, (None, 0, None, 0))
                    if no_data[0] and no_data[0] > 0.001:
                        _update_tracker(mid, "NO", no_data[0], no_data[1], None)
                    if yes_data[0] and yes_data[0] > 0.001:
                        _update_tracker(mid, "YES", yes_data[0], yes_data[1], None)

        for mid, pos in list(portfolio.positions.items()):
            if not pos.from_wallet:
                continue  # only scan wallet positions here
            if pos.is_arb_locked:
                continue  # already locked
            if not pos.tok_yes or not pos.tok_no:
                continue
            # SKIP HAMMERED — the share imbalance is INTENTIONAL.
            # Buying the cheap losing side undoes the hammer strategy.
            if pos.total_hammered > 0 or getattr(pos, 'total_trimmed', 0) > 0:
                continue

            no_ask_c, _, no_bid_c, _ = cycle_price_cache.get(pos.tok_no, (None, 0, None, 0))
            yes_ask_c, _, yes_bid_c, _ = cycle_price_cache.get(pos.tok_yes, (None, 0, None, 0))
            if not no_ask_c or not yes_ask_c:
                continue

            # Update current prices on the wallet position
            pos.current_ask = no_ask_c
            pos.current_bid = no_bid_c
            pos.current_yes_ask = yes_ask_c
            pos.current_yes_bid = yes_bid_c

            # Determine which side we hold and what the other side costs
            # GUARD: don't buy sides < 10¢ — game is near-resolved, not a real arb
            if no_ask_c < LIVE_MIN_PRICE or yes_ask_c < LIVE_MIN_PRICE:
                continue

            # Case 1: Have NO shares, check if YES is cheap enough
            if pos.shares > 0 and pos.yes_shares <= 0:
                combined = pos.avg_price + yes_ask_c
                if combined <= LIVE_COMPLETE_THRESHOLD:
                    yes_order_p = min(round(yes_ask_c + ARB_SLIPPAGE, 2), 0.99)
                    target = pos.shares  # match our shares
                    yes_order_cost = round(yes_order_p * target, 2)
                    if target >= 5 and rate_ok():
                        label = f"🛡️WALLET-ARB-YES|{combined:.3f}|{pos.title[:25]}"
                        oid, sh, _ = exec_buy(pos.tok_yes, yes_order_p, yes_order_cost,
                                            label, mode="aggressive", is_completion=True)
                        if oid and sh > 0:
                            actual_cost = round(sh * yes_ask_c, 2)
                            pos.record_yes_buy(yes_ask_c, actual_cost, actual_shares=sh)
                            portfolio.total_cost += actual_cost
                            if pos.is_arb_locked:
                                portfolio.n_arb_locks += 1
                                gp = pos.guaranteed_profit or 0
                                log.info(f"  🛡️ WALLET→ARB! {pos.title[:40]} "
                                        f"NO@{pos.avg_price:.3f}+YES@{yes_ask_c:.3f}="
                                        f"{combined:.3f} → ${gp:.2f} guaranteed!")
                            record_order()
                            trades += 1

            # Case 2: Have YES shares, check if NO is cheap enough
            elif pos.yes_shares > 0 and pos.shares <= 0:
                combined = no_ask_c + pos.yes_avg_price
                if combined <= LIVE_COMPLETE_THRESHOLD:
                    no_order_p = min(round(no_ask_c + ARB_SLIPPAGE, 2), 0.99)
                    target = pos.yes_shares  # match our shares
                    no_order_cost = round(no_order_p * target, 2)
                    if target >= 5 and rate_ok():
                        label = f"🛡️WALLET-ARB-NO|{combined:.3f}|{pos.title[:25]}"
                        oid, sh, _ = exec_buy(pos.tok_no, no_order_p, no_order_cost,
                                            label, mode="aggressive", is_completion=True)
                        if oid and sh > 0:
                            actual_cost = round(sh * no_ask_c, 2)
                            pos.record_buy(no_ask_c, actual_cost, actual_shares=sh)
                            portfolio.total_cost += actual_cost
                            if pos.is_arb_locked:
                                portfolio.n_arb_locks += 1
                                gp = pos.guaranteed_profit or 0
                                log.info(f"  🛡️ WALLET→ARB! {pos.title[:40]} "
                                        f"NO@{no_ask_c:.3f}+YES@{pos.yes_avg_price:.3f}="
                                        f"{combined:.3f} → ${gp:.2f} guaranteed!")
                            record_order()
                            trades += 1

            # Case 3: Already have BOTH sides but not enough to lock
            # (e.g. user has 40 NO shares but only 10 YES — top up YES)
            # GUARD: only top up ONCE — after filling, mark position to prevent re-buy.
            # Without this, wallet sync corrections create phantom gaps each cycle.
            elif pos.shares > 0 and pos.yes_shares > 0 and not pos.is_arb_locked:
                # Skip if we already topped up this position (check via last_hammer as a flag)
                if getattr(pos, '_wallet_topped_up', False):
                    continue
                # Don't top up a side that's already collapsing — throwing money away.
                # If the short side's ask is below TRIM_THRESHOLD, it's a loser.
                if pos.shares > pos.yes_shares and yes_ask_c < TRIM_THRESHOLD:
                    continue
                if pos.yes_shares > pos.shares and no_ask_c < TRIM_THRESHOLD:
                    continue
                # Need more of whichever side is short
                if pos.shares > pos.yes_shares:
                    # Need more YES
                    gap = pos.shares - pos.yes_shares
                    combined = pos.avg_price + yes_ask_c
                    if combined <= LIVE_COMPLETE_THRESHOLD and gap >= 5 and rate_ok():
                        yes_order_p = min(round(yes_ask_c + ARB_SLIPPAGE, 2), 0.99)
                        yes_order_cost = round(yes_order_p * gap, 2)
                        label = f"🛡️WALLET-TOPUP-YES|{combined:.3f}|{pos.title[:25]}"
                        oid, sh, _ = exec_buy(pos.tok_yes, yes_order_p, yes_order_cost,
                                            label, mode="aggressive", is_completion=True)
                        pos._wallet_topped_up = True  # prevent re-buy regardless of fill
                        if oid and sh > 0:
                            actual_cost = round(sh * yes_ask_c, 2)
                            pos.record_yes_buy(yes_ask_c, actual_cost, actual_shares=sh)
                            portfolio.total_cost += actual_cost
                            if pos.is_arb_locked:
                                portfolio.n_arb_locks += 1
                                gp = pos.guaranteed_profit or 0
                                log.info(f"  🛡️ WALLET→ARB! {pos.title[:40]} topped up YES "
                                        f"→ ${gp:.2f} guaranteed!")
                            record_order()
                            trades += 1
                elif pos.yes_shares > pos.shares:
                    # Need more NO
                    gap = pos.yes_shares - pos.shares
                    combined = no_ask_c + pos.yes_avg_price
                    if combined <= LIVE_COMPLETE_THRESHOLD and gap >= 5 and rate_ok():
                        no_order_p = min(round(no_ask_c + ARB_SLIPPAGE, 2), 0.99)
                        no_order_cost = round(no_order_p * gap, 2)
                        label = f"🛡️WALLET-TOPUP-NO|{combined:.3f}|{pos.title[:25]}"
                        oid, sh, _ = exec_buy(pos.tok_no, no_order_p, no_order_cost,
                                            label, mode="aggressive", is_completion=True)
                        pos._wallet_topped_up = True  # prevent re-buy regardless of fill
                        if oid and sh > 0:
                            actual_cost = round(sh * no_ask_c, 2)
                            pos.record_buy(no_ask_c, actual_cost, actual_shares=sh)
                            portfolio.total_cost += actual_cost
                            if pos.is_arb_locked:
                                portfolio.n_arb_locks += 1
                                gp = pos.guaranteed_profit or 0
                                log.info(f"  🛡️ WALLET→ARB! {pos.title[:40]} topped up NO "
                                        f"→ ${gp:.2f} guaranteed!")
                            record_order()
                            trades += 1

        # ─── STEP A: Complete existing accumulation positions ───
        # ESCALATING COMPLETION: the longer we wait, the more we accept.
        # 0-5 min:  complete at 0.95 (ideal, 5%+ profit)
        # 5-7 min:  complete at 0.98 (still profitable, decent margin)
        # 7-10 min: complete at 1.01 (tiny loss ~1% but position is CLOSED)
        # This is the RN1 approach: always complete, never get stuck one-sided.
        for mid, pos in list(portfolio.positions.items()):
            if not pos.is_accumulation or pos.is_arb_locked:
                continue
            if not pos.tok_yes:
                continue
            # Skip triad legs — they complete via Step T (NO on all 3 legs),
            # NOT via binary completion (buying the opposite YES/NO side)
            if pos.triad_group:
                continue

            no_ask_c, _, no_bid_c, _ = cycle_price_cache.get(pos.tok_no, (None, 0, None, 0))
            yes_ask_c, _, yes_bid_c, _ = cycle_price_cache.get(pos.tok_yes, (None, 0, None, 0))
            if not no_ask_c or not yes_ask_c:
                continue

            pos.current_ask = no_ask_c
            pos.current_bid = no_bid_c
            pos.current_yes_ask = yes_ask_c
            pos.current_yes_bid = yes_bid_c

            # Escalating threshold based on position age + SPORT-SPECIFIC TIMING
            # Each sport has different volatility: baseball swings per inning (~15 min),
            # soccer swings on goals (rare), basketball swings constantly.
            pos_age = now_live - pos.created
            held_shares = pos.shares if pos.accum_side == "NO" else pos.yes_shares
            held_shares = max(held_shares, 1)  # avoid div by zero

            # Sport-specific timing lookup
            _sport = getattr(pos, 'sport', '') or 'other'
            _timing = SPORT_COMPLETION_TIMING.get(_sport, SPORT_COMPLETION_TIMING["other"])
            _tight_secs, _tight_thresh, _med_secs, _med_thresh, _max_loss = _timing

            max_loss_per_share = _max_loss / held_shares
            if pos_age < _tight_secs:       # tight window: only complete at ideal profit
                complete_thresh = _tight_thresh
            elif pos_age < _med_secs:        # medium window: accept smaller margin
                complete_thresh = _med_thresh
            else:                            # past medium: accept losses to hedge
                complete_thresh = min(1.0 + max_loss_per_share, 1.25)

            # O/U & SPREAD HARD CAP: never complete above 0.98.
            # These markets swing hard on scoring — the "losing" side can recover.
            # Completing at combined > 1.00 locks in a guaranteed loss.
            # Better to let it ride or trim than overpay for the other side.
            _pos_typ = getattr(pos, 'typ', '')
            if _pos_typ in ("ou", "spread"):
                complete_thresh = min(complete_thresh, 0.98)

            # ─── SMART COMPLETION: momentum-aware + let-it-ride ───
            # Get momentum data for this market from the live tracker
            completion_side_price = None
            completion_side_vel = 0.0
            t_comp = None
            with _live_lock:
                t_comp = _live_tracker.get(mid)
            if t_comp:
                _, _, comp_vel, comp_falling = _get_momentum_score(t_comp, now_live)
            else:
                comp_vel = 0.0
                comp_falling = None

            # Determine the completion side's current price
            if pos.accum_side == "NO" and pos.shares > 0:
                completion_side_price = yes_ask_c   # need YES
                completion_side_name = "YES"
            elif pos.accum_side == "YES" and pos.yes_shares > 0:
                completion_side_price = no_ask_c    # need NO
                completion_side_name = "NO"
            else:
                completion_side_price = None
                completion_side_name = None

            if completion_side_price:
                # ─── LET IT RIDE: DISABLED ───
                # Previously skipped completion when 2nd side < 15¢ ("winner dominant").
                # PROBLEM: converts hedged arb into naked directional bet. When the
                # "dominant" side reverses (Avalanche, Canadiens), you're exposed with
                # zero protection. Always complete the arb — guaranteed profit > EV gamble.

                # ─── STILL FALLING: don't catch a falling knife ───
                # If the completion side has strong negative momentum, WAIT.
                # The price is dropping toward us — buy it cheaper in the next cycle.
                # But respect the patience max — after 7 min, complete no matter what.
                if (comp_vel < COMPLETION_STILL_FALLING_VEL
                    and pos_age < COMPLETION_PATIENCE_MAX_SECS):
                    if _p2_debug_counter % 10 == 0:
                        log.info(f"  ⏳ FALLING KNIFE: {pos.short()} — {completion_side_name}@{completion_side_price:.3f} "
                                f"vel={comp_vel:.4f} still dropping, waiting for floor")
                    continue  # price still falling — wait for it to bottom

            # Try to complete arb by buying the OTHER side
            # GUARD: never complete by buying the side we just trimmed
            _trimmed = getattr(pos, 'trimmed_side', '')
            if pos.accum_side == "NO" and pos.shares > 0:
                # Have NO, need YES to complete
                if _trimmed == "YES":
                    continue  # trimmed YES side — don't buy it back
                combined_c = pos.avg_price + yes_ask_c
                if combined_c <= complete_thresh:
                    room = portfolio.arb_room_for(mid, pos.group_key)
                    yes_order_p = min(round(yes_ask_c + ARB_SLIPPAGE, 2), 0.99)
                    target = min(pos.shares - pos.yes_shares, room / yes_ask_c if yes_ask_c > 0 else 0)
                    if target >= 5 and rate_ok():
                        yes_order_cost = round(yes_order_p * target, 2)
                        patience_tag = ""
                        if comp_vel < -0.0003:
                            patience_tag = f"🎯FLOOR "  # momentum slowed enough to buy
                        label = f"{patience_tag}🎯LIVE-COMPLETE-YES|{combined_c:.3f}|{_sport}|{pos.short()}"
                        oid, sh, fill_p = exec_buy(pos.tok_yes, yes_order_p, yes_order_cost,
                                            label, mode="aggressive", is_completion=True)
                        if oid and sh > 0:
                            pos._completed_this_cycle = True
                            # Use ACTUAL fill price from API, not the ask we detected.
                            # FAK fills at maker's price which may be cheaper.
                            real_fill = fill_p if fill_p > 0 else yes_ask_c
                            actual_cost = round(sh * real_fill, 2)
                            pos.record_yes_buy(real_fill, actual_cost, actual_shares=sh)
                            portfolio.total_cost += actual_cost
                            if pos.is_arb_locked:
                                pos.is_accumulation = False
                                portfolio.n_accum_completed += 1
                                portfolio.n_arb_locks += 1
                                gp = pos.guaranteed_profit or 0
                                audit("ARB_LOCKED", mid=mid, combined=round(pos.avg_price+real_fill, 4),
                                      profit=round(gp, 2), title=pos.title[:50])
                                log.info(f"  🎯 LIVE→ARB! {pos.short()} "
                                        f"NO@{pos.avg_price:.3f}+YES@{real_fill:.3f}="
                                        f"{pos.avg_price+real_fill:.3f} → ${gp:.2f} guaranteed!")
                            audit("COMPLETION_BUY", mid=mid, side="YES", price=round(real_fill, 4),
                                  shares=round(sh, 1), combined=round(pos.avg_price+real_fill, 4))
                            record_order()
                            accum_trades += 1; trades += 1

            elif pos.accum_side == "YES" and pos.yes_shares > 0:
                if _trimmed == "NO":
                    continue  # trimmed NO side — don't buy it back
                combined_c = no_ask_c + pos.yes_avg_price
                if combined_c <= complete_thresh:
                    room = portfolio.arb_room_for(mid, pos.group_key)
                    no_order_p = min(round(no_ask_c + ARB_SLIPPAGE, 2), 0.99)
                    target = min(pos.yes_shares - pos.shares, room / no_ask_c if no_ask_c > 0 else 0)
                    if target >= 5 and rate_ok():
                        no_order_cost = round(no_order_p * target, 2)
                        patience_tag = ""
                        if comp_vel < -0.0003:
                            patience_tag = f"🎯FLOOR "
                        label = f"{patience_tag}🎯LIVE-COMPLETE-NO|{combined_c:.3f}|{_sport}|{pos.short()}"
                        oid, sh, fill_p = exec_buy(pos.tok_no, no_order_p, no_order_cost,
                                            label, mode="aggressive", is_completion=True)
                        if oid and sh > 0:
                            pos._completed_this_cycle = True
                            real_fill = fill_p if fill_p > 0 else no_ask_c
                            actual_cost = round(sh * real_fill, 2)
                            pos.record_buy(real_fill, actual_cost, actual_shares=sh)
                            portfolio.total_cost += actual_cost
                            if pos.is_arb_locked:
                                pos.is_accumulation = False
                                portfolio.n_accum_completed += 1
                                portfolio.n_arb_locks += 1
                                gp = pos.guaranteed_profit or 0
                                audit("ARB_LOCKED", mid=mid, combined=round(real_fill+pos.yes_avg_price, 4),
                                      profit=round(gp, 2), title=pos.title[:50])
                                log.info(f"  🎯 LIVE→ARB! {pos.short()} "
                                        f"NO@{real_fill:.3f}+YES@{pos.yes_avg_price:.3f}="
                                        f"{real_fill+pos.yes_avg_price:.3f} → ${gp:.2f} guaranteed!")
                            audit("COMPLETION_BUY", mid=mid, side="NO", price=round(real_fill, 4),
                                  shares=round(sh, 1), combined=round(real_fill+pos.yes_avg_price, 4))
                            record_order()
                            accum_trades += 1; trades += 1

            # ─── ADD MORE to existing accumulation (buy THE OTHER SIDE only) ───
            # If we already have one side, only buy the OPPOSITE side on dips
            # to complete the arb. Never stack more of the same side.
            # Skip triad legs — they only buy NO, managed by Step T
            # Skip if Step A already bought on this position this cycle
            if (pos.is_accumulation and not pos.is_arb_locked and not pos.triad_group
                    and getattr(pos, '_completed_this_cycle', False) is False):
                t = _live_tracker.get(mid)
                if t:
                    mkt_deployed = portfolio.market_deployed(mid)
                    # Check per-market cap, total accum budget, AND per-market cooldown
                    accum_total_now = sum(
                        p.total_cost for p in portfolio.positions.values()
                        if p.is_accumulation and not p.is_arb_locked
                    )
                    last_dip_buy = getattr(pos, '_last_dip_buy', 0)
                    dip_cooldown_ok = (now_live - last_dip_buy) >= LIVE_DIP_COOLDOWN_SECS
                    if (mkt_deployed < LIVE_MAX_PER_MARKET
                        and accum_total_now < LIVE_MAX_ACCUM_TOTAL
                        and dip_cooldown_ok
                        and rate_ok()):
                        # ONLY buy the OTHER side — the side we DON'T already hold.
                        # This prevents stacking the same side and ensures dip buys
                        # work toward completing the arb.
                        other_side = "YES" if pos.accum_side == "NO" else "NO"
                        # GUARD: never buy back the side we just trimmed.
                        # Trim says "this side is dead" — dip buy should NOT contradict that.
                        if getattr(pos, 'trimmed_side', '') == other_side:
                            continue  # trimmed this side — don't buy it back
                        candidates_dip = []
                        for check_side, check_price, check_tok in [
                            ("NO", no_ask_c, pos.tok_no),
                            ("YES", yes_ask_c, pos.tok_yes),
                        ]:
                            if check_side != other_side:
                                continue  # skip the side we already hold
                            if not check_price or check_price < LIVE_MIN_PRICE or check_price > LIVE_MAX_PRICE:
                                continue
                            # Use recent (5-min) low for DIP threshold — session lows can be stale
                            side_recent_low = t.get(f"{check_side.lower()}_recent_low", 999)
                            side_low = side_recent_low if side_recent_low < 900 else t.get(f"{check_side.lower()}_low", 999)
                            if side_low >= 900:
                                continue
                            # Buy if price is within DIP_THRESHOLD of recent low
                            # TIME-ESCALATING: widen the net as position ages
                            pos_age_dip = now_live - pos.created
                            dip_thresh = LIVE_DIP_THRESHOLD
                            if pos_age_dip >= HEDGE_URGENCY_STAGE3_SECS:
                                dip_thresh += 0.04   # 7¢ total — urgently looking for 2nd side
                            elif pos_age_dip >= HEDGE_URGENCY_STAGE2_SECS:
                                dip_thresh += 0.02   # 5¢ total — moderately widened
                            if check_price <= side_low + dip_thresh:
                                # Check potential combined: this price + best seen other side (recent low)
                                other_recent = t.get(f"{'no' if check_side == 'YES' else 'yes'}_recent_low", 999)
                                other_low = other_recent if other_recent < 900 else t.get("no_low" if check_side == "YES" else "yes_low", 999)
                                potential = check_price + other_low
                                if potential < ARB_LOCK_THRESHOLD + 0.03:  # entry gate
                                    buy_size = min(LIVE_BUY_SIZE, LIVE_MAX_BUY, LIVE_MAX_PER_MARKET - mkt_deployed)
                                    if buy_size < LIVE_MIN_BUY:
                                        continue
                                    order_p = min(round(check_price + ARB_SLIPPAGE, 2), 0.99)
                                    label = f"📈LIVE-DIP-{check_side}|@{check_price:.3f}|pot={potential:.3f}|{pos.title[:20]}"
                                    oid, sh, _ = exec_buy(check_tok, order_p, buy_size,
                                                        label, mode="aggressive", )
                                    if oid and sh > 0:
                                        actual_cost = round(sh * check_price, 2)
                                        if check_side == "NO":
                                            pos.record_buy(check_price, actual_cost, actual_shares=sh)
                                        else:
                                            pos.record_yes_buy(check_price, actual_cost, actual_shares=sh)
                                        portfolio.total_cost += actual_cost
                                        portfolio.n_accum_buys += 1
                                        pos._last_dip_buy = now_live  # cooldown timer
                                        record_order()
                                        accum_trades += 1; trades += 1
                                        mkt_deployed += actual_cost
                                        audit("DIP_BUY", mid=mid, side=check_side,
                                              price=round(check_price, 4), shares=round(sh, 1),
                                              cost=actual_cost, low=round(side_low, 4),
                                              potential=round(potential, 4), title=pos.title[:50])
                                        log.info(f"  📈 LIVE DIP {check_side}: {pos.title[:35]} "
                                                f"@{check_price:.3f} {sh:.0f}sh ${actual_cost:.2f} "
                                                f"(low={side_low:.3f} potential={potential:.3f})")
                                    break  # one buy per market per cycle

        # ─── STEP H: WINNER HAMMER — double down on positions winning big ───
        # When ANY position's stronger side pushes past 85¢, the game is going our way.
        # Add $5 to ride the winner to $1.00 resolution. At 85¢ = 17.6% return.
        # Only hammer WITH momentum (price still rising) to avoid buying dead spikes.
        # Works on ALL positions — one-sided, locked arbs, wallet. Maximize winners.
        for mid, pos in list(portfolio.positions.items()):
            # Determine which side to hammer — the STRONGER side (higher bid)
            has_no = pos.shares > 0
            has_yes = pos.yes_shares > 0
            if not has_no and not has_yes:
                continue  # no position at all

            # Cooldown check
            if now_live - pos.last_hammer < HAMMER_COOLDOWN_SECS:
                continue
            # Max hammer cap per position
            if pos.total_hammered >= HAMMER_MAX_PER_POSITION:
                continue

            # Get current bid for the WINNING side
            # For one-sided: hammer the side we hold
            # For two-sided (locked arbs): hammer whichever side has higher bid (the winner)
            no_bid = pos.current_bid or 0
            yes_bid = pos.current_yes_bid or 0
            if has_no and has_yes:
                # Both sides — pick the stronger side
                if no_bid >= yes_bid:
                    held_bid = no_bid; held_tok = pos.tok_no; held_side = "NO"
                else:
                    held_bid = yes_bid; held_tok = pos.tok_yes; held_side = "YES"
            elif has_no:
                held_bid = no_bid; held_tok = pos.tok_no; held_side = "NO"
            else:
                held_bid = yes_bid; held_tok = pos.tok_yes; held_side = "YES"

            if not held_bid:
                continue
            # Price above max — skip the buy but DON'T reset the sustain timer.
            # If price dips back below max, we want credit for time already above threshold.
            if held_bid > HAMMER_MAX_PRICE:
                continue

            # ─── SUSTAINED ABOVE CHECK — no blips ───
            # Must stay above threshold for HAMMER_SUSTAIN_SECS before we commit.
            # FAST-TRACK: bid >= 90¢ = game is clearly decided, hammer immediately.
            # Below 90¢, wait the full sustain period to confirm it's not a blip.
            if held_bid >= HAMMER_THRESHOLD:
                if pos.hammer_above_since <= 0:
                    pos.hammer_above_since = now_live  # first time crossing above — start timer
                if held_bid >= 0.90:
                    # 90¢+ = game decided — no need to wait
                    log.info(f"  🔨 FAST-TRACK: {pos.title[:30]} {held_side}@{held_bid:.3f} "
                            f"≥90¢ — hammering immediately")
                else:
                    time_above = now_live - pos.hammer_above_since
                    if time_above < HAMMER_SUSTAIN_SECS:
                        if _p2_debug_counter % 20 == 0:
                            remaining = int(HAMMER_SUSTAIN_SECS - time_above)
                            log.info(f"  🔨 QUALIFY: {pos.title[:30]} {held_side}@{held_bid:.3f} "
                                    f"above {HAMMER_THRESHOLD} for {time_above:.0f}s ({remaining}s to go)")
                        continue  # not sustained long enough yet
            else:
                pos.hammer_above_since = 0.0  # dipped below — reset timer
                continue

            # Momentum check — OPTIONAL boost, not a gate.
            # The 4-minute sustain above 75¢ is the REAL confirmation the game is decided.
            # Momentum is just extra info for logging. Previously this gate blocked
            # hammers on plateau prices (vel~0) and wallet positions (no tracker).
            # That caused the bot to NEVER hammer — trim fires but hammer doesn't.
            t_ham = None
            ham_vel = 0.0
            with _live_lock:
                t_ham = _live_tracker.get(mid)
            if t_ham:
                _, _, ham_vel, _ = _get_momentum_score(t_ham, now_live)
            # If momentum is STRONGLY negative (price crashing back), skip.
            # This catches a blip that passed the sustain timer but is now reversing.
            if ham_vel < -0.002:
                log.info(f"  🔨 SKIP REVERSAL: {pos.title[:30]} {held_side}@{held_bid:.3f} "
                        f"vel={ham_vel:.4f} — price reversing hard, not safe to hammer")
                continue

            # HAMMER TIME — buy more of the winning side
            hammer_price = min(round(held_bid + ARB_SLIPPAGE, 2), 0.99)
            if not rate_ok():
                continue
            label = f"🔨HAMMER-{held_side}|@{held_bid:.3f}|vel={ham_vel:.4f}|{pos.title[:25]}"
            # Set cooldown BEFORE order to prevent rapid-fire, but DON'T mark as
            # fully hammered until we confirm the fill. If verification fails,
            # the cooldown (2 min) prevents spam but allows retry next cycle.
            pos.last_hammer = now_live
            # is_completion=True bypasses governor position cap — hammers add to
            # EXISTING positions, they're not new entries. Without this, the governor
            # silently blocks hammers when 3/3 slots are full.
            oid, sh, fill_p = exec_buy(held_tok, hammer_price, HAMMER_SIZE, label, mode="aggressive", is_completion=True)
            # ALWAYS mark as hammered once exec_buy returns an order ID, even if
            # verification failed (sh=0). Polymarket returned success:true + orderID,
            # meaning the order was accepted. If we only cap on confirmed fill, failed
            # verifications cause double-hammers (bot retries after cooldown thinking
            # the first one didn't fill, but it did).
            if oid:
                pos.total_hammered = HAMMER_MAX_PER_POSITION  # ONE SHOT — never retry
                if sh > 0:
                    # Use actual fill price, fall back to hammer_price (ask+slippage)
                    ham_fill = fill_p if fill_p and fill_p > 0 else hammer_price
                    actual_cost = round(sh * ham_fill, 2)
                    if held_side == "NO":
                        pos.record_buy(ham_fill, actual_cost, actual_shares=sh)
                    else:
                        pos.record_yes_buy(ham_fill, actual_cost, actual_shares=sh)
                    portfolio.total_cost += actual_cost
                    trades += 1
                    log.info(f"  🔨 HAMMER! {pos.title[:40]} {held_side}@{held_bid:.3f} "
                            f"+{sh:.0f}sh ${actual_cost:.2f} — ONE SHOT DONE")
                    record_game_action("hammer", {
                        "mid": mid, "title": pos.title[:50],
                        "side": held_side, "price": round(held_bid, 4),
                        "shares": sh, "cost": actual_cost,
                    })
                    # IMMEDIATE SAVE — prevent crash-before-save race condition.
                    # If the bot crashes later in this cycle, the hammer mark must
                    # already be on disk. Without this, each crash + restart = another
                    # hammer on the same position ($15 × N restarts = big loss).
                    try:
                        portfolio.save()
                    except Exception:
                        pass  # best-effort — main save loop still runs every 10s
                else:
                    log.warning(f"  🔨 HAMMER SENT but verification failed: {pos.title[:40]} "
                               f"{held_side}@{held_bid:.3f} — marked as hammered (likely filled)")
                    try:
                        portfolio.save()
                    except Exception:
                        pass
            else:
                log.warning(f"  🔨 HAMMER FAILED (no order): {pos.title[:40]} {held_side}@{held_bid:.3f} "
                           f"— will retry after cooldown")

        # ─── STEP S: SKIM — small buys on winning side for extra profit ───
        # When a held side stays above SKIM_THRESHOLD for SKIM_SUSTAIN_SECS,
        # buy a small amount ($1-2) to capture value. Repeatable with cooldown.
        if SKIM_ENABLED:
            for mid, pos in list(portfolio.positions.items()):
                if not pos.is_two_sided:
                    continue  # skim only applies to two-sided positions (arbs/hedges)
                # Don't skim positions that already got the full hammer
                if pos.total_hammered >= HAMMER_MAX_PER_POSITION:
                    continue
                # Cooldown check
                if now_live - getattr(pos, 'last_skim', 0) < SKIM_COOLDOWN_SECS:
                    continue
                # Max skim cap
                if getattr(pos, 'total_skimmed', 0) >= SKIM_MAX_PER_POSITION:
                    continue

                has_no = pos.shares > 0
                has_yes = pos.yes_shares > 0
                no_bid = pos.current_bid or 0
                yes_bid = pos.current_yes_bid or 0

                # Pick the stronger side
                if has_no and has_yes:
                    if no_bid >= yes_bid:
                        held_bid = no_bid; held_tok = pos.tok_no; held_side = "NO"
                    else:
                        held_bid = yes_bid; held_tok = pos.tok_yes; held_side = "YES"
                elif has_no:
                    held_bid = no_bid; held_tok = pos.tok_no; held_side = "NO"
                else:
                    held_bid = yes_bid; held_tok = pos.tok_yes; held_side = "YES"

                if not held_bid or held_bid >= SKIM_MAX_PRICE:
                    pos.skim_above_since = 0.0
                    continue

                # Sustained above check — must be in the .70-.79 sweet spot
                if held_bid >= SKIM_THRESHOLD:
                    if pos.skim_above_since <= 0:
                        pos.skim_above_since = now_live
                    time_above = now_live - pos.skim_above_since
                    if time_above < SKIM_SUSTAIN_SECS:
                        if _p2_debug_counter % 20 == 0 and time_above > 30:
                            remaining = int(SKIM_SUSTAIN_SECS - time_above)
                            log.info(f"  💰 SKIM-Q: {pos.title[:30]} {held_side}@{held_bid:.3f} "
                                    f"above {SKIM_THRESHOLD} for {time_above:.0f}s ({remaining}s to go)")
                        continue
                else:
                    pos.skim_above_since = 0.0
                    continue

                # SKIM — buy small amount of winning side
                skim_price = min(round(held_bid + ARB_SLIPPAGE, 2), 0.99)
                if not rate_ok():
                    continue
                label = f"💰SKIM-{held_side}|@{held_bid:.3f}|{pos.title[:25]}"
                oid, sh, fill_p = exec_buy(held_tok, skim_price, SKIM_SIZE, label, mode="aggressive", is_completion=True)
                if oid and sh > 0:
                    # Use skim_price (ask+slippage) as cost ceiling — more accurate than bid
                    skim_fill = fill_p if fill_p and fill_p > 0 else skim_price
                    actual_cost = round(sh * skim_fill, 2)
                    if held_side == "NO":
                        pos.record_buy(skim_fill, actual_cost, actual_shares=sh)
                    else:
                        pos.record_yes_buy(skim_fill, actual_cost, actual_shares=sh)
                    portfolio.total_cost += actual_cost
                    pos.total_skimmed = getattr(pos, 'total_skimmed', 0) + actual_cost
                    pos.last_skim = now_live
                    trades += 1
                    log.info(f"  💰 SKIM! {pos.title[:40]} {held_side}@{held_bid:.3f} "
                            f"+{sh:.0f}sh ${actual_cost:.2f} (total skimmed: ${pos.total_skimmed:.2f})")
                    record_game_action("skim", {
                        "mid": mid, "title": pos.title[:50],
                        "side": held_side, "price": round(held_bid, 4),
                        "shares": sh, "cost": actual_cost,
                        "total_skimmed": pos.total_skimmed,
                    })

        # ─── STEP L: LOSS TRIM — sell 50% when a side is clearly losing ───
        # When a position's held side drops below TRIM_THRESHOLD for TRIM_SUSTAIN_SECS,
        # sell 50% of the losing shares to cut losses before total wipeout.
        # GAME OVER FAST PATH: if one side is < 5¢ and other > 90¢, trim immediately.
        if TRIM_ENABLED:
            for mid, pos in list(portfolio.positions.items()):
                # Trim ANY position — two-sided OR one-sided.
                # On two-sided positions, trimming the LOSING side recovers a
                # few cents per share that would otherwise resolve at $0.
                # One-sided positions (incomplete arbs) can crash too.
                if pos.shares <= 0 and pos.yes_shares <= 0:
                    continue  # no shares at all
                # ONE SHOT — only trim each position once, like hammer
                if getattr(pos, 'total_trimmed', 0) > 0:
                    continue

                no_bid = pos.current_bid or 0
                yes_bid = pos.current_yes_bid or 0

                # Find the LOSING side (lower bid)
                # Handle zero bids: when a game resolves, losing side goes to 0¢
                trim_side = None
                trim_bid = 0
                trim_tok = None
                trim_shares = 0
                game_over = False

                if pos.is_two_sided:
                    # TWO-SIDED: GAME OVER FAST PATH — one side near 0, other near $1
                    if no_bid <= 0.05 and yes_bid > 0.90 and pos.shares > 0:
                        trim_side = "NO"; trim_bid = max(no_bid, 0.01)
                        trim_tok = pos.tok_no; trim_shares = pos.shares
                        game_over = True
                    elif yes_bid <= 0.05 and no_bid > 0.90 and pos.yes_shares > 0:
                        trim_side = "YES"; trim_bid = max(yes_bid, 0.01)
                        trim_tok = pos.tok_yes; trim_shares = pos.yes_shares
                        game_over = True
                    # Normal trim: both sides have a bid, but one is losing badly
                    elif no_bid > 0 and yes_bid > 0:
                        if no_bid < yes_bid and no_bid < TRIM_THRESHOLD and pos.shares > 0:
                            trim_side = "NO"; trim_bid = no_bid
                            trim_tok = pos.tok_no; trim_shares = pos.shares
                        elif yes_bid < no_bid and yes_bid < TRIM_THRESHOLD and pos.yes_shares > 0:
                            trim_side = "YES"; trim_bid = yes_bid
                            trim_tok = pos.tok_yes; trim_shares = pos.yes_shares
                else:
                    # ONE-SIDED: only one side held — trim if that side is crashing
                    # SANITY CHECK: for game-over on one-sided positions, verify the OTHER
                    # side confirms it (bid > 90¢). Without this, a thin order book or API
                    # glitch returning bid=0 triggers a false game-over and dumps shares at 1¢.
                    # Also require the bid has crashed > 80% from our avg price — a 5¢ bid on
                    # a 53¢ position is a 90% crash, only real if the game is truly over.
                    _held_avg = pos.avg_price if pos.shares > 0 else pos.yes_avg_price
                    _other_bid = yes_bid if pos.shares > 0 else no_bid
                    if pos.shares > 0 and no_bid <= 0.05:
                        if _other_bid > 0.90 and (_held_avg <= 0 or no_bid < _held_avg * 0.20):
                            trim_side = "NO"; trim_bid = max(no_bid, 0.01)
                            trim_tok = pos.tok_no; trim_shares = pos.shares
                            game_over = True
                        # else: likely a thin book / API glitch — skip, don't panic sell
                    elif pos.yes_shares > 0 and yes_bid <= 0.05:
                        if _other_bid > 0.90 and (_held_avg <= 0 or yes_bid < _held_avg * 0.20):
                            trim_side = "YES"; trim_bid = max(yes_bid, 0.01)
                            trim_tok = pos.tok_yes; trim_shares = pos.yes_shares
                            game_over = True
                    elif pos.shares > 0 and no_bid > 0 and no_bid < TRIM_THRESHOLD:
                        trim_side = "NO"; trim_bid = no_bid
                        trim_tok = pos.tok_no; trim_shares = pos.shares
                    elif pos.yes_shares > 0 and yes_bid > 0 and yes_bid < TRIM_THRESHOLD:
                        trim_side = "YES"; trim_bid = yes_bid
                        trim_tok = pos.tok_yes; trim_shares = pos.yes_shares

                if not trim_side:
                    # GRACE BAND: don't reset timer on brief bounces above threshold.
                    # Only reset if price recovered significantly (above threshold + 10¢).
                    # This prevents volatile oscillation around 28-32¢ from perpetually
                    # resetting the 4-min timer while the position bleeds out.
                    losing_bid = min(no_bid, yes_bid) if (no_bid > 0 and yes_bid > 0) else max(no_bid, yes_bid)
                    if losing_bid > 0.35:
                        pos.trim_below_since = 0.0  # recovered past 35¢ — reset timer
                    continue

                # Log resolution for threshold analysis when game is over
                if game_over:
                    winner = "YES" if trim_side == "NO" else "NO"
                    record_resolution(mid, pos.title, getattr(pos, 'typ', ''),
                                     winner, no_bid, yes_bid, pos)

                # GAME OVER: skip the sustained-below timer — act immediately
                if game_over:
                    log.info(f"  ✂️ GAME OVER: {pos.title[:40]} {trim_side}@{trim_bid:.3f} — trimming NOW")
                else:
                    # Sustained below check — must be below threshold for TRIM_SUSTAIN_SECS
                    # O/U and spread markets get 2x patience — every score swings the line
                    _trim_sustain = TRIM_SUSTAIN_SECS
                    if getattr(pos, 'typ', '') in ("ou", "spread"):
                        _trim_sustain = TRIM_SUSTAIN_SECS * 2  # 8 min — scores cause temporary dips
                    if pos.trim_below_since <= 0:
                        pos.trim_below_since = now_live
                    time_below = now_live - pos.trim_below_since
                    if time_below < _trim_sustain:
                        if _p2_debug_counter % 20 == 0 and time_below > 30:
                            remaining = int(_trim_sustain - time_below)
                            log.info(f"  ✂️ TRIM-Q: {pos.title[:30]} {trim_side}@{trim_bid:.3f} "
                                    f"below {TRIM_THRESHOLD} for {time_below:.0f}s ({remaining}s to go)")
                        continue

                # GAME OVER WRITE-OFF: shares at ~0¢ can't be sold (no buyers).
                # Just write them off — remove from position and log the loss.
                if game_over and trim_bid <= 0.02:
                    loss = round(trim_shares * (pos.avg_price if trim_side == "NO" else pos.yes_avg_price), 2)
                    if trim_side == "NO":
                        pos.shares = 0
                        pos.cost = 0
                    else:
                        pos.yes_shares = 0
                        pos.yes_cost = 0
                    portfolio.total_cost = max(0, portfolio.total_cost - loss)
                    pos.total_trimmed = getattr(pos, 'total_trimmed', 0) + loss
                    pos.last_trim = now_live
                    pos.trim_below_since = 0.0
                    pos.trimmed_side = trim_side  # block dip-buy/completion of this side
                    trades += 1
                    _burned_markets.add(mid)  # never re-enter
                    log.info(f"  ✂️ WRITE-OFF: {pos.title[:40]} {trim_shares:.0f} {trim_side} shares "
                            f"@ ~0¢ — game over, ${loss:.2f} loss written off")
                    record_game_action("write_off", {
                        "mid": mid, "title": pos.title[:50],
                        "side": trim_side, "shares_lost": trim_shares,
                        "loss": loss,
                    })
                    continue

                # TRIM — sell 75% of losing side
                sell_shares = math.floor(trim_shares * TRIM_PCT)
                if sell_shares < 5:
                    continue  # Polymarket minimum
                # Sell AGGRESSIVELY below bid using FAK — fill what's available, kill rest.
                # GTC sells on crashing positions just sit unfilled forever.
                # 2¢ below bid ensures we cross the spread and hit takers.
                # FAK means no stale orders — partial fills OK, rest killed instantly.
                sell_price = round(max(trim_bid - 0.02, 0.01), 2)
                if not rate_ok():
                    continue
                label = f"✂️TRIM-{trim_side}|@{trim_bid:.3f}|{TRIM_PCT*100:.0f}%|{pos.title[:25]}"
                oid, revenue, fill_price = exec_sell(trim_tok, sell_price, sell_shares, label, mode="aggressive")
                if oid == "NO_BALANCE":
                    # Shares don't exist on-chain — zero out the stale position
                    loss = round(sell_shares * (pos.avg_price if trim_side == "NO" else pos.yes_avg_price), 2)
                    if trim_side == "NO":
                        pos.shares = 0
                        pos.cost = 0
                    else:
                        pos.yes_shares = 0
                        pos.yes_cost = 0
                    portfolio.total_cost = max(0, portfolio.total_cost - loss)
                    pos.last_trim = now_live
                    pos.trim_below_since = 0.0
                    pos.trimmed_side = trim_side  # block dip-buy/completion of this side
                    _burned_markets.add(mid)  # never re-enter
                    log.info(f"  ✂️ TRIM-STALE: {pos.title[:40]} {sell_shares:.0f} {trim_side} shares "
                            f"not on-chain — zeroed out (${loss:.2f} loss)")
                    continue
                if oid and revenue > 0:
                    # Update position — decrement BOTH position cost AND portfolio total_cost
                    cost_freed = round(sell_shares * (pos.avg_price if trim_side == "NO" else pos.yes_avg_price), 2)
                    if trim_side == "NO":
                        pos.shares -= sell_shares
                        pos.cost = max(0, pos.cost - cost_freed)
                    else:
                        pos.yes_shares -= sell_shares
                        pos.yes_cost = max(0, pos.yes_cost - cost_freed)
                    portfolio.total_cost = max(0, portfolio.total_cost - cost_freed)
                    pos.total_trimmed = getattr(pos, 'total_trimmed', 0) + revenue
                    pos.last_trim = now_live
                    pos.trim_below_since = 0.0  # reset timer after trim
                    pos.trimmed_side = trim_side  # block dip-buy/completion of this side
                    portfolio.total_realized += revenue
                    trades += 1
                    _burned_markets.add(mid)  # never re-enter this market
                    log.info(f"  ✂️ TRIM! {pos.title[:40]} sold {sell_shares:.0f} {trim_side}@{trim_bid:.3f} "
                            f"= ${revenue:.2f} (cutting losses) — BURNED")
                    record_game_action("trim", {
                        "mid": mid, "title": pos.title[:50],
                        "side": trim_side, "price": round(trim_bid, 4),
                        "shares_sold": sell_shares, "revenue": revenue,
                        "remaining": pos.shares if trim_side == "NO" else pos.yes_shares,
                    })

        # ─── STEP A½: STALE POSITION EXIT — hedge one-sided positions ───
        # Bought one side, waiting for 2nd side dip. After timeout no luck:
        #   bid < 0.55 → losing side of the game, sell now, free the slot
        #   bid >= 0.55 → winning side, hold — resolves to $1.00 = profit
        # Also hold if price is still volatile (game still swinging)
        # O/U positions get 2x patience — they swing back and forth naturally
        for mid, pos in list(portfolio.positions.items()):
            if not pos.is_accumulation or pos.is_arb_locked:
                continue
            if pos.from_wallet:
                continue
            # Two-sided positions (hedged) should never be stale-exited
            if pos.shares > 0 and pos.yes_shares > 0:
                continue

            # Use the LATER of pos.created or bot start time, so that on restart
            # positions get at least a full timeout window from the restart moment.
            # Without this, a 15-min-old position would immediately get stale-exited
            # on restart before the bot even has a chance to find the other side.
            effective_age_start = max(pos.created, _start_time)
            age_secs = now_live - effective_age_start
            # O/U bets swing more than moneylines — give them 2x patience
            # Early scoring runs drop Over, then defense brings it back (and vice versa)
            stale_timeout = LIVE_STALE_TIMEOUT_SECS
            if pos.typ in ("ou", "spread"):
                stale_timeout = LIVE_STALE_TIMEOUT_SECS * 2  # 16 min for O/U and spreads — they swing naturally
            if age_secs < stale_timeout:
                continue  # too young, give it more time

            # Determine which side we hold and its current price
            if pos.accum_side == "NO" and pos.shares > 0:
                sell_tok = pos.tok_no
                sell_shares = pos.shares
                current_bid = pos.current_bid
                sell_cost = pos.cost
            elif pos.accum_side == "YES" and pos.yes_shares > 0:
                sell_tok = pos.tok_yes
                sell_shares = pos.yes_shares
                current_bid = pos.current_yes_bid
                sell_cost = pos.yes_cost
            else:
                continue

            if not current_bid or current_bid <= 0:
                continue

            # Check if game is still volatile (price still swinging = dips still possible)
            t = _live_tracker.get(mid)
            if t:
                side_key = pos.accum_side.lower()
                vol = _get_volatility(t.get(f"{side_key}_prices", []))
                if vol >= LIVE_STALE_STILL_ACTIVE:
                    continue  # game still swinging, other side could still dip, hold
                # TIME-ESCALATING: if trailing shows FIRST_BOUNCE or READY,
                # the market is actively moving — give it more time even if stale
                trailing_st = t.get("trailing_state", "WATCHING")
                if trailing_st in ("FIRST_BOUNCE", "READY"):
                    continue  # momentum shift in progress — don't stale-exit now

            # bid >= threshold → we're on the winning side. Hold and let it resolve to $1.00
            # Spreads hover closer to 50/50, so use a lower sell threshold (40¢ vs 55¢)
            sell_below = 0.40 if pos.typ == "spread" else LIVE_STALE_SELL_BELOW
            if current_bid >= sell_below:
                continue

            # bid < threshold → we're on the losing side, game getting away. Cut losses, free slot
            sell_price = max(round(current_bid - 0.01, 2), 0.01)  # 1 tick below bid for fill
            loss = round(sell_price * sell_shares - sell_cost, 2)
            label = (f"🗑️STALE-EXIT-{pos.accum_side}|@{sell_price:.3f}|"
                    f"age={age_secs/60:.0f}m|loss=${loss:.2f}|{pos.title[:25]}")
            oid, revenue, _ = exec_sell(sell_tok, sell_price, sell_shares, label)
            if oid and revenue > 0:
                cost_freed = sell_cost  # cost basis of shares being sold
                pos.record_sell(sell_price, sell_shares, revenue)
                portfolio.total_cost = max(0, portfolio.total_cost - cost_freed)
                portfolio.total_realized += loss
                portfolio.n_loss_cuts += 1
                # Remove the position entirely — slot freed
                _burned_markets.add(mid)  # never re-enter
                if pos.shares <= 0 and pos.yes_shares <= 0:
                    del portfolio.positions[mid]
                audit("STALE_EXIT", mid=mid, side=pos.accum_side,
                      price=round(sell_price, 4), shares=round(sell_shares, 1),
                      loss=round(loss, 2), age_min=round(age_secs/60, 1),
                      bid=round(current_bid, 4), title=pos.title[:50])
                log.info(f"  🗑️ STALE EXIT: {pos.title[:40]} "
                        f"{pos.accum_side}@{sell_price:.3f} {sell_shares:.0f}sh "
                        f"${loss:+.2f} after {age_secs/60:.0f}min "
                        f"(bid={current_bid:.3f} < {LIVE_STALE_SELL_BELOW})")
                accum_trades += 1; trades += 1

        # ─── PRE-SCAN: Tag triad-eligible markets BEFORE Step B ───
        # This ensures Step B won't treat win/draw soccer legs as regular binary arbs.
        _triad_pre_events = defaultdict(list)
        for _mid, _t in _live_tracker.items():
            _mkt = _t.get("mkt") or {}
            _slug = _mkt.get("event_slug", "")
            _typ = _mkt.get("typ", "")
            if _slug and _typ in ("win", "draw", "match"):
                _triad_pre_events[_slug].append((_mid, _t, _mkt))
        for _slug, _legs in _triad_pre_events.items():
            wins = [x for x in _legs if x[2].get("typ") == "win"]
            draws = [x for x in _legs if x[2].get("typ") == "draw"]
            if len(wins) >= 2 and len(draws) >= 1:
                # This is a valid triad — tag ALL legs so Step B skips them
                for _mid, _t, _mkt in _legs:
                    _t["_triad_slug"] = _slug

        # ─── STEP B: Open NEW live accumulation positions ───
        # Score all tracked markets, buy the best opportunities.
        # SLOT LOGIC: max LIVE_MAX_PENDING single-sided positions "searching for 2nd side".
        # When a position completes into an arb (both sides locked) → slot freed.
        # Very high scores (>= LIVE_MAX_PENDING_OVERRIDE) ignore the slot cap.
        accum_age = now_live - _start_time
        pending_count = portfolio.accum_pending_count()  # single-sided only, not completed arbs
        if accum_age >= LIVE_WARMUP_SECS:
            # Score all tracked markets
            scored = []
            with _live_lock:
                for mid, t in _live_tracker.items():
                    if portfolio.has(mid):
                        continue  # already in portfolio
                    score, reason = _score_live_market(mid, t, now_live)
                    if score >= LIVE_MIN_SCORE_ENTRY:
                        scored.append((score, mid, t, reason))

            scored.sort(reverse=True)  # best opportunities first

            # Periodic logging of top opportunities
            if _p2_debug_counter % 10 == 0 and scored:
                log.info(f"  🎯 LIVE TRACKER: {len(_live_tracker)} markets tracked, "
                        f"{len(scored)} scoreable (>={LIVE_MIN_SCORE_ENTRY}), "
                        f"pending={pending_count}/{LIVE_MAX_PENDING}, top 5:")
                for sc, mid, t, reason in scored[:5]:
                    mkt = t.get("mkt") or {}
                    trail_icon = {"READY": "✅", "FIRST_BOUNCE": "⏳", "WATCHING": "👀"}.get(
                        t.get("trailing_state", "WATCHING"), "?")
                    log.info(f"    [{sc:3d}] {mkt.get('title','?')[:40]} "
                            f"floor={t['combined_floor']:.3f} "
                            f"Y@{t.get('yes_now','?')}/{t['yes_low']:.3f} "
                            f"N@{t.get('no_now','?')}/{t['no_low']:.3f} "
                            f"{trail_icon} | {reason}")

            # Calculate total accumulation deployment — only ONE-SIDED positions
            # that haven't completed yet. Arb-locked (completed) positions don't
            # count — they're hedged. Hammered $ on existing positions also doesn't
            # count against the entry budget (hammers bypass this via is_completion).
            accum_total_deployed = sum(
                p.total_cost for p in portfolio.positions.values()
                if p.is_accumulation and not p.is_arb_locked
                and ((p.shares > 0) != (p.yes_shares > 0))  # truly one-sided only
            )

            # Calculate TOTAL one-sided exposure (bot + wallet) — risk cap
            onesided_total = sum(
                p.total_cost for p in portfolio.positions.values()
                if not p.is_arb_locked
                and ((p.shares > 0) != (p.yes_shares > 0))  # truly one-sided (XOR)
            )

            for score, mid, t, reason in scored:
                # ─── TOTAL ONE-SIDED RISK CAP (bot + wallet) ───
                if onesided_total >= LIVE_MAX_ONESIDED_TOTAL:
                    if _p2_debug_counter % 10 == 0:
                        log.info(f"  🛑 RISK CAP: ${onesided_total:.2f} one-sided >= ${LIVE_MAX_ONESIDED_TOTAL:.2f} — no new entries until positions complete or exit")
                    break
                # ─── TOTAL ACCUMULATION BUDGET CAP ───
                if accum_total_deployed >= LIVE_MAX_ACCUM_TOTAL:
                    if _p2_debug_counter % 10 == 0:
                        log.info(f"  🛑 ACCUM CAP: ${accum_total_deployed:.2f} >= ${LIVE_MAX_ACCUM_TOTAL:.2f} — no new positions")
                    break
                # ─── SUSTAINED SCORE CHECK — must hold score for N consecutive cycles ───
                # EXCEPTION: high momentum markets get fast-tracked (1 cycle only)
                mom_score = t.get("_momentum_score", 0)
                required_streak = 1 if mom_score >= 30 else LIVE_SCORE_SUSTAIN_CYCLES
                streak = t.get("high_score_streak", 0)
                if streak < required_streak:
                    continue  # score hasn't been stable long enough

                # ─── DUAL TRAILING STOP GATE ───
                # Wait for BOTH sides to bounce from their session lows before entering.
                # This catches the bottom of the combined price curve — not mid-drop.
                # BYPASS: very high momentum (mom_score >= 40) can skip this gate.
                #         When momentum is screaming, the window is NOW.
                # BYPASS: combined floor already deeply below target = arb is so good
                #         we don't want to miss it waiting for the perfect timing.
                if TRAILING_REQUIRED and mom_score < 40:
                    trailing_ready = t.get("_trailing_ready", False)
                    combined_floor = t.get("combined_floor", 999)
                    deep_arb = combined_floor < (LIVE_TARGET - 0.05)  # 5¢ deeper than target

                    if not trailing_ready and not deep_arb:
                        trailing_info = t.get("_trailing_info", "")
                        trailing_state = t.get("trailing_state", "WATCHING")
                        if _p2_debug_counter % 20 == 0 and trailing_state != "WATCHING":
                            mkt_info = t.get("mkt", {})
                            log.info(f"  ⏳ TRAILING GATE: {mkt_info.get('title','?')[:40]} "
                                    f"score={score} but {trailing_info} — waiting for dual bounce")
                        audit("ENTRY_GATE", mid=mid, score=score,
                              reason="trailing_not_ready", state=trailing_state,
                              title=t.get("mkt", {}).get("title", "")[:50])
                        continue  # not ready — wait for both sides to bounce

                # DOUBLE-BUY GUARD: re-check portfolio in case we bought this market earlier this cycle
                if portfolio.has(mid):
                    continue
                if mid in _burned_markets:
                    continue  # trimmed/stale-exited — don't re-enter
                # Slot check: allow override for very high scores
                if pending_count >= LIVE_MAX_PENDING and score < LIVE_MAX_PENDING_OVERRIDE:
                    continue  # all slots full and score not high enough to override
                if not rate_ok(is_new=True):
                    break

                mkt = t.get("mkt") or {}
                if not mkt:
                    continue
                # Only accumulate sports markets (resolution risk on non-sports)
                if not mkt.get("is_sports", True):
                    continue
                # BLOCKED market types (e.g. esports match winners)
                if mkt.get("typ", "other") in BLOCKED_MARKET_TYPES:
                    continue
                # SKIP triad markets — these are win/draw legs of a soccer match.
                # Triad arb = buy NO on all 3 legs. Regular accumulation would buy
                # YES+NO as a binary arb, which is WRONG for triads.
                # Let Step T (triad scanner) handle these exclusively.
                if mkt.get("typ") in ("win", "draw") and t.get("_triad_slug"):
                    continue
                # PER-EVENT CLUSTER CAP: max 2 positions from the same game/event.
                # On busy NBA nights, all spreads/totals for one game are correlated.
                # If the team loses, ML + spread + O/U all lose together.
                event_slug = mkt.get("event_slug", "")
                if event_slug:
                    same_event_count = sum(
                        1 for p in portfolio.positions.values()
                        if getattr(p, 'event_slug', '') == event_slug
                    )
                    if same_event_count >= 2:
                        if _p2_debug_counter % 20 == 0:
                            log.info(f"  🛑 EVENT CAP: {event_slug[:30]} already has {same_event_count} positions — skip")
                        continue

                yes_now = t.get("yes_now")
                no_now = t.get("no_now")
                if not yes_now or not no_now:
                    continue

                # Determine which side to buy first.
                # PREFERENCE: buy a side in the 0.40-0.50 range (cheap, pre-crossover).
                # If momentum detected, buy the surging side.
                # Otherwise pick the higher-priced side (original strategy).
                buy_side = None
                buy_price = None
                buy_tok = None

                # Build candidates: any side where the arb math works
                # For INITIAL entries, use LIVE_MIN_ENTRY_PRICE (no cheap underdog buys)
                candidates = []
                no_in_range = (no_now >= LIVE_MIN_ENTRY_PRICE and no_now <= LIVE_MAX_PRICE)
                yes_in_range = (yes_now >= LIVE_MIN_ENTRY_PRICE and yes_now <= LIVE_MAX_PRICE)

                mom_side = t.get("_momentum_side")
                mom_vel = t.get("_momentum_vel", 0)
                falling = t.get("_falling_side")

                # Use recent (5-min rolling) lows for potential — session lows can be stale
                _yes_low_pot = t.get("yes_recent_low", 999)
                _no_low_pot = t.get("no_recent_low", 999)
                # Fallback to session lows if recent window hasn't populated yet
                if _yes_low_pot >= 900:
                    _yes_low_pot = t["yes_low"]
                if _no_low_pot >= 900:
                    _no_low_pot = t["no_low"]

                if no_in_range:
                    potential_no = no_now + _yes_low_pot
                    if potential_no < ARB_LOCK_THRESHOLD + 0.03:
                        candidates.append(("NO", no_now, mkt["tok_no"], potential_no))
                if yes_in_range:
                    potential_yes = yes_now + _no_low_pot
                    if potential_yes < ARB_LOCK_THRESHOLD + 0.03:
                        candidates.append(("YES", yes_now, mkt.get("tok_yes"), potential_yes))

                # MOMENTUM OVERRIDE: If momentum is strong on a side but it was
                # excluded from candidates because its arb potential is too high
                # (price already surged past cheap zone), STILL add it as a candidate.
                # Rationale: momentum buys aim to WIN, not just arb. A side at 55¢
                # surging toward 75¢+ will be hammered and profit if it wins.
                # Only do this if the OTHER side isn't already a candidate (avoid
                # buying momentum side when the falling side is the only arb play).
                #
                # DISABLED for low-scoring sports (soccer, hockey): goals are rare and
                # a single goal causes massive price surges that DON'T revert. Chasing
                # the surging side in soccer = buying at inflated price with bad arb math.
                # In soccer, ALWAYS buy the dipping side for arb potential.
                _entry_sport = detect_sport(mkt.get("title", ""))
                _no_momentum_sports = ("soccer", "hockey")
                if mom_side and mom_vel >= 0.002 and _entry_sport not in _no_momentum_sports:
                    mom_in_candidates = any(c[0] == mom_side for c in candidates)
                    if not mom_in_candidates:
                        # Add momentum side with relaxed potential check
                        if mom_side == "YES" and yes_in_range:
                            potential_yes = yes_now + _no_low_pot
                            if potential_yes < 1.02:  # relaxed: up to 2¢ over parity (was 5¢)
                                candidates.append(("YES", yes_now, mkt.get("tok_yes"), potential_yes))
                        elif mom_side == "NO" and no_in_range:
                            potential_no = no_now + _yes_low_pot
                            if potential_no < 1.02:
                                candidates.append(("NO", no_now, mkt["tok_no"], potential_no))

                # O/U & SPREAD LOPSIDED GUARD: If one side is already dominant (>65¢),
                # the game is mid-play and scoring momentum has shifted the line.
                # Don't enter — the cheap side looks like a bargain but it's cheap for
                # a reason (e.g., Under at 35¢ with 3 runs already scored = bad entry).
                # O/U markets swing hard on every score and don't mean-revert like MLs.
                _mkt_typ = mkt.get("typ", "")
                if _mkt_typ in ("ou", "spread"):
                    _max_side = max(yes_now, no_now)
                    if _max_side > 0.65:
                        if _p2_debug_counter % 30 == 0:
                            log.info(f"  🚫 O/U LOPSIDED SKIP: {mkt['title'][:40]} "
                                    f"YES={yes_now:.2f} NO={no_now:.2f} — one side >{65}¢, game already shifted")
                        continue

                if candidates:
                    # HARD RULE: NEVER buy the falling side on initial entry.
                    # The Panathinaikos bug: bot bought PANATHIN at 49¢ as it was falling
                    # through the sweet spot. Should have bought OLYMPIAC (the surging side).
                    if falling:
                        candidates = [c for c in candidates if c[0] != falling]
                    if not candidates:
                        continue  # both sides falling or only falling side available — skip

                    # For low-scoring sports, ignore momentum preference entirely —
                    # always pick based on arb potential / sweet spot
                    if _entry_sport not in _no_momentum_sports:
                        mom_match = [c for c in candidates if c[0] == mom_side] if mom_side and mom_vel >= 0.002 else []
                    else:
                        mom_match = []  # soccer/hockey: never chase momentum

                    # PULLBACK DETECTION: if a surging side just pulled back from its high,
                    # that's a better entry than buying at peak. The side was rising but
                    # just dipped temporarily — buy the dip on a winner.
                    pullback_match = []
                    if _entry_sport not in _no_momentum_sports:
                        for c_side, c_price, c_tok, c_pot in candidates:
                            is_pb, pb_high, pb_cur, pb_amt = _get_pullback(t, c_side, now_live)
                            if is_pb and pb_amt >= TRAILING_DELTA:
                                pullback_match.append((c_side, c_price, c_tok, c_pot, pb_amt))
                        # Sort by largest pullback (best discount from high)
                        pullback_match.sort(key=lambda x: -x[4])

                    # PREFERRED: side in the sweet spot (0.40-0.50)
                    sweet_spot = [c for c in candidates if 0.40 <= c[1] <= 0.50]

                    if mom_match:
                        # MOMENTUM BUY: a side is surging — buy it while it's cheap
                        buy_side, buy_price, buy_tok, _ = mom_match[0]
                    elif pullback_match:
                        # PULLBACK BUY: surging side just dipped — buy the dip
                        buy_side, buy_price, buy_tok, _, _pb = pullback_match[0]
                    elif sweet_spot:
                        # SWEET SPOT: a side in 0.40-0.50 range (falling already filtered out)
                        sweet_spot.sort(key=lambda c: c[3])  # best arb potential
                        buy_side, buy_price, buy_tok, _ = sweet_spot[0]
                    else:
                        # DEFAULT: pick the side with best arb potential
                        candidates.sort(key=lambda c: (c[3], c[1]))  # lowest potential combined first
                        buy_side, buy_price, buy_tok, _ = candidates[0]

                if not buy_side or not buy_tok:
                    continue

                # Buy size: $4-8 for testing (~10-20 shares at typical 0.30-0.60 prices)
                buy_size = min(LIVE_BUY_SIZE, LIVE_MAX_BUY)
                if score >= 70:
                    buy_size = LIVE_MAX_BUY   # high score → max test size
                elif score < 45:
                    buy_size = LIVE_MIN_BUY   # lower score → minimum

                order_p = min(round(buy_price + ARB_SLIPPAGE, 2), 0.99)
                other_low = _no_low_pot if buy_side == "YES" else _yes_low_pot
                potential = buy_price + other_low
                is_override = pending_count >= LIVE_MAX_PENDING
                override_tag = "🚀OVERRIDE " if is_override else ""
                mom_tag = f"⚡MOM:{mom_side}" if mom_side else ""
                fall_tag = f"⬇️AVOID:{falling}" if falling else ""
                trail_tag = "🎯TRAIL " if t.get("_trailing_ready") else ""
                # Check if this was a pullback buy
                _is_pb, _pb_high, _, _pb_amt = _get_pullback(t, buy_side, now_live)
                pb_tag = f"📉PB:{_pb_amt:.2f} " if _is_pb else ""
                label = f"📦{override_tag}{trail_tag}{pb_tag}{mom_tag}{fall_tag}LIVE-{buy_side}|@{buy_price:.3f}|pot={potential:.3f}|sc={score}|{mkt['title'][:20]}"
                oid, sh, fill_p = exec_buy(buy_tok, order_p, buy_size,
                                    label, mode="aggressive", )
                if oid and sh > 0:
                    real_fill = fill_p if fill_p > 0 else buy_price
                    actual_cost = round(sh * real_fill, 2)
                    pos = Position(mid, mkt["title"], mkt["typ"], mkt["tok_no"],
                                 mkt.get("tok_yes", ""), mkt.get("group", mid))
                    if buy_side == "NO":
                        pos.record_buy(real_fill, actual_cost, actual_shares=sh)
                    else:
                        pos.record_yes_buy(real_fill, actual_cost, actual_shares=sh)
                    pos.is_accumulation = True
                    pos.accum_side = buy_side
                    pos.event_slug = mkt.get("event_slug", "")
                    portfolio.add_position(pos)
                    _governor_position_added()
                    portfolio.n_entries += 1
                    portfolio.n_accum_buys += 1
                    portfolio.total_cost += actual_cost
                    accum_total_deployed += actual_cost  # update running total for loop cap
                    pending_count += 1
                    record_order(is_new=True)
                    accum_trades += 1; trades += 1
                    ws_start([mkt["tok_no"]])
                    if mkt.get("tok_yes"):
                        ws_start([mkt["tok_yes"]])
                    audit("ENTRY_BUY", mid=mid, side=buy_side, price=round(real_fill, 4),
                          shares=round(sh, 1), cost=actual_cost, score=score,
                          potential=round(potential, 4), floor=round(t['combined_floor'], 4),
                          trailing=t.get("trailing_state", "?"),
                          momentum_side=t.get("_momentum_side"),
                          title=mkt['title'][:50])
                    log.info(f"  📦 {override_tag}LIVE NEW {buy_side}: {mkt['title'][:40]} "
                            f"@{real_fill:.3f} {sh:.0f}sh ${actual_cost:.2f} "
                            f"[{pending_count}/{LIVE_MAX_PENDING} pending] "
                            f"(score={score} potential={potential:.3f} floor={t['combined_floor']:.3f})")

    if accum_trades:
        log.info(f"  📦 LIVE ACCUM: {accum_trades} trades this cycle "
                f"({portfolio.accum_pending_count()}/{LIVE_MAX_PENDING} pending, "
                f"${portfolio.accum_deployed():.2f} deployed)")

    # ─── STEP T: TRIAD ARB SCANNER ───
    # Soccer moneylines have 3 mutually exclusive outcomes: Win A, Win B, Draw.
    # Buy NO on all 3 → exactly 2 NOs pay $1 each = $2 payout.
    # If combined NO cost < $2 → guaranteed profit.
    # Group tracked markets by event_slug, find triads, buy cheapest leg first.
    triad_trades = 0
    triad_events = defaultdict(list)  # event_slug → list of (mid, tracker_entry)
    for mid, t in _live_tracker.items():
        mkt = t.get("mkt") or {}
        slug = mkt.get("event_slug", "")
        if not slug:
            continue
        typ = mkt.get("typ", "")
        if typ in ("win", "draw", "match"):
            triad_events[slug].append((mid, t, mkt))

    for slug, legs in triad_events.items():
        if len(legs) < 3:
            continue
        # Need exactly: 2 "win" + 1 "draw", or check all combinations
        wins = [(mid, t, m) for mid, t, m in legs if m.get("typ") == "win"]
        draws = [(mid, t, m) for mid, t, m in legs if m.get("typ") == "draw"]

        if len(wins) < 2 or len(draws) < 1:
            continue

        # Take first 2 wins + first draw as the triad
        triad = wins[:2] + draws[:1]

        # Get current NO prices for all 3 legs (NO on all 3 = triad arb)
        no_prices = []
        all_valid = True
        for mid, t, m in triad:
            no_now = t.get("no_now")
            if not no_now or no_now <= 0:
                all_valid = False
                break
            # GUARD: Skip near-resolved legs — if NO price > 0.90, that outcome
            # is basically decided (the YES side nearly lost). Not a real arb.
            if no_now > 0.90:
                all_valid = False
                break
            # GUARD: Skip tiny NO prices — if NO < 0.10, the YES side is nearly
            # certain to win. Buying NO at <10¢ is a directional bet, not an arb.
            if no_now < 0.10:
                all_valid = False
                break
            no_prices.append((mid, t, m, no_now))

        if not all_valid or len(no_prices) != 3:
            continue

        combined_no = sum(np for _, _, _, np in no_prices)

        # Check if any legs are already owned (partial triad)
        owned_legs = [(mid, t, m, np) for mid, t, m, np in no_prices
                      if portfolio.has(mid) and portfolio.get(mid).shares > 0]
        needed_legs = [(mid, t, m, np) for mid, t, m, np in no_prices
                       if not portfolio.has(mid) or portfolio.get(mid).shares <= 0]

        # Log triad opportunity for tracker
        for mid, t, m, np in no_prices:
            t["_triad_combined"] = combined_no
            t["_triad_slug"] = slug
            t["_triad_legs"] = len(no_prices)
            t["_triad_owned"] = len(owned_legs)

        # Already complete? Check if locked
        if len(owned_legs) == 3:
            continue  # all legs owned, let portfolio handle it

        if not rate_ok(is_new=True):
            continue

        # ENTRY LOGIC: buy missing NO legs if combined is attractive
        # Payout = $2 (2 NOs pay $1 each), so combined NO < $2 = profit
        if combined_no <= TRIAD_ENTRY_THRESHOLD and needed_legs:
            # Sort needed legs by price (buy cheapest first)
            needed_legs.sort(key=lambda x: x[3])

            for mid, t, m, no_price in needed_legs:
                if mid in _burned_markets:
                    continue  # trimmed/stale-exited — don't re-enter
                if not rate_ok():
                    break
                # Check budget
                room = portfolio.arb_room_for(mid, slug)
                if room < 5:
                    continue

                tok_no = m.get("tok_no")
                if not tok_no:
                    continue

                # Determine buy size — scale with how good the triad is
                if combined_no <= TRIAD_NO_THRESHOLD:
                    buy_size = LIVE_MAX_BUY  # great price, max size
                else:
                    buy_size = LIVE_BUY_SIZE  # decent, standard size

                buy_size = min(buy_size, room)
                order_p = min(round(no_price + ARB_SLIPPAGE, 2), 0.99)
                spread_pct = (2.0 - combined_no) / 2.0 * 100

                label = (f"🔺TRIAD-NO|{m['typ']}|comb={combined_no:.3f}|"
                        f"spread={spread_pct:.1f}%|{m['title'][:20]}")
                oid, sh, _ = exec_buy(tok_no, order_p, buy_size,
                                     label, mode="aggressive")
                if oid and sh > 0:
                    actual_cost = round(sh * no_price, 2)
                    if not portfolio.has(mid):
                        pos = Position(mid, m["title"], m["typ"], tok_no,
                                      m.get("tok_yes", ""), group_key=slug)
                        pos.record_buy(no_price, actual_cost, actual_shares=sh)
                        pos.is_accumulation = True
                        pos.accum_side = "NO"
                        pos.triad_group = slug
                        portfolio.add_position(pos)
                        _governor_position_added()
                        portfolio.n_entries += 1
                    else:
                        pos = portfolio.get(mid)
                        pos.record_buy(no_price, actual_cost, actual_shares=sh)
                        pos.triad_group = slug

                    portfolio.n_accum_buys += 1
                    portfolio.total_cost += actual_cost
                    record_order(is_new=True)
                    triad_trades += 1; trades += 1

                    # Start WebSocket for this token
                    ws_start([tok_no, m.get("tok_yes", "")])

                    log.info(f"  🔺 TRIAD LEG: {m['title'][:40]} "
                            f"NO@{no_price:.3f} {sh:.0f}sh ${actual_cost:.2f} "
                            f"(combined={combined_no:.3f}/2.00 {len(owned_legs)+1}/3 legs)")

                    # Check if triad is now complete
                    if portfolio.triad_is_locked(slug):
                        gp = portfolio.triad_guaranteed_profit(slug)
                        portfolio.n_arb_locks += 1
                        log.info(f"  🔺🔒 TRIAD LOCKED! {slug[:40]} "
                                f"3/3 legs → ${gp:.2f} guaranteed!")

    if triad_trades:
        log.info(f"  🔺 TRIAD: {triad_trades} legs bought this cycle")

    if trades:
        log.info(f"  🔒 ARB ENGINE: {trades} trades this cycle")

    # Capture data snapshot (throttled to every 30s internally)
    capture_snapshot(_p2_debug_counter, cycle_price_cache, portfolio)

    return trades


# ═══════════════════════════════════════════════════════════════
# PILLAR 5B: TRIAD/DUO ARB SCANNER (OLD — DISABLED)
# ═══════════════════════════════════════════════════════════════
def scan_cross_market_arbs(all_markets):
    """
    Find guaranteed profit across related markets (parallel fetch):
    
    TRIAD (soccer): Win A NO + Win B NO + Draw NO → payout $2 guaranteed
      If combined < $1.97 → lock it (1.5%+ guaranteed)
    
    DUO (basketball/hockey): Win A NO + Win B NO → payout $1 guaranteed
      If combined < $0.97 → lock it
    
    Groups markets by event, batch fetches all NO prices, executes if profitable.
    """
    from collections import defaultdict
    event_groups = defaultdict(list)
    for m in all_markets:
        group = m.get("group", "")
        if not group or group == m["mid"]: continue
        event_groups[group].append(m)
    
    # Pre-fetch ALL tokens for all candidate groups in ONE batch
    all_tokens = []
    for event, mkts in event_groups.items():
        if len(mkts) >= 2:
            for m in mkts:
                all_tokens.append(m["tok_no"])
    
    if not all_tokens:
        return 0
    
    price_cache = smart_batch_fetch(all_tokens)
    locked = 0
    
    for event, mkts in event_groups.items():
        if len(mkts) < 2: continue
        if _pending_slots_full(): break
        if not rate_ok(is_new=True): break
        
        wins = [m for m in mkts if m["typ"] == "win"]
        draws = [m for m in mkts if m["typ"] == "draw"]
        matches = [m for m in mkts if m["typ"] == "match"]
        
        all_held = all(portfolio.has(m["mid"]) for m in mkts)
        if all_held: continue
        
        candidates = []
        if len(wins) >= 2 and len(draws) >= 1:
            candidates.append((wins[:2] + draws[:1], TRIAD_LOCK_THRESHOLD, 2.0, "TRIAD"))
        if len(wins) >= 2 and len(draws) == 0:
            candidates.append((wins[:2], DUO_LOCK_THRESHOLD, 1.0, "DUO"))
        if len(matches) >= 1 and len(wins) >= 1:
            candidates.append((matches[:1] + wins[:1], DUO_LOCK_THRESHOLD, 1.0, "DUO"))
        
        for group_mkts, threshold, payout, label in candidates:
            # Use pre-fetched prices (no serial HTTP!)
            prices = []
            for m in group_mkts:
                no_ask, depth, _, _ = price_cache.get(m["tok_no"], (None, 0, None, 0))
                if not no_ask or no_ask <= 0 or depth < 3:
                    break
                prices.append((m, no_ask))
            
            if len(prices) != len(group_mkts): continue
            
            combined = sum(p for _, p in prices)
            spread = payout - combined
            
            if combined < threshold and spread >= ARB_LOCK_MIN_SPREAD * payout:
                budget_per_leg = min(BANKROLL * ARB_MAX_PER_MARKET,
                                    portfolio.arb_room_for(prices[0][0]["mid"],
                                    event) / len(prices))
                min_shares = min(budget_per_leg / p for _, p in prices) if all(p > 0 for _, p in prices) else 0
                
                if min_shares < 5: continue
                
                total_cost = 0
                all_ok = True
                positions_created = []
                
                for m, no_ask in prices:
                    cost = no_ask * min_shares
                    if cost < 1:
                        all_ok = False; break
                    leg_label = f"🔒{label}-NO|{spread/payout*100:.1f}%|{m['title'][:25]}"
                    oid, shares, _ = exec_buy(m["tok_no"], no_ask, cost, leg_label,
                                           mode="aggressive", )
                    if not oid:
                        all_ok = False; break
                    
                    if not portfolio.has(m["mid"]):
                        pos = Position(m["mid"], m["title"], m["typ"],
                                      m["tok_no"], m["tok_yes"], event)
                        pos.record_buy(no_ask, cost)
                        pos.current_ask = no_ask
                        portfolio.add_position(pos)
                        _governor_position_added()
                        portfolio.n_entries += 1
                    else:
                        pos = portfolio.get(m["mid"])
                        pos.record_buy(no_ask, cost)
                    
                    total_cost += cost
                    positions_created.append(pos)
                    record_order(is_new=True)
                
                if all_ok and positions_created:
                    guaranteed = payout * min_shares - total_cost
                    roi = guaranteed / total_cost * 100
                    log.info(f"  🔒 {label} ARB LOCKED! {event[:40]}")
                    log.info(f"     {len(prices)} legs × {min_shares:.0f}sh = ${total_cost:.2f} "
                            f"→ ${payout*min_shares:.2f} payout")
                    log.info(f"     💰 Guaranteed: ${guaranteed:.2f} ({roi:.1f}% ROI)")
                    portfolio.n_arb_locks += 1
                    portfolio.total_cost += total_cost
                    locked += 1
    
    if locked > 0:
        log.info(f"  🔒 Locked {locked} cross-market arbs!")
    return locked

# ═══════════════════════════════════════════════════════════════
# DASHBOARD
# ═══════════════════════════════════════════════════════════════
_start_time = time.time()

def dashboard():
    os.system("clear" if os.name != "nt" else "cls")
    now_s = datetime.now().strftime("%H:%M:%S")
    runtime = (time.time() - _start_time) / 60
    pf = portfolio
    
    print(f"\033[1;96m╔═══════════════════════════════════════════════════════════════════════════╗\033[0m")
    print(f"\033[1;96m║  PolyArb v10 — FULL RN1 STRATEGY   {now_s}  {'DRY' if DRY_RUN else 'LIVE':>4s}  ⏱ {runtime:.0f}m       ║\033[0m")
    print(f"\033[1;96m╚═══════════════════════════════════════════════════════════════════════════╝\033[0m")
    
    deployed = pf.deployed()
    est_val = pf.est_value()
    active = pf.active_count()
    unreal_pnl = est_val - deployed
    locked_list = [p for p in pf.positions.values() if p.is_arb_locked]
    locked_total = sum((p.guaranteed_profit or 0) for p in locked_list)
    
    print(f"  💰 Bank: ${BANKROLL:,.0f}  Deployed: ${deployed:,.2f} ({deployed/max(BANKROLL,1)*100:.0f}%)  "
          f"Est Value: ${est_val:,.2f}  Unrealized: ${unreal_pnl:+,.2f}")
    if locked_list:
        print(f"  🔒 Locked: {len(locked_list)} positions  Guaranteed: \033[92m${locked_total:+,.2f}\033[0m")
    
    accum_pos = [p for p in pf.positions.values() if p.is_accumulation and not p.is_arb_locked]
    accum_deployed = pf.accum_deployed()
    if accum_pos:
        print(f"  📦 Accum: {len(accum_pos)}/{ACCUM_MAX_OPEN} slots  ${accum_deployed:,.2f} deployed  "
              f"({pf.n_accum_buys} buys, {pf.n_accum_completed} completed→arb)")

    print(f"  📊 Positions: {active}  Entries: {pf.n_entries}  "
          f"Hedges: {pf.n_profit_takes}  Cuts: {pf.n_loss_cuts}  Adds: {pf.n_dip_buys}  "
          f"🔒Arbs: {pf.n_arb_locks}  "
          f"Realized: ${pf.total_realized:,.2f}")
    
    # Type breakdown (exclude wallet positions)
    type_counts = defaultdict(int)
    type_cost = defaultdict(float)
    for p in pf.positions.values():
        if p.shares > 0 and not p.from_wallet:
            type_counts[p.typ] += 1
            type_cost[p.typ] += p.total_cost
    
    if type_counts:
        print(f"\n  \033[1mBY TYPE:\033[0m")
        for typ in sorted(type_counts, key=lambda t: -type_counts[t]):
            print(f"    {typ:<10s}: {type_counts[typ]:>3d} pos  ${type_cost[typ]:>8,.2f}")
    
    # Top positions by value (exclude wallet positions — user manages those)
    active_pos = sorted([p for p in pf.positions.values()
                         if p.shares > 0 and not p.from_wallet],
                        key=lambda p: -p.total_cost)
    locked_pos = [p for p in active_pos if p.is_arb_locked]
    open_pos = [p for p in active_pos if not p.is_arb_locked]
    
    # Total guaranteed profit across all locked positions
    total_guaranteed = sum(p.guaranteed_profit or 0 for p in locked_pos)
    
    if locked_pos:
        print(f"\n  \033[92;1m🔒 ARB-LOCKED POSITIONS ({len(locked_pos)}) "
              f"— Guaranteed: ${total_guaranteed:+.2f}\033[0m")
        print(f"  {'Market':<34s} {'NO':>10s} {'YES':>10s} {'Cost':>7s} {'G.Profit':>9s} {'ROI':>6s}")
        for p in locked_pos[:8]:
            gp = p.guaranteed_profit or 0
            roi = p.guaranteed_roi * 100
            clr = "\033[92m" if gp > 0 else "\033[91m"
            print(f"  {p.short():<34s} "
                  f"{p.shares:.0f}@{p.avg_price:.2f} "
                  f"{p.yes_shares:.0f}@{p.yes_avg_price:.2f} "
                  f"${p.total_cost:>6.2f} "
                  f"{clr}${gp:>+7.2f} {roi:>+5.1f}%\033[0m")
    
    if open_pos:
        print(f"\n  \033[1mOPEN POSITIONS ({len(open_pos)}):\033[0m")
        print(f"  {'Market':<38s} {'Type':>7s} {'Cost':>8s} {'Bid':>6s} {'P&L':>7s} {'Buys':>4s}")
        for p in open_pos[:25]:
            pnl = p.pnl_pct * 100
            clr = "\033[92m" if pnl > 10 else ("\033[91m" if pnl < -20 else "\033[0m")
            bid_s = f"@{p.current_bid:.3f}" if p.current_bid else "  ???"
            print(f"  {p.short():<38s} {p.typ:>7s} ${p.cost:>7.2f} {bid_s} "
                  f"{clr}{pnl:>+6.1f}%\033[0m {p.n_buys:>4d}")
        if len(open_pos) > 25:
            print(f"  \033[2m  ... and {len(open_pos) - 25} more positions\033[0m")
    
    # Accumulation positions
    if accum_pos:
        print(f"\n  \033[93;1m📦 ACCUMULATION POSITIONS ({len(accum_pos)}) "
              f"— waiting for other side\033[0m")
        print(f"  {'Market':<34s} {'Side':>4s} {'Shares':>7s} {'Avg':>6s} {'Cost':>7s} {'Other Ask':>9s} {'Comb':>6s}")
        for p in sorted(accum_pos, key=lambda x: -x.total_cost)[:10]:
            if p.accum_side == "NO":
                other_ask = p.current_yes_ask or 0
                comb = p.avg_price + other_ask if other_ask else 0
                shares_s = f"{p.shares:.0f}"
                avg_s = f"@{p.avg_price:.3f}"
            else:
                other_ask = p.current_ask or 0
                comb = other_ask + p.yes_avg_price if other_ask else 0
                shares_s = f"{p.yes_shares:.0f}"
                avg_s = f"@{p.yes_avg_price:.3f}"
            other_s = f"@{other_ask:.3f}" if other_ask else "  ???"
            comb_s = f"{comb:.3f}" if comb > 0 else "  ???"
            clr = "\033[92m" if comb > 0 and comb < ACCUM_COMPLETE_THRESHOLD else "\033[0m"
            print(f"  {p.short():<34s} {p.accum_side:>4s} {shares_s:>7s} {avg_s:>6s} "
                  f"${p.total_cost:>6.2f} {other_s:>9s} {clr}{comb_s:>6s}\033[0m")

    # Near arbs (positions that could be locked)
    near_arbs = [(p, p.avg_price + (p.current_yes_ask or 1))
                 for p in open_pos
                 if p.tok_yes and p.current_yes_ask and p.avg_price > 0
                 and not p.is_arb_locked and not p.is_accumulation
                 and (p.avg_price + p.current_yes_ask) < 1.03]
    near_arbs.sort(key=lambda x: x[1])
    
    if near_arbs:
        print(f"\n  \033[1mNEAR ARB OPPORTUNITIES:\033[0m")
        for p, comb in near_arbs[:5]:
            gap = (comb - ARB_LOCK_THRESHOLD) * 100
            print(f"  \033[96m  🔒 {p.short():<30s} "
                  f"combined={comb:.3f} ({gap:+.1f}% to lock)\033[0m")
    
    churn = (pf.total_cost + pf.total_realized) / max(BANKROLL, 1)
    disc_age = time.time() - _last_discovery_time if _last_discovery_time > 0 else 999
    print(f"\n  {'═'*70}")
    print(f"  Churn: {churn:.1f}x bankroll  |  Rate: {len(_order_ts)}/min  {len(_new_mkt_ts)}/hr new")
    print(f"  🔍 Discovery: {_last_discovery_count} markets  |  {disc_age:.0f}s ago  |  every {_discovery_interval}s  "
          f"⚡ +{_newmarket_added} fresh")

# ═══════════════════════════════════════════════════════════════
# BACKGROUND MARKET DISCOVERY THREAD
# ═══════════════════════════════════════════════════════════════
# Runs discover_markets() in a background thread so the arb loop
# NEVER blocks waiting for HTTP. The main loop reads _shared_markets
# which gets atomically swapped by the background thread.
_shared_markets = []           # thread-safe: list assignment is atomic in CPython
_p2_debug_counter = 0          # Phase 2 diagnostic counter (global for arb_engine)
_shared_markets_lock = threading.Lock()
_discovery_running = False
_discovery_interval = 10       # seconds between full discoveries (heavy Gamma API call — don't go below 10)
_last_discovery_count = 0
_last_discovery_time = 0.0

def _discovery_worker():
    """Background thread: continuously discovers markets and updates shared list."""
    global _shared_markets, _discovery_running, _last_discovery_count, _last_discovery_time
    log.info("🔍 Background discovery thread started (every %ds)", _discovery_interval)
    while _discovery_running:
        try:
            new_markets = discover_markets()
            if new_markets:
                with _shared_markets_lock:
                    _shared_markets = new_markets
                _last_discovery_count = len(new_markets)
                _last_discovery_time = time.time()
                # Subscribe new tokens to WebSocket + update token→market map
                new_ws = []
                for m in new_markets:
                    new_ws.append(m["tok_no"])
                    _ws_token_to_market[m["tok_no"]] = m
                    if m.get("tok_yes"):
                        new_ws.append(m["tok_yes"])
                        _ws_token_to_market[m["tok_yes"]] = m
                ws_start(new_ws)
        except Exception as e:
            log.error(f"Discovery thread error: {e}")
        time.sleep(_discovery_interval)
    log.info("🔍 Background discovery thread stopped")

def start_discovery_thread():
    """Start the background market discovery thread."""
    global _discovery_running
    _discovery_running = True
    t = threading.Thread(target=_discovery_worker, daemon=True, name="discovery")
    t.start()
    return t

def get_shared_markets():
    """Get current market list (thread-safe read)."""
    with _shared_markets_lock:
        return list(_shared_markets)  # return a copy

# ═══════════════════════════════════════════════════════════════
# FAST NEW-MARKET SCANNER — catches freshly-created markets in <5s
# ═══════════════════════════════════════════════════════════════
# Fresh markets have the juiciest mispricing — market makers haven't
# converged yet. RN1 likely has a fast path to new listings.
# This lightweight thread checks newest-created markets every 5s
# and merges them into the shared market list.
_newmarket_interval = 3  # seconds — just 1 API call, very lightweight (was 5)
_newmarket_seen = set()  # track what we've already found
_newmarket_added = 0

def _newmarket_worker():
    """Background thread: fast scan for newly-created markets."""
    global _shared_markets, _newmarket_added
    log.info("⚡ New-market scanner started (every %ds)", _newmarket_interval)
    now_utc = datetime.now(timezone.utc)

    while _discovery_running:
        try:
            # Fetch newest markets sorted by creation time (descending)
            url = (f"{GAMMA}/markets?limit=50&offset=0"
                   f"&active=true&closed=false"
                   f"&order=startDate&ascending=false")
            data = api_get(url, timeout=8)
            if not data or not isinstance(data, list):
                time.sleep(_newmarket_interval)
                continue

            now_utc = datetime.now(timezone.utc)
            new_found = []

            for m in data:
                mid = m.get("id", "")
                if not mid or mid in _newmarket_seen:
                    continue
                _newmarket_seen.add(mid)

                if not m.get("active") or m.get("closed"):
                    continue

                # End date check — same smart logic as discover_markets()
                end_str = m.get("endDate") or m.get("end_date_iso") or ""
                max_hours = MAX_DAYS_OUT * 24
                hours_left = max_hours
                title_nw = m.get("question", "") or m.get("title", "")
                if end_str:
                    try:
                        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                        hours_left = (end_dt - now_utc).total_seconds() / 3600
                        if hours_left < -1:
                            continue
                        if hours_left > max_hours:
                            # Smart date extraction from title
                            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', title_nw)
                            if date_match:
                                try:
                                    match_dt = datetime.fromisoformat(date_match.group(1) + "T23:59:59+00:00")
                                    match_hours = (match_dt - now_utc).total_seconds() / 3600
                                    if match_hours <= max_hours and match_hours >= -24:
                                        hours_left = match_hours
                                    else:
                                        continue
                                except (ValueError, TypeError):
                                    continue
                            else:
                                # Sports keywords = event market, accept
                                tl_nw = title_nw.lower()
                                if ('vs' in tl_nw or 'end in a draw' in tl_nw
                                        or 'o/u' in tl_nw or ' win on ' in tl_nw
                                        or ' beat ' in tl_nw or ' win ' in tl_nw
                                        or 'over/under' in tl_nw or 'spread:' in tl_nw):
                                    hours_left = 48
                                else:
                                    continue
                    except (ValueError, TypeError):
                        pass

                clob_ids = m.get("clobTokenIds", [])
                if isinstance(clob_ids, str):
                    try: clob_ids = json.loads(clob_ids)
                    except (json.JSONDecodeError, TypeError): clob_ids = []
                if not isinstance(clob_ids, list) or len(clob_ids) < 2:
                    continue

                title = m.get("question", "") or m.get("title", "")
                # STRICT SPORTS GATE — only sports markets
                if not is_sports_market(title, end_str):
                    continue

                # No volume filter for new markets — they're brand new!
                typ = classify(title)
                vol24 = float(m.get("volume24hr") or m.get("volume_24h") or 0)

                new_found.append({
                    "mid": mid, "title": title, "typ": typ,
                    "is_sports": True,
                    "tok_yes": str(clob_ids[0]), "tok_no": str(clob_ids[1]),
                    "volume": float(m.get("volume", 0) or 0),
                    "volume24": vol24,
                    "liquidity": float(m.get("liquidity", 0) or 0),
                    "group": m.get("groupItemTitle", "") or m.get("conditionId", "") or mid,
                    "event_slug": m.get("event_slug", m.get("groupSlug", "")),
                    "hours_left": hours_left,
                })

            if new_found:
                with _shared_markets_lock:
                    # Merge into existing list (avoid duplicates by mid)
                    existing_mids = {m["mid"] for m in _shared_markets}
                    added = [m for m in new_found if m["mid"] not in existing_mids]
                    if added:
                        _shared_markets = _shared_markets + added
                        _newmarket_added += len(added)
                        log.info(f"  ⚡ NEW MARKETS: +{len(added)} fresh listings "
                                 f"(total tracked: {_newmarket_added})")
                        for nm in added[:3]:
                            log.info(f"    ⚡ {nm['typ']:<8s} {nm['title'][:50]}")
                        # Subscribe to WS immediately + update token→market map
                        ws_toks = []
                        for nm in added:
                            ws_toks.append(nm["tok_no"])
                            _ws_token_to_market[nm["tok_no"]] = nm
                            if nm.get("tok_yes"):
                                ws_toks.append(nm["tok_yes"])
                                _ws_token_to_market[nm["tok_yes"]] = nm
                        if ws_toks:
                            ws_start(ws_toks)

        except Exception as e:
            log.debug(f"New-market scanner error: {e}")
        time.sleep(_newmarket_interval)
    log.info("⚡ New-market scanner stopped")

def start_newmarket_thread():
    """Start the fast new-market scanner thread."""
    t = threading.Thread(target=_newmarket_worker, daemon=True, name="newmarket")
    t.start()
    return t

# ═══════════════════════════════════════════════════════════════
# BATCH ORDER EXECUTION — post_orders() for multiple orders in 1 call
# ═══════════════════════════════════════════════════════════════
def exec_batch_buy(orders_list, label_prefix="", mode="aggressive", is_completion=False):
    """Execute multiple buy orders in a SINGLE API call using post_orders().

    orders_list: [(token_id, price, size_usd, label), ...] or
                 [(token_id, price, size_usd, label, target_shares), ...]
    is_completion: True if buying 2nd side to lock arb (bypasses spend governor)
    Returns: [(order_id, shares_filled, actual_price), ...] same length as input
    """
    if not orders_list:
        return []

    # ─── HARD DEPLOYMENT CAP — prevent runaway buying ───
    max_deploy = BANKROLL * MAX_DEPLOY_PCT
    if portfolio and portfolio.total_cost >= max_deploy:
        log.warning(f"  🛑 HARD CAP: total deployed ${portfolio.total_cost:.2f} >= ${max_deploy:.2f} — refusing batch buy")
        return [(None, 0, 0)] * len(orders_list)

    results = []

    if DRY_RUN:
        for order in orders_list:
            token_id, price, size_usd, label = order[0], order[1], order[2], order[3]
            target_shares = order[4] if len(order) > 4 else None
            if not price or price <= 0:
                results.append((None, 0, 0))
                continue
            # ─── HARD PER-ORDER CAP — no single order can exceed LIVE_MAX_BUY ───
            if size_usd > LIVE_MAX_BUY:
                size_usd = LIVE_MAX_BUY
                target_shares = None  # recalculate from capped size
            # ─── GLOBAL GOVERNOR ───
            size_usd = _governor_allow_buy(size_usd, is_completion=is_completion)
            if size_usd < 1:
                results.append((None, 0, 0))
                continue
            price = round(price, 2)
            shares = math.floor(target_shares) if target_shares else math.floor(size_usd / price)
            if shares < 5:
                results.append((None, 0, 0))
                continue
            log.info(f"  📗 BATCH BUY @{price:.2f} ${size_usd:.2f} ({shares:.0f}sh) | {label}")
            _governor_record(size_usd)
            results.append((f"dry_{int(time.time()*1000)}", shares, price))
        return results

    try:
        from py_clob_client.order_builder.constants import BUY
        from py_clob_client.clob_types import OrderArgs, MarketOrderArgs, OrderType, PostOrdersArgs
        c = get_clob()

        # Build all signed orders
        signed_orders = []
        order_meta = []  # track price/shares for each order
        for order in orders_list:
            token_id, price, size_usd, label = order[0], order[1], order[2], order[3]
            target_shares = order[4] if len(order) > 4 else None
            if not price or price <= 0:
                order_meta.append(None)
                continue
            # ─── HARD PER-ORDER CAP — no single order can exceed LIVE_MAX_BUY ───
            if size_usd > LIVE_MAX_BUY:
                size_usd = LIVE_MAX_BUY
                target_shares = None  # recalculate from capped size
            # ─── GLOBAL GOVERNOR ───
            size_usd = _governor_allow_buy(size_usd, is_completion=is_completion)
            if size_usd < 1:
                order_meta.append(None)
                continue
            # CRITICAL: integer shares × 2-decimal price = clean maker_amount (≤ 2 decimals)
            price = round(price, 2)
            shares = math.floor(target_shares) if target_shares else math.floor(size_usd / price)
            if shares < 5:
                _governor_refund(size_usd)  # Governor approved but too few shares → refund
                order_meta.append(None)
                continue

            if mode == "aggressive":
                # FAK: market order — partial fills OK
                fak_args = MarketOrderArgs(
                    token_id=token_id,
                    amount=round(size_usd, 2),
                    side=BUY
                )
                signed = c.create_market_order(fak_args)
                signed_orders.append(PostOrdersArgs(order=signed, orderType=OrderType.FAK))
            else:
                args = OrderArgs(
                    token_id=token_id,
                    price=price,
                    size=float(shares),
                    side=BUY
                )
                signed = c.create_order(args)
                signed_orders.append(PostOrdersArgs(order=signed, orderType=OrderType.GTC))
            order_meta.append({"price": price, "shares": shares, "size_usd": size_usd, "label": label})

        if not signed_orders:
            return [(None, 0, 0)] * len(orders_list)

        # SINGLE API call for ALL orders
        resp = c.post_orders(signed_orders)
        log.debug(f"  BATCH resp: {json.dumps(resp) if resp else 'None'}")

        # Parse response — post_orders returns list of order results
        # Each result has same structure as post_order: orderID, status, takingAmount, makingAmount
        def _safe_float(v):
            if v is None or v == "" or v == "0": return 0.0
            try: return float(v)
            except (ValueError, TypeError): return 0.0

        # Extract per-order results from response
        order_results = []
        if isinstance(resp, list):
            order_results = resp
        elif isinstance(resp, dict):
            # Some API versions wrap in {"orders": [...]} or return single result
            order_results = resp.get("orders", resp.get("results", []))
            if not order_results and resp.get("orderID"):
                order_results = [resp]  # single order response

        signed_idx = 0  # index into signed_orders / order_results
        for i, om in enumerate(order_meta):
            if om is None:
                results.append((None, 0, 0))
                continue

            # Get matching API response for this order
            order_resp = order_results[signed_idx] if signed_idx < len(order_results) else {}
            signed_idx += 1

            oid = order_resp.get("orderID", f"batch_{int(time.time()*1000)}_{i}")
            status = order_resp.get("status", "")

            if status in ("CANCELLED", "EXPIRED", "REJECTED"):
                log.warning(f"  ⚠️ BATCH order {status}: {om['label']}")
                _governor_refund(om['size_usd'])  # Order didn't fill → refund governor
                results.append((oid, 0, 0))
                continue

            # Parse fill data — CRITICAL: for BUY orders:
            #   makingAmount = USDC spent, takingAmount = shares received
            making = _safe_float(order_resp.get("makingAmount"))  # USDC spent
            taking = _safe_float(order_resp.get("takingAmount"))  # shares received
            matched = _safe_float(order_resp.get("matchedAmount") or
                                  order_resp.get("filled") or
                                  order_resp.get("filledSize"))

            # Check for API error in response — order may have error but still show up
            error_msg = order_resp.get("errorMsg", "")
            if error_msg:
                # FAK "no match" is normal — just means no liquidity at our price.
                # Don't log as warning (shows up in dashboard errors), use info instead.
                if "no orders found to match" in error_msg.lower() or "killed if no match" in error_msg.lower():
                    log.info(f"  📭 FAK NO FILL (empty book): {om['label']}")
                else:
                    log.warning(f"  ⚠️ BATCH ORDER ERROR: {error_msg} | {om['label']}")
                _governor_refund(om['size_usd'])  # Error/no-fill → refund governor
                results.append((None, 0, 0))
                continue

            actual_shares = 0
            actual_cost = 0
            if taking > 0:
                actual_shares = taking
                actual_cost = making if making > 0 else round(taking * om['price'], 2)
                log.info(f"  📗 BATCH BUY @{om['price']:.2f} "
                        f"filled={actual_shares:.1f}sh ${actual_cost:.2f} | {om['label']}")
            elif making > 0:
                actual_cost = making
                actual_shares = making / om['price']
                log.info(f"  📗 BATCH BUY @{om['price']:.2f} "
                        f"filled={actual_shares:.1f}sh ${actual_cost:.2f} | {om['label']}")
            elif matched > 0:
                if matched > om['shares'] * 2:
                    actual_shares = matched / om['price']
                    actual_cost = matched
                else:
                    actual_shares = matched
                    actual_cost = round(matched * om['price'], 2)
                log.info(f"  📗 BATCH BUY @{om['price']:.2f} "
                        f"matched={actual_shares:.1f}sh ${actual_cost:.2f} | {om['label']}")
            else:
                if mode == "aggressive":
                    # FOK with 'delayed' — VERIFY via get_order() API
                    log.info(f"  📗 BATCH BUY @{om['price']:.2f} ${om['size_usd']:.2f} "
                            f"({om['shares']:.0f}sh) [DELAYED — verifying...] | {om['label']}")
                    try:
                        # Retry loop for batch orders too
                        od = None
                        for _br_i, _br_delay in enumerate([0.3, 1.0, 2.0]):
                            time.sleep(_br_delay)
                            od = c.get_order(oid)
                            try:
                                _od_dump = json.dumps(od, default=str) if od else "None"
                            except Exception:
                                _od_dump = repr(od)
                            log.info(f"  🔎 BATCH VERIFY attempt {_br_i+1}: {_od_dump[:500]}")
                            if od and isinstance(od, dict):
                                break
                            log.info(f"  ⏳ batch get_order returned {type(od).__name__}, retrying...")

                        if od and isinstance(od, dict):
                            sm = _safe_float(od.get("size_matched", 0))
                            od_st = od.get("status", "")
                            od_pr = _safe_float(od.get("price", 0))
                            if sm > 0 or od_st == "MATCHED":
                                actual_shares = sm if sm > 0 else om['shares']
                                if actual_shares > om['shares'] * 1.05:
                                    # Accept real shares — capping causes position mismatch
                                    # with on-chain balance. Position sync will reconcile.
                                    log.warning(f"  ⚠️ BATCH OVERFILL: got {actual_shares:.1f}sh "
                                               f"but requested {om['shares']:.0f}sh — using REAL amount | {om['label']}")
                                # od_pr from get_order() can return wrong price — cap at limit
                                if od_pr > 0 and od_pr <= om['price'] * 1.02:
                                    fill_price = od_pr
                                else:
                                    fill_price = om['price']
                                    if od_pr > 0:
                                        log.warning(f"  ⚠️ BATCH od_price={od_pr:.3f} >> limit={om['price']:.3f}"
                                                   f" — using limit | {om['label']}")
                                actual_cost = round(actual_shares * fill_price, 2)
                                log.info(f"  ✅ BATCH VERIFIED FILL: {actual_shares:.1f}sh "
                                        f"${actual_cost:.2f} (status={od_st}) | {om['label']}")
                            elif od_st in ("CANCELLED", "EXPIRED"):
                                log.info(f"  ❌ BATCH VERIFIED NO FILL: status={od_st} | {om['label']}")
                                _governor_refund(om['size_usd'])
                            else:
                                # Any other status — assume filled (conservative)
                                log.warning(f"  ⚠️ BATCH AMBIGUOUS '{od_st}' sm={sm} — assuming filled | {om['label']}")
                                actual_shares = sm if sm > 0 else om['shares']
                                actual_cost = round(actual_shares * om['price'], 2)
                        else:
                            # None after retries — treat as no fill
                            log.warning(f"  ❌ BATCH get_order None after retries — treating as NO FILL | {om['label']}")
                            _governor_refund(om['size_usd'])
                            actual_shares = 0
                            actual_cost = 0
                    except Exception as ve:
                        log.warning(f"  ❌ BATCH VERIFY FAILED: {ve} — treating as NO FILL | {om['label']}")
                        _governor_refund(om['size_usd'])
                        actual_shares = 0
                        actual_cost = 0
                else:
                    log.info(f"  📗 BATCH BUY @{om['price']:.2f} ${om['size_usd']:.2f} "
                            f"({om['shares']:.0f}sh) [PENDING VERIFY] | {om['label']}")
                    # GTC: don't refund — may fill later

            if actual_shares > 0:
                _governor_record(actual_cost if actual_cost > 0 else om['size_usd'])
            results.append((oid, actual_shares, om['price']))

        # Pad results if some were skipped
        while len(results) < len(orders_list):
            results.append((None, 0, 0))

        return results
    except Exception as e:
        log.error(f"  BATCH BUY ERR: {e}")
        # Refund ALL governor approvals from this batch — they all failed
        for om in order_meta:
            if om is not None:
                _governor_refund(om['size_usd'])
        return [(None, 0, 0)] * len(orders_list)


# ═══════════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════════
def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--bankroll", type=float, default=1000)
    p.add_argument("--fresh", action="store_true")
    p.add_argument("--live", action="store_true")
    p.add_argument("--scan", action="store_true", help="Scan only, no trades")
    p.add_argument("--turbo", action="store_true", help="Faster cycles")
    p.add_argument("--no-dash", action="store_true")
    p.add_argument("--accum", action="store_true", help="Enable live event accumulation (default: ON)")
    p.add_argument("--no-live", action="store_true", help="Disable live event tracker")
    args = p.parse_args()

    global BANKROLL, DRY_RUN, args_scan_mode, LIVE_ENABLED, ACCUM_ENABLED
    BANKROLL = args.bankroll
    if args.live: DRY_RUN = False
    if args.accum: LIVE_ENABLED = True; ACCUM_ENABLED = True
    if getattr(args, 'no_live', False): LIVE_ENABLED = False; ACCUM_ENABLED = False
    args_scan_mode = args.scan
    # --turbo is legacy flag, arb engine already runs at max speed (50ms cycle)
    
    if args.fresh:
        for f in ["portfolio_v10.json", "ledger.json"]:
            if os.path.exists(f): os.remove(f)
    else:
        portfolio.load()
    
    # Always try wallet sync (works in live mode, gracefully fails in dry)
    if not args.scan:
        portfolio.sync_wallet()
    
    running = True
    def _sig(s, f):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)
    
    log.info("=" * 60)
    log.info(f"PolyArb v10 — PURE ARB ENGINE")
    log.info(f"  Bankroll: ${BANKROLL:,.0f}  Mode: {'DRY' if DRY_RUN else 'LIVE'}  "
             f"Turbo: {args.turbo}  Scan: {args.scan}")
    log.info(f"  Arb limits: {ARB_MAX_PER_MARKET*100:.0f}%/market {ARB_MAX_PER_EVENT*100:.0f}%/event "
             f"(${BANKROLL*ARB_MAX_PER_MARKET:.0f}/${BANKROLL*ARB_MAX_PER_EVENT:.0f})")
    log.info(f"  Arb threshold: YES+NO < {ARB_LOCK_THRESHOLD} (min {ARB_LOCK_MIN_SPREAD*100:.1f}% spread)")
    log.info(f"  Slippage buffer: +${ARB_SLIPPAGE:.3f} above ask for guaranteed fills")
    log.info(f"  Live Tracker: {'ON' if LIVE_ENABLED else 'OFF'} — "
             f"max {LIVE_MAX_PENDING} pending × ${LIVE_MIN_BUY:.0f}-${LIVE_MAX_BUY:.0f}/buy, "
             f"min score={LIVE_MIN_SCORE_ENTRY}, override={LIVE_MAX_PENDING_OVERRIDE}, "
             f"target < {LIVE_TARGET:.2f}, warmup {LIVE_WARMUP_SECS}s")
    log.info(f"  Strategy: INSTANT ARB + LIVE DIP ACCUMULATION (RN1 reverse-engineered)")
    log.info(f"  Orders: AGGRESSIVE at ask+slip — speed + guaranteed fills")
    log.info("=" * 60)
    
    # ─── INITIAL DISCOVERY (blocking, so user sees output) ───
    log.info("Running initial market discovery...")
    global _shared_markets
    initial_markets = discover_markets()
    if initial_markets:
        with _shared_markets_lock:
            _shared_markets = initial_markets
        log.info(f"  Ready to trade {len(initial_markets)} sports markets! (soonest-ending first)")
        for m in initial_markets[:5]:
            h = m.get("hours_left", 99)
            time_s = f"LIVE" if h <= 0 else (f"{h:.1f}h" if h < 24 else f"{h/24:.0f}d")
            log.info(f"    → {m['typ']:<8s} {time_s:>5s}  {m['title'][:50]}")
        # Start WebSocket for real-time prices + build token→market map for instant arb detection
        ws_tokens = []
        for m in initial_markets:
            ws_tokens.append(m["tok_no"])
            _ws_token_to_market[m["tok_no"]] = m
            if m.get("tok_yes"):
                ws_tokens.append(m["tok_yes"])
                _ws_token_to_market[m["tok_yes"]] = m
        ws_start(ws_tokens)
        log.info(f"  🗺️ Token→market map: {len(_ws_token_to_market)} tokens mapped for WS arb detection")

        # Subscribe existing portfolio positions to WebSocket too — on restart,
        # portfolio positions are already in portfolio so they're excluded from new_markets
        # scanning. Without this, they never get WS subscriptions and the hammer/tracker
        # can't get real-time momentum data for them.
        portfolio_ws_tokens = []
        for mid, pos in portfolio.positions.items():
            if pos.tok_no and pos.tok_no not in _ws_subscribed:
                portfolio_ws_tokens.append(pos.tok_no)
            if pos.tok_yes and pos.tok_yes not in _ws_subscribed:
                portfolio_ws_tokens.append(pos.tok_yes)
        if portfolio_ws_tokens:
            ws_start(portfolio_ws_tokens)
            log.info(f"  🔌 Subscribed {len(portfolio_ws_tokens)} portfolio position tokens to WS")
    else:
        log.warning("  ⚠ NO SPORTS MARKETS FOUND — check logs/v10_*.log for details")

    # ─── START ODDS ENGINE (external sportsbook odds) ───
    if _odds_available and _odds_engine:
        # Set API key from env if not already set
        if not _odds_engine.api_key:
            _odds_engine.api_key = os.getenv("THE_ODDS_API_KEY", "")
        _odds_engine.start_background()
    else:
        log.info("  📊 Odds engine: unavailable (install odds_engine.py or set THE_ODDS_API_KEY)")

    # ─── START BACKGROUND DISCOVERY (never blocks arb loop again) ───
    start_discovery_thread()
    log.info(f"  🔍 Background discovery: every {_discovery_interval}s (non-blocking)")

    # ─── START FAST NEW-MARKET SCANNER ───
    start_newmarket_thread()
    log.info(f"  ⚡ New-market scanner: every {_newmarket_interval}s (catches fresh listings)")

    # ─── INITIALIZE DATA COLLECTION ───
    try:
        init_game_data()
        init_audit_log()
    except Exception as e:
        log.warning(f"  ⚠ DATA: Failed to init game data: {e}")

    last_save = 0
    last_dash = time.time()
    _main_loop_diag = 0  # counter for periodic main-loop diagnostics

    time.sleep(2)  # brief pause so user can read discovery output

    while running:
      try:
        now = time.time()

        # ─── GET LATEST MARKETS (non-blocking read from background thread) ───
        all_markets = get_shared_markets()

        # ─── MAIN LOOP DIAGNOSTIC (every ~10s = 200 cycles) ───
        _main_loop_diag += 1
        if _main_loop_diag % 200 == 0:
            if not all_markets:
                log.warning(f"  ⚠ MAIN LOOP: get_shared_markets() returned EMPTY list!")
            elif _main_loop_diag % 1000 == 0:  # every ~50s
                log.info(f"  🔄 MAIN LOOP: {len(all_markets)} markets, "
                         f"{portfolio.active_count()} positions, "
                         f"${portfolio.deployed():.2f} deployed")

        # ─── ARB ENGINE — runs EVERY cycle, never waits for discovery ───
        if all_markets and not args.scan:
            arb_engine(all_markets)
            # DISABLED: scan_cross_market_arbs() — FATALLY FLAWED.
            # It groups markets by groupItemTitle (e.g. "FC Bayern München") and
            # assumes outcomes are mutually exclusive. But "Bayern win Mar 6" and
            # "Bayern win Mar 10" are DIFFERENT GAMES — both NOs can go to $0.
            # Only safe arbs are BINARY (YES+NO < $1 within the SAME market).
            # scan_cross_market_arbs(all_markets)

        # ─── SCAN MODE: batch price-check (no trading) ───
        if args.scan and all_markets:
            all_tokens = []
            for m in all_markets:
                all_tokens.append(m["tok_no"])
                if m.get("tok_yes"): all_tokens.append(m["tok_yes"])
            cache = smart_batch_fetch(all_tokens)
            arb_found = 0
            scan_all_combined = []
            scan_no_price = 0
            for m in all_markets:
                no_ask, _, no_bid, _ = cache.get(m["tok_no"], (None, 0, None, 0))
                yes_ask, _, _, _ = cache.get(m.get("tok_yes", ""), (None, 0, None, 0))
                if no_ask and yes_ask and yes_ask > 0.001:
                    combined = no_ask + yes_ask
                    scan_all_combined.append((combined, m["title"][:45], no_ask, yes_ask))
                    if combined <= ARB_LOCK_THRESHOLD:
                        spread = (1.0 - combined) * 100
                        log.info(f"  🔒 ARB: {m['title'][:45]} "
                                f"NO@{no_ask:.3f}+YES@{yes_ask:.3f}={combined:.3f} "
                                f"({spread:.1f}%)")
                        arb_found += 1
                else:
                    scan_no_price += 1
            if arb_found:
                log.info(f"  Found {arb_found} arb opportunities in {len(all_markets)} markets")
            else:
                log.info(f"  No arbs found in {len(all_markets)} markets "
                         f"({len(scan_all_combined)} priced, {scan_no_price} no-price)")
            # Always show top 5 closest combined
            scan_all_combined.sort(key=lambda x: x[0])
            log.info(f"  📊 TOP 5 closest to arb (need <={ARB_LOCK_THRESHOLD}):")
            for c, title, na, ya in scan_all_combined[:5]:
                gap = c - ARB_LOCK_THRESHOLD
                marker = "✅" if c <= ARB_LOCK_THRESHOLD else "❌"
                log.info(f"    {marker} {c:.4f} (+{gap:.4f}) NO@{na:.3f}+YES@{ya:.3f} {title}")
            if scan_all_combined:
                log.info(f"  📊 Range: best={scan_all_combined[0][0]:.4f} "
                         f"worst={scan_all_combined[-1][0]:.4f} "
                         f"median={scan_all_combined[len(scan_all_combined)//2][0]:.4f}")

        # ─── SAVE + VERIFY + CLEANUP every 30s ───
        if now - last_save >= 10:  # was 30s — catch GTC fills faster for position tracking
            if not args.scan and not DRY_RUN:
                verify_positions()  # sync real fills from wallet
                cancel_stale_orders(max_age_minutes=5)  # clean up unfilled GTC orders
            portfolio.save()
            # ZOMBIE CLEANUP: remove positions with 0 shares on both sides.
            # These accumulate from write-offs, trims, and resolved positions.
            zombies = [mid for mid, p in portfolio.positions.items()
                       if p.shares <= 0 and p.yes_shares <= 0]
            for mid in zombies:
                del portfolio.positions[mid]
            if zombies:
                log.info(f"  🧹 CLEANUP: removed {len(zombies)} empty positions")
            # Update live game scores from ESPN
            if _game_scores_available and _game_score_fetcher:
                try:
                    _game_score_fetcher.update()
                except Exception:
                    pass
            # Export live tracker state for dashboard
            _export_live_tracker()
            last_save = now

        # ─── DASHBOARD every 5s ───
        if not args.no_dash and now - last_dash >= 5:
            dashboard()
            last_dash = now

        time.sleep(0.05)  # 50ms cycle = arb checks never blocked by discovery
      except KeyboardInterrupt:
        break
      except Exception as _cycle_err:
        log.error(f"  💥 MAIN CYCLE CRASH: {_cycle_err}", exc_info=True)
        time.sleep(2)  # prevent tight crash loop
        continue  # keep running — don't let one cycle kill the bot

    # Stop background threads
    global _discovery_running, _ws_running
    _discovery_running = False
    _ws_running = False
    portfolio.save()
    log.info(f"SHUTDOWN | Positions: {portfolio.active_count()} "
             f"Deployed: ${portfolio.deployed():.2f} Realized: ${portfolio.total_realized:.2f}")

if __name__ == "__main__":
    main()
