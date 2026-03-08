I have been working on a Polymarket Sports Arbitrage bot for about a month now, been using mostly vibe coding with claude Co-Work. 
I have run into some hiccups and speed bumps as i have gotten it to do a lot of things that i WANT, but it is also doing things that i dont want. lol
I home some python experience, but not enough to build a bot this complex, so the help has been nice, but i could use help from real humans.
I understand if you can make this work you probably wouldn't want to share with people, but heres all i got. 
Right now in laymans terms, the problem i am runnning across, it creates arbs, some stay 1-sided, but it is programmed to hedge them.
when the price of an arb-locked position becomes 75/25 it basically buys more of the 75 (most-likely) side, and cuts some of the 25.
this has worked and basically got me to break even most times when i am using it, but occasionally things go wrong and it loses me 10% or so.
i am trying to find out what else i can add or what other data i can use to improve it and make it functional to the point of making 5-10% per day. 
5% compounded daily on 1,000$ for 90 days is about 80k, and that would be nice. 10% would be even better. I think i am close but am missing a few things.

as you can see in the code, there are about 10 versions of it, with 10 being the newest/most recent and working best. 
i have had several different strageties along the way, originally i was trying to copy a trader named RN1 who is on polymarket that
seemed to be making consitatnt profit daily, i was just not able to find exactly what he was using as signals to get into trades.

i had it create a read-me that lists all of its functions below. 
If you see this and have some suggestions send me a message or a comment. 


# PolyArb v10 — Sports Arbitrage Engine for Polymarket

A fully automated sports arbitrage trading bot for [Polymarket](https://polymarket.com)'s CLOB (Central Limit Order Book). Detects and locks guaranteed-profit binary arbitrage opportunities across live sports markets in real time.

**Core principle:** In a binary market (YES/NO), if you can buy both sides for a combined cost under $1.00, you lock in guaranteed profit at resolution — one side always pays $1.00. This bot finds those opportunities, executes both legs simultaneously, and manages positions through the life of the event.

~10,000 lines of Python across 6 core modules. Runs at 20 cycles/second with sub-50ms arb detection via WebSocket price streaming.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     MAIN LOOP (50ms cycles)                 │
│                                                             │
│  Phase 0: WS-Triggered Instant Arbs (<50ms detection)       │
│  Phase 1: Build Existing Arbs (book walking, completion)    │
│  Phase 2: Scan All Markets (new arb discovery)              │
│  Phase 3: Live Event Accumulation (dip buying)              │
│                                                             │
│  Position Steps (per cycle):                                │
│    W: Wallet-Arb Scanner    H: Winner Hammer                │
│    S: Skim (small buys)     T: Loss Trim                    │
│    R: Hedge Recovery                                        │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│              BACKGROUND THREADS                             │
│  - Market Discovery (every 15s)                             │
│  - New-Market Fast Scanner (every 5s)                       │
│  - WebSocket Price Stream (real-time)                       │
│  - Odds Engine (external sportsbook data)                   │
│  - Game Score Fetcher (live scores)                         │
└─────────────────────────────────────────────────────────────┘
```

---

## Core Features

### Instant Arb Detection & Execution

The bot runs two parallel detection methods. The WebSocket stream processes real-time price ticks and fires arb orders within 50ms of detection — no polling delay. The scan-based Phase 2 sweeps all tracked markets each cycle, catching opportunities the WebSocket might miss on less-subscribed tokens.

When an arb is found (combined YES + NO ask < configurable threshold, default $0.97), both legs execute simultaneously via Polymarket's batch order API (`post_orders`) using Fill-And-Kill (FAK) orders. This isn't sequential — both sides go in one API call. If simultaneous execution isn't available, the bot falls back to a sequential strategy (buy one side, then complete the other).

### Order Book Walking

Rather than buying at a single price level, the bot walks through the full order book to eat every profitable tier. If NO is available at $0.45 (100 shares), $0.46 (50 shares), $0.47 (30 shares), and YES at $0.50 (200 shares), it buys matched pairs at each level where the combined cost stays under threshold. This is how the bot maximizes position size on each arb.

### Fresh Price Validation

Before any execution, the bot re-fetches both order books in parallel to confirm the arb still exists. Prices can move in the 10-20ms between detection and order placement. If the fresh check shows the spread has closed, the order is skipped. The validation cache has a 500ms TTL to avoid redundant fetches within the same cycle.

### Dynamic Limit Pricing

Instead of fixed slippage, the bot analyzes book structure to calculate optimal limit prices. It walks the asks to find the price level needed to fill the target share count, then adds a small buffer (0.5-2 cents) to ensure fills while keeping the combined cost profitable. The limit is hard-capped so both sides never exceed $0.99 combined.

### Book Depth Checking

Before committing capital, the bot verifies sufficient liquidity exists at profitable prices. If a market only has 5 shares available below the arb threshold, it's not worth the execution risk. Configurable minimum depth (default 10 shares) prevents entering illiquid markets.

### Live Event Accumulation (RN1-Style)

This is the more sophisticated strategy. In live sports, both sides of a binary market experience dips as the game unfolds — a basketball lead change drops the leader's price and spikes the trailer's. The bot tracks momentum across all live markets and buys each side during its respective dip. When the combined average cost of both sides drops below $1.00, the position is a locked arb built over time rather than caught in a single instant.

### 19-Criteria Market Scoring

Every tracked live market gets scored each cycle on 19 dimensions: floor price proximity, 24h volume, market type, sportsbook odds edge, trailing momentum, lead switch count, price velocity, pullback depth, volatility profile, trend strength, bounce detection, consecutive highs/lows, bet volume acceleration, book depth, score streak, odds confluence, and confidence state. Only markets exceeding the entry threshold (configurable, default 200 points) trigger accumulation buys.

### Dual Trailing Stop Entry

Borrowed from high-frequency trailing stop strategies but applied as a timing signal. The bot waits for Side A to bounce up from its session low (momentum shift detected), then waits for Side B to stop falling (dip over). When both sides have bounced, the combined price is at its cheapest — that's when the buy fires. This catches the bottom of the combined price curve, not the middle of a one-way move.

### Sport-Specific Completion Timing

Each sport has different volatility patterns. Baseball innings cause swings every 15 minutes; soccer goals are rare but massive; basketball has constant small swings. The bot uses sport-specific timing windows for arb completion:

| Sport | Tight Window | Medium Window | Notes |
|-------|-------------|---------------|-------|
| Baseball | 10 min | 15 min | Patient — wait for other half-inning |
| Soccer | 3 min | 5 min | Fast — grab dips before bounce |
| Basketball | 4 min | 7 min | Medium — wait for runs |
| Hockey | 6 min | 10 min | Patient — low scoring like soccer |
| Football | 5 min | 8 min | Drive-based swings |
| Tennis | 4 min | 7 min | Break-of-serve opportunities |
| MMA | 3 min | 5 min | Fights are short |

### Position Shaping

Once a position is established, the bot actively shapes it:

- **Winner Hammer** — When a held side sustains above 75 cents for 4+ minutes (or 90+ cents for instant trigger), the bot adds $15 to ride the winner to resolution. One shot per position, momentum-confirmed, with immediate portfolio save to prevent double-fires on restarts.
- **Skim** — Smaller $2 buys on the winning side when it holds above 70 cents for 7+ minutes. Repeatable with cooldown, captures value before the full hammer fires.
- **Loss Trim** — When a held side collapses below 30 cents for 4+ minutes, sells 75% of shares to cut losses and free capital.
- **Hedge Recovery** — For hedged positions (both sides held, negative P&L), buys cheap matched pairs to dilute the average combined cost. If enough cheap pairs are added, the position transitions from hedged to locked (guaranteed profit).

### Wallet Position Scanner

On startup, the bot syncs on-chain balances and detects existing positions in the wallet. If both sides of a market are already held and the combined cost is under threshold, it recognizes the arb. It also skips wallet positions that were intentionally shaped (hammered/trimmed) to avoid undoing the strategy.

### Cross-Market Arbs (Triads)

For soccer matches with three outcomes (Win A, Win B, Draw), the bot detects when buying all three NO tokens costs less than $2.00 (guaranteed $2.00 payout since exactly one outcome wins). These triad arbs are tracked and completed leg-by-leg.

### Time-Escalating Hedge Urgency

When the bot has a one-sided position waiting for completion, urgency increases over time. At 3 minutes it relaxes the trailing gate, at 5 minutes it widens the dip threshold, at 7 minutes it skips the trailing gate entirely. This prevents capital getting stuck in single-sided positions while still giving the market time to present optimal entries.

### Stale Position Management

Positions that sit one-sided for 8+ minutes with no second-side opportunity get evaluated. If the held side is below 55 cents (losing), it exits to free the slot. If above 55 cents (winning), it holds for resolution. Dead positions (held side below 2 cents) are marked for cleanup.

---

## External Intelligence

### Sportsbook Odds Engine (`odds_engine.py`)

Fetches live odds from The Odds API across 9 sports, computes consensus probabilities from sharp books (DraftKings, FanDuel, BetMGM), removes vig, and compares against Polymarket prices. Detects when Polymarket is mispriced relative to the sportsbook consensus. Line movement tracking spots sharp money signals. Runs in a background thread, stays within the free API tier (500 requests/month).

### Elo Rating Model (`elo_ratings.py`)

Maintains dynamic Elo ratings for ~400 teams across NBA, NHL, MLB, and NFL. Seeded from season standings, updated on game resolutions. Used to validate sportsbook odds — if the Elo model agrees with the sportsbook but disagrees with Polymarket, that's a stronger signal.

### Team Name Mapper (`team_name_mapper.py`)

Zero-dependency fuzzy matching system that maps Polymarket titles ("Lightning vs Maple Leafs") to canonical team keys across 200+ teams. Multi-tier matching: exact lookup, substring, then SequenceMatcher fuzzy matching. Confidence-gated at 65% minimum to prevent false matches.

### Game Score Fetcher (`game_scores.py`)

Polls live game scores to feed the lead switch detection system. When the bot knows a lead change happened, it can anticipate the price swing and time entries accordingly.

---

## Dashboard (`watch_v10.py`)

Real-time terminal UI showing:

- Active positions with P&L (locked arbs, hedged, hammered, one-sided)
- Guaranteed profit calculations for locked positions
- Live market heat map (hot/warm/cold scoring with momentum indicators)
- Recent trades and fills
- Governor slot status and deployed capital
- Session statistics (markets traded, arbs locked, success rate)
- Error monitoring from log tail

Reads from `portfolio_v10.json` and `live_tracker.json` exported by the main bot.

---

## File Structure

```
Core:
  arb_v10.py            Main bot engine (7,400 lines)
  watch_v10.py           Live dashboard (977 lines)
  odds_engine.py         Sportsbook odds integration (628 lines)
  elo_ratings.py         Elo rating model (311 lines)
  team_name_mapper.py    Fuzzy team matching (492 lines)
  game_scores.py         Live score fetcher (391 lines)

Config:
  keys.env               API keys (PK, PROXY_WALLET, THE_ODDS_API_KEY)

Data (auto-generated):
  portfolio_v10.json     Persisted position state
  live_tracker.json      Dashboard feed (scores, momentum, prices)
  elo_data.json          Persisted Elo ratings
  logs/v10_*.log         Timestamped session logs
  game_data/             ML training data (snapshots, resolutions)
  audit.jsonl            Append-only trade audit log
```

---

## Usage

```bash
# Dry run (no real orders)
python3 arb_v10.py --bankroll 1000 --fresh

# Live trading
python3 arb_v10.py --bankroll 1000 --fresh --live

# Scan only (price check, no trades)
python3 arb_v10.py --bankroll 1000 --fresh --scan

# Dashboard (separate terminal)
python3 watch_v10.py
```

### Flags

| Flag | Description |
|------|-------------|
| `--bankroll N` | Starting capital in USD (default 1000) |
| `--fresh` | Reset portfolio and ledger on startup |
| `--live` | Enable real trading (default is dry run) |
| `--scan` | Price-check mode, no orders placed |
| `--no-dash` | Disable inline dashboard rendering |

---

## Configuration

All tunable parameters are constants at the top of `arb_v10.py`. Key ones:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ARB_LOCK_THRESHOLD` | 0.97 | Max combined cost to trigger arb (3% spread) |
| `ARB_SLIPPAGE` | 0.015 | Cents above ask for guaranteed fills |
| `ARB_MAX_PER_MARKET` | 2% | Max bankroll % per arb market |
| `ARB_MAX_PER_EVENT` | 5% | Max bankroll % per event |
| `LIVE_MIN_SCORE_ENTRY` | 200 | Minimum score for accumulation entry |
| `LIVE_MAX_PENDING` | 3 | Max single-sided positions |
| `HAMMER_THRESHOLD` | 0.75 | Min bid to qualify for hammer |
| `HAMMER_SIZE` | 15.00 | USD per hammer buy |
| `TRIM_THRESHOLD` | 0.30 | Max bid to trigger loss trim |
| `MAX_DAYS_OUT` | 30 | Only trade markets resolving within N days |

---

## Requirements

- Python 3.11+
- `py_clob_client` (Polymarket CLOB SDK)
- `requests`
- Polymarket account with API keys (private key + proxy wallet)
- Optional: The Odds API key for sportsbook intelligence

---

## Data Collection

The bot writes training data to `game_data/` each session: periodic market snapshots (prices, books, momentum state) and resolution outcomes. This pipeline is designed for future ML model training to improve scoring and entry timing.

---

## Risk Management

- Per-market and per-event capital caps prevent concentration
- Governor system enforces max 3 pending single-sided positions
- Fill-And-Kill orders prevent partial fills from creating unintended exposure
- Fresh validation before every execution prevents stale-price arbs
- Immediate portfolio persistence after position-shaping actions (hammer, trim) prevents state loss on crashes
- Main loop crash guard with automatic recovery prevents silent death
- Stale position exits free capital from stuck positions
- Sport-specific timing prevents premature completions

---

## Disclaimer

This is experimental trading software. Use at your own risk. Prediction markets involve real money and can result in losses. Past performance does not guarantee future results. Always start with small bankrolls and dry-run mode.
