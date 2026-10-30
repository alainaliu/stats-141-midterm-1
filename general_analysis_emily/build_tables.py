"""Turn the cached raw data (pull_data.py) into small analysis tables in general_analysis_emily/data/.

Each table is skipped if it already exists; delete a file to rebuild it. Run from the project root:
    .venv/bin/python general_analysis_emily/build_tables.py
"""
import os, time
from pathlib import Path
import duckdb

CACHE = Path(os.environ.get("PM_CACHE", Path.home() / "polymarket_cache"))
OUT = Path(__file__).parent / "data"
OUT.mkdir(exist_ok=True)

con = duckdb.connect()
con.sql("SET TimeZone = 'UTC'")
con.sql(f"CREATE VIEW markets AS SELECT * FROM '{CACHE}/markets.parquet'")
con.sql(f"CREATE VIEW events AS SELECT * FROM '{CACHE}/events.parquet'")
con.sql(f"CREATE VIEW resolutions AS SELECT * FROM '{CACHE}/resolutions.parquet'")
con.sql(f"""CREATE VIEW trades AS
            SELECT * FROM read_parquet('{CACHE}/wallet_trades/trades-*.parquet')
            WHERE price > 0 AND price < 1 AND dollars > 0""")

# Category from the event slug (e) and title (t). Rules are checked top to bottom; first match wins.
con.sql(r"""
CREATE MACRO category(e, t) AS CASE
  WHEN regexp_matches(lower(e), '(^|-)(bitcoin|btc|ethereum|eth|solana|sol|xrp|doge|dogecoin|bnb|hype|zec|crypto)(-|$)')
    OR regexp_matches(lower(t), 'bitcoin|ethereum|solana|xrp|dogecoin|crypto|up or down') THEN 'crypto'
  WHEN regexp_matches(lower(e), '^(highest|lowest)-temperature|precipitation|hurricane|weather') THEN 'weather'
  WHEN regexp_matches(lower(e), '\d{4}-\d{2}-\d{2}') OR regexp_matches(lower(t), ' vs\.? | o/u |spread:|moneyline')
    OR regexp_matches(lower(t), 'world cup|fifa|nba|nfl|mlb|nhl|super bowl|champions league|premier league|la liga|serie a|bundesliga|stanley cup|grand prix|formula 1|ufc|mvp|heisman|ballon d|wimbledon|us open|french open|australian open|world series|playoffs|f1 |drivers. champion|constructors. champion') THEN 'sports'
  WHEN regexp_matches(lower(t), 'election|president|senate|governor|congress|parliament|prime minister|trump|democrat|republican|minister|mayor|ceasefire|war |iran|israel|ukraine|russia|china|tariff|supreme court|nominee|primary|hormuz|nato|venezuela|maduro|gaza|putin|zelensky|military|strike on|invade|pope|government shutdown|xi jinping|signed into law') THEN 'politics'
  WHEN regexp_matches(lower(t), 'fed |fed\b|interest rate|inflation|cpi|gdp|recession|unemployment|jobs report|s&p|nasdaq|stock|price of|ipo|market cap|earnings|oil') THEN 'economy'
  WHEN regexp_matches(lower(t), 'openai|gpt|gemini|anthropic|claude|google|apple|tesla|nvidia|spacex|microsoft|meta |ai model|ai |elon|musk|tweet') THEN 'tech'
  WHEN regexp_matches(lower(t), 'oscar|grammy|emmy|movie|film|box office|album|song|spotify|billboard|netflix|taylor swift|celebrity|tv |youtube|award') THEN 'entertainment'
  ELSE 'other'
END""")


def build(name, sql):
    out = OUT / f"{name}.parquet"
    if out.exists():
        print(f"{name}: cached")
        return
    t = time.time()
    con.sql(f"COPY ({sql}) TO '{out}' (FORMAT parquet)")
    n = con.sql(f"SELECT count(*) FROM '{out}'").fetchall()[0][0]
    print(f"{name}: {n:,} rows in {time.time() - t:.0f} s", flush=True)


# One row per market that traded: who, how much, when, plus its category and outcome if resolved.
build("market_stats", """
    WITH t AS (
        SELECT market, count(*) AS trades, sum(dollars) AS dollars, count(DISTINCT wallet) AS wallets,
               min(time) AS first_trade, max(time) AS last_trade
        FROM trades GROUP BY market)
    SELECT t.*, m.title, m.event_slug, m.start_date, m.end_date, m.neg_risk,
           category(m.event_slug, m.title) AS category,
           r.resolved_yes, r.resolved_at
    FROM t JOIN markets m ON m.slug = t.market
    LEFT JOIN resolutions r ON r.market_slug = t.market""")

# One row per wallet.
build("wallet_stats", """
    SELECT wallet, count(*) AS trades, sum(dollars) AS dollars, count(DISTINCT market) AS markets,
           min(time) AS first_trade, max(time) AS last_trade,
           sum(dollars) FILTER (WHERE side = 'BUY') AS bought
    FROM trades GROUP BY wallet""")

# Calibration: the market's Yes price at fixed horizons before it resolved.
# Price = dollar-weighted average Yes price of the trades in the window just before the horizon
# (6 h window for 1 and 7 days, 1 h window for the 1 h horizon), and the last trade in that window.
# A No-side trade at p counts as 1 - p.
build("calibration", """
    WITH res AS (
        SELECT r.market_slug AS market, r.resolved_yes, r.resolved_at,
               json_extract_string(m.outcomes, '$[0]') AS yes_label
        FROM resolutions r JOIN markets m ON m.slug = r.market_slug),
    h AS (SELECT * FROM (VALUES ('1h', INTERVAL 1 HOUR, INTERVAL 1 HOUR),
                                ('1d', INTERVAL 1 DAY, INTERVAL 6 HOUR),
                                ('7d', INTERVAL 7 DAY, INTERVAL 6 HOUR)) v(horizon, lead, win)),
    w AS (SELECT res.*, h.horizon, res.resolved_at - h.lead AS t_end, res.resolved_at - h.lead - h.win AS t_start
          FROM res CROSS JOIN h)
    SELECT w.market, w.horizon, w.resolved_yes, w.resolved_at,
           sum(t.dollars * CASE WHEN t.outcome = w.yes_label THEN t.price ELSE 1 - t.price END) / sum(t.dollars) AS price_yes,
           arg_max(CASE WHEN t.outcome = w.yes_label THEN t.price ELSE 1 - t.price END, t.time) AS price_last,
           count(*) AS trades, sum(t.dollars) AS dollars
    FROM w JOIN trades t ON t.market = w.market AND t.time > w.t_start AND t.time <= w.t_end
    GROUP BY ALL""")

# One-shot wallets: every trade they ever made is in one market, buys only.
# Their bet = the outcome they spent most on; did it win, and what did it cost?
build("one_shot", """
    WITH ws AS (SELECT wallet FROM trades GROUP BY wallet
                HAVING count(DISTINCT market) = 1 AND bool_and(side = 'BUY')),
    pos AS (SELECT t.wallet, t.market, t.outcome, sum(t.dollars) AS spent, sum(t.size) AS shares, count(*) AS trades,
                   min(t.time) AS first_trade
            FROM trades t JOIN ws USING (wallet) GROUP BY ALL),
    main AS (SELECT * FROM pos QUALIFY row_number() OVER (PARTITION BY wallet ORDER BY spent DESC) = 1)
    SELECT main.*, main.spent / main.shares AS avg_price,
           (main.outcome = json_extract_string(m.outcomes, '$[0]')) = r.resolved_yes AS won,
           r.resolved_at
    FROM main JOIN markets m ON m.slug = main.market
    JOIN resolutions r ON r.market_slug = main.market""")

# Case study: daily dollar-weighted Yes price of every team in the 2026 World Cup winner event.
build("world_cup_daily", """
    SELECT m.slug AS market, regexp_extract(m.title, 'Will (.*) win the 2026', 1) AS team,
           date_trunc('day', t.time) AS day,
           sum(t.dollars * CASE WHEN t.outcome = json_extract_string(m.outcomes, '$[0]') THEN t.price ELSE 1 - t.price END)
               / sum(t.dollars) AS price_yes,
           sum(t.dollars) AS dollars
    FROM trades t JOIN markets m ON m.slug = t.market
    WHERE m.event_slug = 'world-cup-winner'
    GROUP BY ALL""")

# Wins per wallet: every BUY made before its market resolved. A win = the outcome bought won.
# expected_wins = sum of prices paid, the wins a wallet needs just to break even.
build("wallet_wins", """
    SELECT t.wallet, count(*) AS trades,
           count(*) FILTER (WHERE (t.outcome = json_extract_string(m.outcomes, '$[0]')) = r.resolved_yes) AS wins,
           sum(t.price) AS expected_wins,
           sum(t.dollars) AS staked,
           sum(CASE WHEN (t.outcome = json_extract_string(m.outcomes, '$[0]')) = r.resolved_yes
                    THEN t.size - t.dollars ELSE -t.dollars END) AS pnl,
           count(DISTINCT t.market) AS markets
    FROM trades t JOIN markets m ON m.slug = t.market
    JOIN resolutions r ON r.market_slug = t.market
    WHERE t.side = 'BUY' AND t.time < r.resolved_at
    GROUP BY t.wallet""")

# ---- Three-way comparison: all time · all markets in the World Cup window · World Cup markets in the window ----
# 2026 World Cup events: match events (fifwc-) plus tournament-wide events whose title or slug says "world cup",
# minus the other world cups sharing the name.
build("wc_markets", r"""
    SELECT m.slug AS market, CASE WHEN e.slug LIKE 'fifwc-%' THEN 'match' ELSE 'tournament-wide' END AS kind
    FROM events e JOIN markets m ON m.event_slug = e.slug
    WHERE e.slug LIKE 'fifwc-%' OR ((lower(e.title) LIKE '%world cup%' OR lower(e.slug) LIKE '%world-cup%')
      AND NOT regexp_matches(lower(e.title), 'esports|t20|cricket|overwatch|honor of kings|rocket league|women|2030|2027|club world cup|rugby|dota|valorant|counter-strike|call of duty|rainbow six|league of legends|lol:|farewell'))""")

WINDOW = "t.time >= TIMESTAMPTZ '2026-05-01 00:00:00+00' AND t.time < TIMESTAMPTZ '2026-08-02 00:00:00+00'"
con.sql(f"""CREATE VIEW scoped_trades AS
    SELECT t.*, 'all' AS scope FROM trades t
    UNION ALL SELECT t.*, 'window' FROM trades t WHERE {WINDOW}
    UNION ALL SELECT t.*, 'wc_window' FROM trades t WHERE {WINDOW}
        AND t.market IN (SELECT market FROM '{OUT}/wc_markets.parquet')""")

# Per scope: dollars per market and per wallet (concentration, wallet sizes).
build("scope_markets", """
    SELECT scope, market, count(*) AS trades, sum(dollars) AS dollars FROM scoped_trades GROUP BY ALL""")
build("scope_wallets", """
    SELECT scope, wallet, count(*) AS trades, sum(dollars) AS dollars, count(DISTINCT market) AS markets
    FROM scoped_trades GROUP BY ALL""")

# Per scope: wallet_wins, using only the buys inside the scope.
build("scope_wallet_wins", """
    SELECT t.scope, t.wallet, count(*) AS trades,
           count(*) FILTER (WHERE (t.outcome = json_extract_string(m.outcomes, '$[0]')) = r.resolved_yes) AS wins,
           sum(t.price) AS expected_wins, sum(t.dollars) AS staked,
           sum(CASE WHEN (t.outcome = json_extract_string(m.outcomes, '$[0]')) = r.resolved_yes
                    THEN t.size - t.dollars ELSE -t.dollars END) AS pnl
    FROM scoped_trades t JOIN markets m ON m.slug = t.market
    JOIN resolutions r ON r.market_slug = t.market
    WHERE t.side = 'BUY' AND t.time < r.resolved_at
    GROUP BY ALL""")

print("done")
