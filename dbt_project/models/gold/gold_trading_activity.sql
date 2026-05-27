/*
  gold_trading_activity

  Analyzes trading patterns and volume.
  Answers questions like:
  - Which tickers are traded most?
  - What is each account's total trading volume?
  - How does trading activity look over time?
  - What is the running total spend per account?
*/

WITH

trades AS (
    SELECT
        trade_id,
        account_id,
        ticker,
        trade_type,
        quantity,
        price,
        traded_at,
        -- Trade value = how much money changed hands
        ROUND(quantity * price, 2) AS trade_value
    FROM {{ ref('silver_trades') }}
),

accounts AS (
    SELECT account_id, owner_name
    FROM {{ ref('silver_accounts') }}
),

-- Aggregate trading statistics per account per ticker
account_ticker_stats AS (
    SELECT
        t.account_id,
        a.owner_name,
        t.ticker,

        -- How many times did this account trade this stock?
        COUNT(*)                                            AS total_trades,

        -- How many BUYs and SELLs separately?
        COUNTIF(trade_type = 'BUY')                        AS total_buys,
        COUNTIF(trade_type = 'SELL')                       AS total_sells,

        -- Total shares bought and sold
        SUM(CASE WHEN trade_type = 'BUY'  THEN quantity ELSE 0 END)
                                                           AS total_shares_bought,
        SUM(CASE WHEN trade_type = 'SELL' THEN quantity ELSE 0 END)
                                                           AS total_shares_sold,

        -- Average price paid on buys
        ROUND(
            AVG(CASE WHEN trade_type = 'BUY' THEN price END),
            4
        )                                                  AS avg_buy_price,

        -- Average price received on sells
        ROUND(
            AVG(CASE WHEN trade_type = 'SELL' THEN price END),
            4
        )                                                  AS avg_sell_price,

        -- Total money spent buying
        ROUND(
            SUM(CASE WHEN trade_type = 'BUY' THEN trade_value ELSE 0 END),
            2
        )                                                  AS total_spent,

        -- Total money received selling
        ROUND(
            SUM(CASE WHEN trade_type = 'SELL' THEN trade_value ELSE 0 END),
            2
        )                                                  AS total_received,

        -- First and last trade for this account+ticker combination
        MIN(traded_at)                                     AS first_trade_at,
        MAX(traded_at)                                     AS last_trade_at

    FROM trades t
    JOIN accounts a ON a.account_id = t.account_id
    GROUP BY t.account_id, a.owner_name, t.ticker
),

-- Add running totals using window functions
-- This shows cumulative spend over time per account
with_running_totals AS (
    SELECT
        *,
        -- Rank each account by total trades across all tickers
        -- RANK() assigns the same rank to ties
        RANK() OVER (
            ORDER BY total_trades DESC
        )                                                  AS activity_rank,

        -- What percentage of this account's total trades
        -- does this ticker represent?
        ROUND(
            total_trades * 100.0
            / SUM(total_trades) OVER (PARTITION BY account_id),
            1
        )                                                  AS pct_of_account_trades

    FROM account_ticker_stats
)

SELECT * FROM with_running_totals
ORDER BY account_id, total_trades DESC