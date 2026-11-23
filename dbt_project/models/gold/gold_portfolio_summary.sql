/*
  gold_portfolio_summary

  The main portfolio analytics model.
  For every account and every stock they hold, calculates:
  - Current market value (shares × current price)
  - Unrealized P&L (current value vs what they paid)
  - Unrealized P&L percentage
  - Total amount invested
  - 7-day moving average price

  This is the table a portfolio dashboard reads from.
*/

WITH

positions AS (
    SELECT
        account_id,
        ticker,
        shares_held,
        avg_buy_price
    FROM {{ ref('silver_positions') }}
),

latest_prices AS (
    SELECT
        ticker,
        close_price,
        fetched_at
    FROM {{ ref('silver_market_prices') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ticker
        ORDER BY fetched_at DESC
    ) = 1
),

accounts AS (
    SELECT
        account_id,
        owner_name,
        email
    FROM {{ ref('silver_accounts') }}
),

price_moving_avg AS (
    SELECT
        ticker,
        fetched_at,
        close_price,
        AVG(close_price) OVER (
            PARTITION BY ticker
            ORDER BY fetched_at
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS price_7day_avg
    FROM {{ ref('silver_market_prices') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ticker
        ORDER BY fetched_at DESC
    ) = 1
),

portfolio AS (
    SELECT
        a.account_id,
        a.owner_name,
        a.email,

        p.ticker,
        p.shares_held,
        p.avg_buy_price,

        lp.close_price                                     AS current_price,
        lp.fetched_at                                      AS price_as_of,
        pma.price_7day_avg,

        ROUND(p.shares_held * p.avg_buy_price, 2)          AS total_invested,

        ROUND(p.shares_held * lp.close_price, 2)           AS market_value,

        ROUND(
            (lp.close_price - p.avg_buy_price) * p.shares_held,
            2
        )                                                  AS unrealized_pnl,

        ROUND(
            (lp.close_price - p.avg_buy_price)
            / NULLIF(p.avg_buy_price, 0) * 100,
            2
        )                                                  AS pnl_pct,

        CASE
            WHEN lp.close_price > p.avg_buy_price THEN 'PROFIT'
            WHEN lp.close_price < p.avg_buy_price THEN 'LOSS'
            ELSE 'BREAKEVEN'
        END                                                AS position_status

    FROM positions p
    JOIN accounts a      ON a.account_id = p.account_id
    JOIN latest_prices lp ON lp.ticker   = p.ticker
    LEFT JOIN price_moving_avg pma ON pma.ticker = p.ticker
)

SELECT * FROM portfolio
ORDER BY unrealized_pnl DESC