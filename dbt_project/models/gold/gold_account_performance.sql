/*
  gold_account_performance

  Top-level account performance summary.
  One row per account showing their overall investment performance.
  This is what an executive dashboard or account summary page shows.
*/

WITH

-- Get portfolio summary already calculated
portfolio AS (
    SELECT
        account_id,
        owner_name,
        email,
        ticker,
        shares_held,
        avg_buy_price,
        current_price,
        total_invested,
        market_value,
        unrealized_pnl,
        pnl_pct,
        position_status
    FROM {{ ref('gold_portfolio_summary') }}
),

-- Get trading activity already calculated
activity AS (
    SELECT
        account_id,
        SUM(total_trades)   AS lifetime_trades,
        SUM(total_spent)    AS lifetime_spent,
        SUM(total_received) AS lifetime_received,
        MIN(first_trade_at) AS first_ever_trade,
        MAX(last_trade_at)  AS last_ever_trade
    FROM {{ ref('gold_trading_activity') }}
    GROUP BY account_id
),

-- Aggregate portfolio to account level
account_portfolio AS (
    SELECT
        account_id,
        owner_name,
        email,

        -- How many different stocks does this account hold?
        COUNT(DISTINCT ticker)                             AS stocks_held,

        -- Total money currently invested across all positions
        SUM(total_invested)                                AS total_invested,

        -- Total current market value across all positions
        SUM(market_value)                                  AS total_market_value,

        -- Total unrealized P&L across all positions
        SUM(unrealized_pnl)                                AS total_unrealized_pnl,

        -- Overall portfolio return percentage
        ROUND(
            SUM(unrealized_pnl)
            / NULLIF(SUM(total_invested), 0) * 100,
            2
        )                                                  AS portfolio_return_pct,

        -- Best performing position
        MAX(pnl_pct)                                       AS best_position_pnl_pct,

        -- Worst performing position
        MIN(pnl_pct)                                       AS worst_position_pnl_pct,

        -- How many positions are currently profitable?
        COUNTIF(position_status = 'PROFIT')                AS profitable_positions,
        COUNTIF(position_status = 'LOSS')                  AS losing_positions

    FROM portfolio
    GROUP BY account_id, owner_name, email
),

-- Join everything together
final AS (
    SELECT
        ap.account_id,
        ap.owner_name,
        ap.email,
        ap.stocks_held,
        ap.total_invested,
        ap.total_market_value,
        ap.total_unrealized_pnl,
        ap.portfolio_return_pct,
        ap.best_position_pnl_pct,
        ap.worst_position_pnl_pct,
        ap.profitable_positions,
        ap.losing_positions,

        -- Trading activity
        a.lifetime_trades,
        a.lifetime_spent,
        a.lifetime_received,
        a.first_ever_trade,
        a.last_ever_trade,

        -- Overall performance label
        CASE
            WHEN ap.portfolio_return_pct >= 5  THEN 'STRONG GAIN'
            WHEN ap.portfolio_return_pct >= 0  THEN 'SLIGHT GAIN'
            WHEN ap.portfolio_return_pct >= -5 THEN 'SLIGHT LOSS'
            ELSE 'SIGNIFICANT LOSS'
        END                                               AS performance_label,

        -- Rank accounts by portfolio return
        RANK() OVER (
            ORDER BY ap.portfolio_return_pct DESC
        )                                                 AS performance_rank

    FROM account_portfolio ap
    LEFT JOIN activity a ON a.account_id = ap.account_id
)

SELECT * FROM final
ORDER BY performance_rank