/*
  gold_account_performance

  Top-level account performance summary.
  One row per account showing their overall investment performance.
*/

WITH

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

account_portfolio AS (
    SELECT
        account_id,
        owner_name,
        email,

        COUNT(DISTINCT ticker)                             AS stocks_held,

        SUM(total_invested)                                AS total_invested,

        SUM(market_value)                                  AS total_market_value,

        SUM(unrealized_pnl)                                AS total_unrealized_pnl,

        ROUND(
            SUM(unrealized_pnl)
            / NULLIF(SUM(total_invested), 0) * 100,
            2
        )                                                  AS portfolio_return_pct,

        MAX(pnl_pct)                                       AS best_position_pnl_pct,

        MIN(pnl_pct)                                       AS worst_position_pnl_pct,

        COUNTIF(position_status = 'PROFIT')                AS profitable_positions,
        COUNTIF(position_status = 'LOSS')                  AS losing_positions

    FROM portfolio
    GROUP BY account_id, owner_name, email
),

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

        a.lifetime_trades,
        a.lifetime_spent,
        a.lifetime_received,
        a.first_ever_trade,
        a.last_ever_trade,

        CASE
            WHEN ap.portfolio_return_pct >= 5  THEN 'STRONG GAIN'
            WHEN ap.portfolio_return_pct >= 0  THEN 'SLIGHT GAIN'
            WHEN ap.portfolio_return_pct >= -5 THEN 'SLIGHT LOSS'
            ELSE 'SIGNIFICANT LOSS'
        END                                               AS performance_label,

        RANK() OVER (
            ORDER BY ap.portfolio_return_pct DESC
        )                                                 AS performance_rank

    FROM account_portfolio ap
    LEFT JOIN activity a ON a.account_id = ap.account_id
)

SELECT * FROM final
ORDER BY performance_rank