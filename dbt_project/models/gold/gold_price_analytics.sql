/*
  gold_price_analytics

  Time-series price analysis per ticker.
  Answers questions like:
  - What is the daily price range for each stock?
  - How volatile is each stock?
  - What are the moving averages?
  - How does today's price compare to the average?
*/

WITH

prices AS (
    SELECT
        ticker,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        fetched_at,
        DATE(fetched_at)                                   AS price_date
    FROM {{ ref('silver_market_prices') }}
),

daily_prices AS (
    SELECT
        ticker,
        price_date,

        MIN(open_price)                                    AS day_open,
        MAX(high_price)                                    AS day_high,
        MIN(low_price)                                     AS day_low,
        MAX(close_price)                                   AS day_close,
        SUM(volume)                                        AS day_volume,
        COUNT(*)                                           AS snapshot_count

    FROM prices
    GROUP BY ticker, price_date
),

price_analytics AS (
    SELECT
        ticker,
        price_date,
        day_open,
        day_high,
        day_low,
        day_close,
        day_volume,
        snapshot_count,

        ROUND(day_high - day_low, 4)                      AS daily_range,

        ROUND(
            (day_close - day_open) / NULLIF(day_open, 0) * 100,
            2
        )                                                  AS daily_return_pct,

        ROUND(
            AVG(day_close) OVER (
                PARTITION BY ticker
                ORDER BY price_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            4
        )                                                  AS close_7day_avg,

        ROUND(
            AVG(day_volume) OVER (
                PARTITION BY ticker
                ORDER BY price_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            0
        )                                                  AS volume_7day_avg,

        CASE
            WHEN day_close > AVG(day_close) OVER (
                PARTITION BY ticker
                ORDER BY price_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) THEN 'ABOVE_AVG'
            ELSE 'BELOW_AVG'
        END                                                AS vs_7day_avg,

        RANK() OVER (
            PARTITION BY price_date
            ORDER BY day_volume DESC
        )                                                  AS volume_rank_today,

        LAG(day_close, 1) OVER (
            PARTITION BY ticker
            ORDER BY price_date
        )                                                  AS prev_day_close,

        ROUND(
            day_close - LAG(day_close, 1) OVER (
                PARTITION BY ticker
                ORDER BY price_date
            ),
            4
        )                                                  AS day_over_day_change

    FROM daily_prices
)

SELECT * FROM price_analytics
ORDER BY ticker, price_date DESC