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
        -- Extract the date part for daily aggregations
        DATE(fetched_at)                                   AS price_date
    FROM {{ ref('silver_market_prices') }}
),

-- Daily OHLCV aggregation
-- Compress all the 1-minute bars into daily summaries
daily_prices AS (
    SELECT
        ticker,
        price_date,

        -- OHLCV for the day
        -- First open of the day
        MIN(open_price)                                    AS day_open,
        -- Highest high of the day
        MAX(high_price)                                    AS day_high,
        -- Lowest low of the day
        MIN(low_price)                                     AS day_low,
        -- Last close of the day
        MAX(close_price)                                   AS day_close,
        -- Total volume for the day
        SUM(volume)                                        AS day_volume,
        -- How many price snapshots we received
        COUNT(*)                                           AS snapshot_count

    FROM prices
    GROUP BY ticker, price_date
),

-- Add analytics on top of daily prices
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

        -- Daily price range — how much did the stock move today?
        ROUND(day_high - day_low, 4)                      AS daily_range,

        -- Daily return — did the stock go up or down today?
        ROUND(
            (day_close - day_open) / NULLIF(day_open, 0) * 100,
            2
        )                                                  AS daily_return_pct,

        -- 7-day moving average of closing price
        ROUND(
            AVG(day_close) OVER (
                PARTITION BY ticker
                ORDER BY price_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            4
        )                                                  AS close_7day_avg,

        -- 7-day moving average of volume
        ROUND(
            AVG(day_volume) OVER (
                PARTITION BY ticker
                ORDER BY price_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),
            0
        )                                                  AS volume_7day_avg,

        -- Is today's price above or below the 7-day average?
        -- This is a simple momentum signal
        CASE
            WHEN day_close > AVG(day_close) OVER (
                PARTITION BY ticker
                ORDER BY price_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) THEN 'ABOVE_AVG'
            ELSE 'BELOW_AVG'
        END                                                AS vs_7day_avg,

        -- Rank this ticker by volume today
        -- Most actively traded stock gets rank 1
        RANK() OVER (
            PARTITION BY price_date
            ORDER BY day_volume DESC
        )                                                  AS volume_rank_today,

        -- Previous day's close for day-over-day comparison
        LAG(day_close, 1) OVER (
            PARTITION BY ticker
            ORDER BY price_date
        )                                                  AS prev_day_close,

        -- Day-over-day change
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