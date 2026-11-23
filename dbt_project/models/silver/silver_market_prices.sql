/*
  silver_market_prices

  Cleans the raw bronze market prices data.
  Each row is one OHLCV snapshot for one ticker at one moment.
  Not deduplicated: an occasional repeated bar has little effect on
  daily aggregates and moving averages.
*/

WITH

casted AS (
    SELECT
        ticker,

        CAST(open_price  AS NUMERIC)  AS open_price,
        CAST(high_price  AS NUMERIC)  AS high_price,
        CAST(low_price   AS NUMERIC)  AS low_price,
        CAST(close_price AS NUMERIC)  AS close_price,

        CAST(volume      AS INT64)    AS volume,

        TIMESTAMP(fetched_at)         AS fetched_at,

        _ingested_at

    FROM {{ source('bronze', 'market_prices') }}

    WHERE ticker IS NOT NULL
      AND close_price IS NOT NULL
      AND fetched_at IS NOT NULL
)

SELECT * FROM casted