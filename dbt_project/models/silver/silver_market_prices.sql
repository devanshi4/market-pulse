/*
  silver_market_prices

  Cleans the raw bronze market prices data.
  Each row is one OHLCV snapshot for one ticker at one moment.
  No deduplication needed here — duplicates are acceptable 
  for time-series price data since we use them for averages.
*/

WITH

casted AS (
    SELECT
        ticker,

        -- All price fields come as strings from our Yahoo fetcher
        CAST(open_price  AS NUMERIC)  AS open_price,
        CAST(high_price  AS NUMERIC)  AS high_price,
        CAST(low_price   AS NUMERIC)  AS low_price,
        CAST(close_price AS NUMERIC)  AS close_price,

        -- Volume is a whole number
        CAST(volume      AS INT64)    AS volume,

        -- fetched_at comes as an ISO string from our fetcher
        -- e.g. "2026-05-21T22:17:30+00:00"
        -- TIMESTAMP() converts an ISO string to BigQuery TIMESTAMP
        TIMESTAMP(fetched_at)         AS fetched_at,

        _ingested_at

    FROM {{ source('bronze', 'market_prices') }}

    WHERE ticker IS NOT NULL
      AND close_price IS NOT NULL
      AND fetched_at IS NOT NULL
)

SELECT * FROM casted