/*
  silver_positions

  Cleans the raw bronze positions data.
  Positions represent current holdings per account per ticker.
  We keep only the most recent position state per account+ticker
  using ROW_NUMBER to deduplicate and get latest.
*/

WITH

casted AS (
    SELECT
        CAST(position_id  AS INT64)   AS position_id,
        CAST(account_id   AS INT64)   AS account_id,
        ticker,

        -- shares_held and avg_buy_price come as strings from Debezium
        CAST(shares_held    AS NUMERIC) AS shares_held,
        CAST(avg_buy_price  AS NUMERIC) AS avg_buy_price,

        -- last_updated is also epoch microseconds like traded_at
        TIMESTAMP_MICROS(
            CAST(last_updated AS INT64)
        )                             AS last_updated,

        _ingested_at,
        _cdc_op

    FROM {{ source('bronze', 'positions') }}

    -- Remove deletes and nulls
    WHERE _cdc_op != 'd'
      AND position_id IS NOT NULL
      AND shares_held IS NOT NULL
),

-- For positions we want the LATEST state per account+ticker
-- not the first occurrence. A position gets updated many times
-- as trades happen. We want the current value.
latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, ticker   -- group by account AND ticker
            ORDER BY last_updated DESC        -- keep the most recent
        ) AS row_num
    FROM casted
)

SELECT
    position_id,
    account_id,
    ticker,
    shares_held,
    avg_buy_price,
    last_updated,
    _ingested_at
FROM latest
WHERE row_num = 1
  AND shares_held > 0    -- exclude positions where all shares were sold