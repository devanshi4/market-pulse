/*
  silver_positions

  Cleans the raw bronze positions data.
  Positions represent current holdings per account per ticker.
  A position is updated on every trade, so only the latest state per
  account and ticker is kept.
*/

WITH

casted AS (
    SELECT
        CAST(position_id  AS INT64)   AS position_id,
        CAST(account_id   AS INT64)   AS account_id,
        ticker,

        CAST(shares_held    AS NUMERIC) AS shares_held,
        CAST(avg_buy_price  AS NUMERIC) AS avg_buy_price,

        TIMESTAMP_MICROS(
            CAST(last_updated AS INT64)
        )                             AS last_updated,

        _ingested_at,
        _cdc_op

    FROM {{ source('bronze', 'positions') }}

    WHERE _cdc_op != 'd'
      AND position_id IS NOT NULL
      AND shares_held IS NOT NULL
),

latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, ticker
            ORDER BY last_updated DESC
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
  AND shares_held > 0