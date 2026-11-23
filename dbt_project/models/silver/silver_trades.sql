/*
  silver_trades
  
  Cleans the raw bronze trades data:
  1. Casts price from string to numeric
  2. Converts traded_at from Unix epoch microseconds to proper timestamp
  3. Removes Debezium snapshot rows (op = 'r'): initial reads of existing
     data, not new trading events
  4. Removes delete events (op = 'd')
  5. Deduplicates on trade_id: the consumer is at-least-once, so a replayed
     batch can deliver the same trade twice
*/

WITH

casted AS (
    SELECT
        CAST(trade_id   AS INT64)   AS trade_id,
        CAST(account_id AS INT64)   AS account_id,

        ticker,
        trade_type,

        CAST(quantity AS INT64)     AS quantity,

        CAST(price AS NUMERIC)      AS price,

        TIMESTAMP_MICROS(
            CAST(traded_at AS INT64)
        )                           AS traded_at,

        _ingested_at,

        _cdc_op

    FROM {{ source('bronze', 'trades') }}

    WHERE _cdc_op NOT IN ('r', 'd')
      AND trade_id IS NOT NULL
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trade_id
            ORDER BY _ingested_at ASC
        ) AS row_num
    FROM casted
)

SELECT
    trade_id,
    account_id,
    ticker,
    trade_type,
    quantity,
    price,
    traded_at,
    _ingested_at,
    _cdc_op
FROM deduped
WHERE row_num = 1