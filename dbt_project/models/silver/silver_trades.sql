/*
  silver_trades
  
  Cleans the raw bronze trades data:
  1. Casts price from string to numeric
  2. Converts traded_at from Unix epoch microseconds to proper timestamp
  3. Removes Debezium snapshot rows (op = 'r') - these are initial 
     reads of existing data, not real trading events
  4. Removes delete events (op = 'd') - we don't want deleted trades
     in our analytics layer
  5. Deduplicates using ROW_NUMBER in case Kafka delivered any message twice
*/

WITH

-- Step 1: Read raw bronze data and cast every column to its correct type
casted AS (
    SELECT
        -- IDs stay as integers — no casting needed
        CAST(trade_id   AS INT64)   AS trade_id,
        CAST(account_id AS INT64)   AS account_id,

        -- Text fields stay as strings
        ticker,
        trade_type,

        -- Quantity is a whole number
        CAST(quantity AS INT64)     AS quantity,

        -- Price comes in as a string like "182.4500"
        -- We cast to NUMERIC which is BigQuery's exact decimal type
        CAST(price AS NUMERIC)      AS price,

        -- traded_at comes from Debezium as Unix epoch MICROSECONDS
        -- TIMESTAMP_MICROS() converts that integer to a real timestamp
        -- Example: 1716393922000000 → 2024-05-22 14:25:22 UTC
        TIMESTAMP_MICROS(
            CAST(traded_at AS INT64)
        )                           AS traded_at,

        -- Keep the ingestion timestamp so we know when it hit our pipeline
        _ingested_at,

        -- Keep the CDC operation for reference in silver
        _cdc_op

    FROM {{ source('bronze', 'trades') }}

    -- Remove Debezium snapshot rows and delete events
    -- 'r' = initial snapshot read (not a real trade event)
    -- 'd' = deleted row (we don't want deletes in analytics)
    WHERE _cdc_op NOT IN ('r', 'd')
      AND trade_id IS NOT NULL
),

-- Step 2: Deduplicate
-- If Kafka delivered the same message twice (which can happen with
-- at-least-once delivery), we'd have duplicate rows with the same trade_id.
-- ROW_NUMBER() assigns 1 to the first occurrence and 2, 3... to duplicates.
-- We then keep only the row with row_num = 1.
deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY trade_id      -- group rows with same trade_id
            ORDER BY _ingested_at ASC  -- keep the earliest one
        ) AS row_num
    FROM casted
)

-- Step 3: Final output — only keep unique rows
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