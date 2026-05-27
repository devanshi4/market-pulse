/*
  silver_accounts

  Cleans the raw bronze accounts data.
  Accounts table is small (10 rows) and mostly static.
*/

WITH

casted AS (
    SELECT
        CAST(account_id AS INT64)     AS account_id,
        owner_name,
        email,

        -- balance comes as string from Debezium NUMERIC handling
        CAST(balance AS NUMERIC)      AS balance,

        -- created_at is epoch microseconds
        TIMESTAMP_MICROS(
            CAST(created_at AS INT64)
        )                             AS created_at,

        _ingested_at,
        _cdc_op

    FROM {{ source('bronze', 'accounts') }}

    WHERE _cdc_op != 'd'
      AND account_id IS NOT NULL
),

-- Keep latest version of each account
-- (in case account details like balance were updated)
latest AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY _ingested_at DESC
        ) AS row_num
    FROM casted
)

SELECT
    account_id,
    owner_name,
    email,
    balance,
    created_at
FROM latest
WHERE row_num = 1