"""Reset the bronze Iceberg tables on GCS: drop them, then create them with explicit schemas.

This is destructive by design. Run it once when setting up, or to start over.
"""
import os
import logging
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    LongType,
    TimestamptzType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def get_catalog() -> SqlCatalog:
    bucket   = os.getenv("GCS_BUCKET")
    project  = os.getenv("GCP_PROJECT_ID")
    creds    = os.path.abspath(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    cat_db   = os.getenv("ICEBERG_CATALOG_DB", "market_pulse_catalog.db")
    cat_name = os.getenv("ICEBERG_CATALOG_NAME", "market_pulse")

    return SqlCatalog(
        cat_name,
        **{
            "uri":                         f"sqlite:///{cat_db}",
            "warehouse":                   f"gs://{bucket}/bronze",
            "gcs.project-id":              project,
            "gcs.oauth2.credentials-file": creds,
        },
    )


# Explicit schemas instead of inferring them from the first message, so a malformed
# event can't change a table's columns. Prices are strings to keep exact decimals.
#
# A partition spec refers to its source column by field ID, and _ingested_at has a
# different ID in each schema (8, 7, 6, 8). That's why each table gets its own spec below.

TRADES_SCHEMA = Schema(
    NestedField(1,  "trade_id",         LongType(),        required=False),
    NestedField(2,  "account_id",       LongType(),        required=False),
    NestedField(3,  "ticker",           StringType(),      required=False),
    NestedField(4,  "trade_type",       StringType(),      required=False),
    NestedField(5,  "quantity",         LongType(),        required=False),
    NestedField(6,  "price",            StringType(),      required=False),
    NestedField(7,  "traded_at",        LongType(),        required=False),
    NestedField(8,  "_ingested_at",     TimestamptzType(), required=False),
    NestedField(9,  "_kafka_topic",     StringType(),      required=False),
    NestedField(10, "_kafka_partition", LongType(),        required=False),
    NestedField(11, "_kafka_offset",    LongType(),        required=False),
    NestedField(12, "_cdc_op",          StringType(),      required=False),
)

POSITIONS_SCHEMA = Schema(
    NestedField(1,  "position_id",      LongType(),        required=False),
    NestedField(2,  "account_id",       LongType(),        required=False),
    NestedField(3,  "ticker",           StringType(),      required=False),
    NestedField(4,  "shares_held",      StringType(),      required=False),
    NestedField(5,  "avg_buy_price",    StringType(),      required=False),
    NestedField(6,  "last_updated",     LongType(),        required=False),
    NestedField(7,  "_ingested_at",     TimestamptzType(), required=False),
    NestedField(8,  "_kafka_topic",     StringType(),      required=False),
    NestedField(9,  "_kafka_partition", LongType(),        required=False),
    NestedField(10, "_kafka_offset",    LongType(),        required=False),
    NestedField(11, "_cdc_op",          StringType(),      required=False),
)

ACCOUNTS_SCHEMA = Schema(
    NestedField(1,  "account_id",       LongType(),        required=False),
    NestedField(2,  "owner_name",       StringType(),      required=False),
    NestedField(3,  "email",            StringType(),      required=False),
    NestedField(4,  "balance",          StringType(),      required=False),
    NestedField(5,  "created_at",       LongType(),        required=False),
    NestedField(6,  "_ingested_at",     TimestamptzType(), required=False),
    NestedField(7,  "_kafka_topic",     StringType(),      required=False),
    NestedField(8,  "_kafka_partition", LongType(),        required=False),
    NestedField(9,  "_kafka_offset",    LongType(),        required=False),
    NestedField(10, "_cdc_op",          StringType(),      required=False),
)

MARKET_PRICES_SCHEMA = Schema(
    NestedField(1,  "ticker",           StringType(),      required=False),
    NestedField(2,  "open_price",       StringType(),      required=False),
    NestedField(3,  "high_price",       StringType(),      required=False),
    NestedField(4,  "low_price",        StringType(),      required=False),
    NestedField(5,  "close_price",      StringType(),      required=False),
    NestedField(6,  "volume",           LongType(),        required=False),
    NestedField(7,  "fetched_at",       StringType(),      required=False),
    NestedField(8,  "_ingested_at",     TimestamptzType(), required=False),
    NestedField(9,  "_kafka_topic",     StringType(),      required=False),
    NestedField(10, "_kafka_partition", LongType(),        required=False),
    NestedField(11, "_kafka_offset",    LongType(),        required=False),
)


# Partition by ingestion day so queries over recent data skip old files.
# Prices are also partitioned by ticker, the most common filter.

TRADES_PARTITION = PartitionSpec(
    PartitionField(
        source_id=8,        # _ingested_at is field 8 in TRADES_SCHEMA
        field_id=1000,
        transform=DayTransform(),
        name="ingested_day",
    )
)

POSITIONS_PARTITION = PartitionSpec(
    PartitionField(
        source_id=7,        # _ingested_at is field 7 in POSITIONS_SCHEMA
        field_id=1000,
        transform=DayTransform(),
        name="ingested_day",
    )
)

PRICES_PARTITION = PartitionSpec(
    PartitionField(
        source_id=8,        # _ingested_at is field 8 in MARKET_PRICES_SCHEMA
        field_id=1000,
        transform=DayTransform(),
        name="ingested_day",
    ),
    PartitionField(
        source_id=1,        # ticker is field 1 in MARKET_PRICES_SCHEMA
        field_id=1001,
        transform=IdentityTransform(),
        name="ticker",
    )
)

TABLES = [
    {
        "name":      "bronze.trades",
        "schema":    TRADES_SCHEMA,
        "partition": TRADES_PARTITION,
    },
    {
        "name":      "bronze.positions",
        "schema":    POSITIONS_SCHEMA,
        "partition": POSITIONS_PARTITION,
    },
    {
        "name":      "bronze.accounts",
        "schema":    ACCOUNTS_SCHEMA,
        "partition": PartitionSpec(),        # 10 rows: partitioning would only add overhead
    },
    {
        "name":      "bronze.market_prices",
        "schema":    MARKET_PRICES_SCHEMA,
        "partition": PRICES_PARTITION,
    },
]


def drop_tables(catalog: SqlCatalog):
    for table_def in TABLES:
        name = table_def["name"]
        if catalog.table_exists(name):
            catalog.drop_table(name)
            log.info("Dropped: %s", name)


def create_tables(catalog: SqlCatalog):
    existing_namespaces = [ns[0] for ns in catalog.list_namespaces()]
    if "bronze" not in existing_namespaces:
        catalog.create_namespace("bronze")
        log.info("Created namespace: bronze")

    for table_def in TABLES:
        name      = table_def["name"]
        schema    = table_def["schema"]
        partition = table_def["partition"]

        if catalog.table_exists(name):
            log.info("Already exists, skipping: %s", name)
            continue

        table = catalog.create_table(
            identifier=name,
            schema=schema,
            partition_spec=partition,
        )
        log.info("Created: %s | %s", name, table.location())


def main():
    log.info("Connecting to Iceberg catalog...")
    catalog = get_catalog()
    log.info("Dropping existing tables...")
    drop_tables(catalog)
    log.info("Creating tables...")
    create_tables(catalog)
    log.info("All bronze tables ready.")


if __name__ == "__main__":
    main()