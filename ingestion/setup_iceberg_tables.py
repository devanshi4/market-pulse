import os
import logging
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    StringType,
    LongType,
    TimestampType,
    IntegerType,
    DoubleType,
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


# ── Catalog connection ─────────────────────────────────────────────────────────
def get_catalog() -> SqlCatalog:
    """
    Connect to the Iceberg catalog.

    SqlCatalog uses a local SQLite database to store the table registry.
    The actual data files live on GCS — SQLite only stores the mapping
    of table names to their GCS locations and schemas.

    In production this would be replaced with a Hive Metastore,
    AWS Glue catalog, or a REST catalog.
    """
    bucket   = os.getenv("GCS_BUCKET")
    project  = os.getenv("GCP_PROJECT_ID")
    creds    = os.path.abspath(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    cat_db   = os.getenv("ICEBERG_CATALOG_DB", "market_pulse_catalog.db")
    cat_name = os.getenv("ICEBERG_CATALOG_NAME", "market_pulse")

    return SqlCatalog(
        cat_name,
        **{
            "uri":                        f"sqlite:///{cat_db}",
            "warehouse":                  f"gs://{bucket}/bronze",
            "gcs.project-id":             project,
            "gcs.oauth2.credentials-file": creds,
        },
    )


# ── Schema definitions ─────────────────────────────────────────────────────────
# Every bronze table has two categories of fields:
#
# 1. Business fields  — the actual data from Postgres/Yahoo Finance
# 2. Metadata fields  — added by our pipeline to track provenance
#
# Metadata fields start with underscore by convention.
# They answer: where did this row come from, when did we receive it,
# and what operation caused it?
#
# _ingested_at   : when our pipeline wrote this row to Iceberg
# _kafka_topic   : which Kafka topic it came from
# _kafka_partition: which partition within that topic
# _kafka_offset  : the offset — unique position of this event in the partition
# _cdc_op        : CDC operation — c=create, u=update, d=delete, r=snapshot read
#
# The combination of (_kafka_topic, _kafka_partition, _kafka_offset) uniquely
# identifies any event in Kafka. This lets you trace any Iceberg row back to
# the exact Kafka message it came from. Critical for debugging and auditing.

TRADES_SCHEMA = Schema(
    NestedField(1,  "trade_id",       LongType(),      required=False),
    NestedField(2,  "account_id",     LongType(),      required=False),
    NestedField(3,  "ticker",         StringType(),    required=False),
    NestedField(4,  "trade_type",     StringType(),    required=False),
    NestedField(5,  "quantity",       LongType(),      required=False),
    NestedField(6,  "price",          StringType(),    required=False),
    NestedField(7,  "traded_at",      StringType(),    required=False),
    NestedField(8,  "_ingested_at",   TimestampType(), required=False),
    NestedField(9,  "_kafka_topic",   StringType(),    required=False),
    NestedField(10, "_kafka_partition", IntegerType(), required=False),
    NestedField(11, "_kafka_offset",  LongType(),      required=False),
    NestedField(12, "_cdc_op",        StringType(),    required=False),
)

POSITIONS_SCHEMA = Schema(
    NestedField(1,  "position_id",    LongType(),      required=False),
    NestedField(2,  "account_id",     LongType(),      required=False),
    NestedField(3,  "ticker",         StringType(),    required=False),
    NestedField(4,  "shares_held",    StringType(),    required=False),
    NestedField(5,  "avg_buy_price",  StringType(),    required=False),
    NestedField(6,  "last_updated",   StringType(),    required=False),
    NestedField(7,  "_ingested_at",   TimestampType(), required=False),
    NestedField(8,  "_kafka_topic",   StringType(),    required=False),
    NestedField(9,  "_kafka_partition", IntegerType(), required=False),
    NestedField(10, "_kafka_offset",  LongType(),      required=False),
    NestedField(11, "_cdc_op",        StringType(),    required=False),
)

ACCOUNTS_SCHEMA = Schema(
    NestedField(1,  "account_id",     LongType(),      required=False),
    NestedField(2,  "owner_name",     StringType(),    required=False),
    NestedField(3,  "email",          StringType(),    required=False),
    NestedField(4,  "balance",        StringType(),    required=False),
    NestedField(5,  "created_at",     StringType(),    required=False),
    NestedField(6,  "_ingested_at",   TimestampType(), required=False),
    NestedField(7,  "_kafka_topic",   StringType(),    required=False),
    NestedField(8,  "_kafka_partition", IntegerType(), required=False),
    NestedField(9,  "_kafka_offset",  LongType(),      required=False),
    NestedField(10, "_cdc_op",        StringType(),    required=False),
)

MARKET_PRICES_SCHEMA = Schema(
    NestedField(1,  "ticker",         StringType(),    required=False),
    NestedField(2,  "open_price",     StringType(),    required=False),
    NestedField(3,  "high_price",     StringType(),    required=False),
    NestedField(4,  "low_price",      StringType(),    required=False),
    NestedField(5,  "close_price",    StringType(),    required=False),
    NestedField(6,  "volume",         LongType(),      required=False),
    NestedField(7,  "fetched_at",     StringType(),    required=False),
    NestedField(8,  "_ingested_at",   TimestampType(), required=False),
    NestedField(9,  "_kafka_topic",   StringType(),    required=False),
    NestedField(10, "_kafka_partition", IntegerType(), required=False),
    NestedField(11, "_kafka_offset",  LongType(),      required=False),
)


# ── Partition specs ────────────────────────────────────────────────────────────
# Partitioning physically organises data files on GCS by a chosen column.
# When you query "give me all trades from today", Iceberg skips every
# partition except today's — it never reads those files at all.
# This is called partition pruning and it's how Iceberg makes queries fast
# on large datasets without a traditional database index.
#
# DayTransform on _ingested_at means:
# - All rows ingested on 2025-05-17 land in one folder
# - All rows ingested on 2025-05-18 land in another folder
# - A query filtered to 2025-05-17 only reads the first folder
#
# IdentityTransform on ticker means:
# - All AAPL rows land in one folder
# - All TSLA rows land in another
# - A query for AAPL only reads the AAPL folder

TRADES_PARTITION = PartitionSpec(
    PartitionField(
        source_id=8,          # field_id of _ingested_at in TRADES_SCHEMA
        field_id=1000,
        transform=DayTransform(),
        name="ingested_day",
    )
)

PRICES_PARTITION = PartitionSpec(
    PartitionField(
        source_id=8,          # field_id of _ingested_at in MARKET_PRICES_SCHEMA
        field_id=1000,
        transform=DayTransform(),
        name="ingested_day",
    ),
    PartitionField(
        source_id=1,          # field_id of ticker
        field_id=1001,
        transform=IdentityTransform(),
        name="ticker",
    )
)

# Trades and positions partition by day only
# Prices partition by day AND ticker — because price queries almost
# always filter by both time range and specific stock


# ── Table creation ─────────────────────────────────────────────────────────────
TABLES = [
    {
        "name":      "bronze.trades",
        "schema":    TRADES_SCHEMA,
        "partition": TRADES_PARTITION,
    },
    {
        "name":      "bronze.positions",
        "schema":    POSITIONS_SCHEMA,
        "partition": TRADES_PARTITION,   # same day partitioning
    },
    {
        "name":      "bronze.accounts",
        "schema":    ACCOUNTS_SCHEMA,
        "partition": PartitionSpec(),    # no partitioning — small table
    },
    {
        "name":      "bronze.market_prices",
        "schema":    MARKET_PRICES_SCHEMA,
        "partition": PRICES_PARTITION,
    },
]


def create_tables(catalog: SqlCatalog):
    # Create the bronze namespace (like a schema in a database)
    existing_namespaces = [ns[0] for ns in catalog.list_namespaces()]
    if "bronze" not in existing_namespaces:
        catalog.create_namespace("bronze")
        log.info("Created namespace: bronze")
    else:
        log.info("Namespace bronze already exists")

    for table_def in TABLES:
        name      = table_def["name"]
        schema    = table_def["schema"]
        partition = table_def["partition"]

        if catalog.table_exists(name):
            log.info("Table already exists, skipping: %s", name)
            continue

        table = catalog.create_table(
            identifier=name,
            schema=schema,
            partition_spec=partition,
        )
        log.info(
            "Created table: %s | location: %s",
            name, table.location()
        )


def main():
    log.info("Connecting to Iceberg catalog...")
    catalog = get_catalog()
    log.info("Catalog connected. Creating bronze tables...")
    create_tables(catalog)
    log.info("All bronze tables ready.")


if __name__ == "__main__":
    main()