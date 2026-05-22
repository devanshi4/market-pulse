import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
from pyiceberg.catalog.sql import SqlCatalog

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


def validate_table(catalog: SqlCatalog, table_name: str):
    print(f"\n{'='*60}")
    print(f"  {table_name}")
    print(f"{'='*60}")

    table = catalog.load_table(table_name)

    # ── Row count ──────────────────────────────────────────────
    arrow  = table.scan().to_arrow()
    count  = arrow.num_rows
    print(f"  Total rows      : {count}")

    if count == 0:
        print("  WARNING: table is empty")
        return

    # ── Snapshot history (time travel) ─────────────────────────
    snapshots = table.metadata.snapshots
    print(f"  Snapshots       : {len(snapshots)}  (one per flush batch)")

    # ── Schema ─────────────────────────────────────────────────
    print(f"  Columns         : {len(table.schema().fields)}")
    for field in table.schema().fields:
        print(f"    {field.field_id:>3}. {field.name:<22} {field.field_type}")

    # ── Sample rows ────────────────────────────────────────────
    print(f"\n  Sample (first 3 rows):")
    sample = table.scan(limit=3).to_arrow()
    # Print each row as a readable dictionary
    for i in range(sample.num_rows):
        row = {col: sample.column(col)[i].as_py() for col in sample.column_names}
        # Only print key fields to keep output readable
        key_fields = [
            k for k in row.keys()
            if not k.startswith("_kafka")
        ]
        print(f"    row {i+1}:")
        for k in key_fields:
            print(f"      {k:<22}: {row[k]}")

    # ── Metadata fields check ───────────────────────────────────
    print(f"\n  Metadata field check:")
    meta_cols = ["_ingested_at", "_kafka_topic", "_kafka_partition",
                 "_kafka_offset"]
    for col in meta_cols:
        if col in sample.column_names:
            val = sample.column(col)[0].as_py()
            print(f"    {col:<22}: {val}  ✓")
        else:
            print(f"    {col:<22}: MISSING  ✗")

    # ── Null check on critical fields ──────────────────────────
    print(f"\n  Null check (critical fields):")
    critical = {
        "bronze.trades":        ["trade_id", "ticker", "price"],
        "bronze.positions":     ["position_id", "ticker"],
        "bronze.accounts":      ["account_id", "email"],
        "bronze.market_prices": ["ticker", "close_price"],
    }
    for col in critical.get(table_name, []):
        if col not in arrow.column_names:
            print(f"    {col:<22}: column missing  ✗")
            continue
        null_count = arrow.column(col).null_count
        status     = "✓" if null_count == 0 else f"✗  {null_count} nulls"
        print(f"    {col:<22}: {status}")


def validate_snapshots(catalog: SqlCatalog):
    """
    Demonstrate time travel — show that Iceberg keeps a full
    history of every write as a snapshot.
    """
    print(f"\n{'='*60}")
    print(f"  Time travel — snapshot history for bronze.trades")
    print(f"{'='*60}")

    table     = catalog.load_table("bronze.trades")
    snapshots = table.metadata.snapshots

    for snap in snapshots[-3:]:   # show last 3 snapshots
        ts = datetime.fromtimestamp(
            snap.timestamp_ms / 1000, tz=timezone.utc
        )
        print(f"  snapshot_id={snap.snapshot_id}")
        print(f"    written at : {ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"    operation  : {snap.summary.get('operation', 'unknown')}")
        print(f"    added files: {snap.summary.get('added-data-files', '?')}")
        print(f"    added rows : {snap.summary.get('added-records', '?')}")
        print()


def main():
    print("\nMarket Pulse — Bronze Layer Validation")
    print(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))

    catalog = get_catalog()

    tables = [
        "bronze.trades",
        "bronze.positions",
        "bronze.accounts",
        "bronze.market_prices",
    ]

    for table_name in tables:
        try:
            validate_table(catalog, table_name)
        except Exception as e:
            print(f"\n  ERROR validating {table_name}: {e}")

    validate_snapshots(catalog)

    print(f"\n{'='*60}")
    print("  Validation complete")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()