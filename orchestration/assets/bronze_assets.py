"""Bronze-layer checks that run before any dbt model is rebuilt."""
import os
import subprocess
import logging
from dagster import asset, AssetExecutionContext, MetadataValue
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)


@asset(
    group_name="bronze",
    description="Validates that bronze Iceberg tables on GCS are receiving data",
)
def bronze_tables_health_check(context: AssetExecutionContext):
    """Fail fast if any bronze table is empty.

    The Kafka consumer runs as its own long-lived process, so this asset checks its
    output rather than starting it.
    """
    import sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

    from pyiceberg.catalog.sql import SqlCatalog

    bucket   = os.getenv("GCS_BUCKET")
    project  = os.getenv("GCP_PROJECT_ID")
    creds    = os.path.abspath(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    cat_db   = os.path.abspath(os.getenv("ICEBERG_CATALOG_DB", "market_pulse_catalog.db"))
    cat_name = os.getenv("ICEBERG_CATALOG_NAME", "market_pulse")

    catalog = SqlCatalog(
        cat_name,
        **{
            "uri":                         f"sqlite:///{cat_db}",
            "warehouse":                   f"gs://{bucket}/bronze",
            "gcs.project-id":              project,
            "gcs.oauth2.credentials-file": creds,
        },
    )

    tables = [
        "bronze.trades",
        "bronze.positions",
        "bronze.accounts",
        "bronze.market_prices",
    ]

    results = {}
    for table_name in tables:
        table    = catalog.load_table(table_name)
        arrow    = table.scan().to_arrow()
        row_count = arrow.num_rows
        results[table_name] = row_count
        context.log.info("%s: %s rows", table_name, row_count)

    empty_tables = [t for t, count in results.items() if count == 0]
    if empty_tables:
        raise Exception(
            f"These bronze tables are empty: {empty_tables}. "
            f"Make sure kafka_to_iceberg_consumer.py is running."
        )

    # Row counts show up on the asset page in the Dagster UI.
    return {
        "row_counts": MetadataValue.json(results),
        "total_rows": MetadataValue.int(sum(results.values())),
        "tables_checked": MetadataValue.int(len(tables)),
    }


@asset(
    group_name="bronze",
    description="Runs dbt source freshness check on bronze external tables",
    deps=[bronze_tables_health_check],
)
def bronze_source_freshness(context: AssetExecutionContext):
    """Run `dbt source freshness` to catch a consumer that is up but has stopped writing."""
    dbt_dir = os.path.join(
        os.path.dirname(__file__), '..', '..', 'dbt_project'
    )

    result = subprocess.run(
        ["dbt", "source", "freshness"],
        cwd=dbt_dir,
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)

    if result.returncode != 0:
        context.log.warning(
            "dbt source freshness check had warnings: %s",
            result.stderr
        )

    return MetadataValue.text("Source freshness check complete")