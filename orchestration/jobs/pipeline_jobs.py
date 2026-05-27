from dagster import define_asset_job, AssetSelection

# ── Bronze validation job ──────────────────────────────────────────────────────
# Runs all assets in the "bronze" group
# Used to validate that data is flowing into GCS
bronze_validation_job = define_asset_job(
    name="bronze_validation_job",
    selection=AssetSelection.groups("bronze"),
    description="Validates bronze layer health — checks row counts and freshness",
)

# ── dbt transformation job ─────────────────────────────────────────────────────
# Runs all dbt models (silver + gold)
# AssetSelection.all() selects every asset Dagster knows about
# We filter out the bronze group to only run dbt models
dbt_transformation_job = define_asset_job(
    name="dbt_transformation_job",
    selection=AssetSelection.all() - AssetSelection.groups("bronze"),
    description="Runs all dbt silver and gold transformations",
)

# ── Full pipeline job ──────────────────────────────────────────────────────────
# Runs everything in order:
# 1. Validate bronze is healthy
# 2. Run all dbt transformations
full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all(),
    description="Full pipeline — bronze validation then dbt transformations",
)