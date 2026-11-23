from dagster import define_asset_job, AssetSelection

bronze_validation_job = define_asset_job(
    name="bronze_validation_job",
    selection=AssetSelection.groups("bronze"),
    description="Validates bronze layer health — checks row counts and freshness",
)

dbt_transformation_job = define_asset_job(
    name="dbt_transformation_job",
    selection=AssetSelection.all() - AssetSelection.groups("bronze"),
    description="Runs all dbt silver and gold transformations",
)

full_pipeline_job = define_asset_job(
    name="full_pipeline_job",
    selection=AssetSelection.all(),
    description="Full pipeline — bronze validation then dbt transformations",
)