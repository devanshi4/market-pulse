"""Dagster entry point: assets, jobs, schedules and the shared dbt resource."""
from pathlib import Path
from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from orchestration.assets import bronze_assets, dbt_assets
from orchestration.jobs.pipeline_jobs import (
    bronze_validation_job,
    dbt_transformation_job,
    full_pipeline_job,
)
from orchestration.schedules.pipeline_schedules import (
    hourly_dbt_schedule,
    daily_full_pipeline_schedule,
)

bronze_asset_list = load_assets_from_modules([bronze_assets])

dbt_asset_list = [dbt_assets.market_pulse_dbt_assets]

all_assets = [*bronze_asset_list, *dbt_asset_list]


DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_project"

resources = {
    "dbt": DbtCliResource(
        project_dir=str(DBT_PROJECT_DIR),
        profiles_dir=str(Path.home() / ".dbt"),
    ),
}


defs = Definitions(
    assets=all_assets,
    jobs=[
        bronze_validation_job,
        dbt_transformation_job,
        full_pipeline_job,
    ],
    schedules=[
        hourly_dbt_schedule,
        daily_full_pipeline_schedule,
    ],
    resources=resources,
)