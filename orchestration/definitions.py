import os
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

# ── Load all assets ────────────────────────────────────────────────────────────
# load_assets_from_modules() scans the module for any function
# decorated with @asset and collects them all automatically.
# You don't have to list every asset manually.
bronze_asset_list = load_assets_from_modules([bronze_assets])

# dbt assets are already collected in the dbt_assets module
dbt_asset_list = [dbt_assets.market_pulse_dbt_assets]

all_assets = [*bronze_asset_list, *dbt_asset_list]


# ── Resources ──────────────────────────────────────────────────────────────────
# Resources are shared connections or clients that assets use.
# Instead of every asset creating its own dbt connection,
# they all share one DbtCliResource defined here.
#
# This is dependency injection — assets declare what resource
# they need (dbt: DbtCliResource in the function signature)
# and Dagster provides it automatically at runtime.

DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt_project"

resources = {
    "dbt": DbtCliResource(
        project_dir=str(DBT_PROJECT_DIR),
        profiles_dir=str(Path.home() / ".dbt"),
    ),
}


# ── Definitions ────────────────────────────────────────────────────────────────
# Definitions is the top-level object that Dagster reads.
# It's the manifest of everything your pipeline contains.
# Think of it as the table of contents for your entire pipeline.

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