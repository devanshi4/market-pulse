import os
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

# ── Find the dbt project ───────────────────────────────────────────────────────
# Path(__file__) is the path to this Python file
# .parent goes up one level to orchestration/assets/
# .parent again goes up to orchestration/
# .parent again goes up to market-pulse/
# then we add dbt_project to get the dbt folder
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_project"

# DbtProject reads the dbt_project.yml and profiles.yml
# so Dagster knows the full structure of your dbt project
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
)

# ── dbt_assets decorator ───────────────────────────────────────────────────────
# This is the magic of dagster-dbt.
# @dbt_assets reads your dbt project and automatically creates
# one Dagster asset for EVERY dbt model.
#
# So all 8 of your dbt models (4 silver + 4 gold) become
# 8 Dagster assets automatically — no manual definition needed.
# Dagster even reads the ref() dependencies between models
# and builds the correct dependency graph.
#
# manifest_path points to the compiled dbt manifest.
# Run "dbt compile" inside dbt_project/ to generate this file.

@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    project=dbt_project,
)
def market_pulse_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
):
    """
    All dbt models as Dagster assets.

    When Dagster runs this, it executes dbt run + dbt test
    for every model in the correct dependency order.

    context is Dagster's execution context — it gives you
    logging, metadata, and run information.

    dbt is the DbtCliResource — it knows how to call dbt
    commands from Python.
    """
    # dbt.cli() runs a dbt command
    # "run" executes all models
    # "--select" filters to specific models or tags
    # yield from streams the results back to Dagster
    # so you see real-time logs in the UI
    yield from dbt.cli(["run"], context=context).stream()

    # After running models, run tests
    # If any test fails, Dagster marks this asset as failed
    yield from dbt.cli(["test"], context=context).stream()