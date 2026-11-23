"""Every dbt model as a Dagster asset, with lineage taken from the dbt manifest."""
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt_project"

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
)

# Requires target/manifest.json: run `dbt compile` in dbt_project/ first.
@dbt_assets(
    manifest=DBT_PROJECT_DIR / "target" / "manifest.json",
    project=dbt_project,
)
def market_pulse_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
):
    """Build all silver and gold models, then run their tests. A failing test fails the run."""
    yield from dbt.cli(["run"], context=context).stream()

    yield from dbt.cli(["test"], context=context).stream()