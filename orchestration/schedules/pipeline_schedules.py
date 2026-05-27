from dagster import ScheduleDefinition
from orchestration.jobs.pipeline_jobs import (
    dbt_transformation_job,
    full_pipeline_job,
)

# ── Hourly dbt refresh ─────────────────────────────────────────────────────────
# Runs dbt silver + gold transformations every hour
# so dashboards always have fresh data
hourly_dbt_schedule = ScheduleDefinition(
    job=dbt_transformation_job,
    cron_schedule="0 * * * *",     # every hour at :00
    name="hourly_dbt_refresh",
    description="Refreshes all silver and gold dbt models every hour",
)

# ── Daily full pipeline ────────────────────────────────────────────────────────
# Runs the complete pipeline every day at 6am
# Validates bronze health then refreshes all transformations
daily_full_pipeline_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 6 * * *",     # every day at 6:00am
    name="daily_full_pipeline",
    description="Full pipeline run every morning at 6am",
)