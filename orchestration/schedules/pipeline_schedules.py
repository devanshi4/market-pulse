from dagster import ScheduleDefinition
from orchestration.jobs.pipeline_jobs import (
    dbt_transformation_job,
    full_pipeline_job,
)

hourly_dbt_schedule = ScheduleDefinition(
    job=dbt_transformation_job,
    cron_schedule="0 * * * *",
    name="hourly_dbt_refresh",
    description="Refreshes all silver and gold dbt models every hour",
)

daily_full_pipeline_schedule = ScheduleDefinition(
    job=full_pipeline_job,
    cron_schedule="0 6 * * *",
    name="daily_full_pipeline",
    description="Full pipeline run every morning at 6am",
)