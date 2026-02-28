from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from dagster import Definitions, ScheduleDefinition, job, op

from pipeline.native_dbt import execute_dbt_project

REPO_ROOT = Path(__file__).resolve().parents[1]


def _native_dbt_project_fqn() -> str:
    return os.getenv(
        "SNOWFLAKE_DBT_PROJECT_FQN", "PORTFOLIO_DB.MARTS.TRIPS_DBT_PROJECT"
    )


def _native_dbt_args() -> str:
    return os.getenv("SNOWFLAKE_DBT_ARGS", "build --target prod")


def _run_command(command: list[str], cwd: Path) -> tuple[int, str, str]:
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.returncode, completed.stdout, completed.stderr


@op
def ingest_step(context) -> str:
    # Full refresh avoids duplicate trip_id collisions across repeated runs.
    command = [sys.executable, "-m", "pipeline.ingest", "--full-refresh"]
    context.log.info("Running ingestion command: %s", " ".join(command))

    return_code, stdout, stderr = _run_command(command=command, cwd=REPO_ROOT)

    if stdout:
        context.log.info(stdout)
    if stderr:
        context.log.warning(stderr)

    if return_code != 0:
        raise RuntimeError("Ingestion step failed")

    return "ingest_complete"


@op
def native_dbt_build_step(context, _ingest_done: str) -> str:
    del _ingest_done
    project_fqn = _native_dbt_project_fqn()
    dbt_args = _native_dbt_args()

    context.log.info(
        "Executing native Snowflake dbt project %s with args=%s",
        project_fqn,
        dbt_args,
    )

    result = execute_dbt_project(project_fqn=project_fqn, args=dbt_args)
    context.log.info("Native dbt execution query_id=%s", result.query_id)

    return "native_dbt_complete"


@op
def native_dbt_only_start() -> str:
    return "native_dbt_only_start"


@job
def daily_elt_job() -> None:
    native_dbt_build_step(ingest_step())


@job
def native_dbt_only_job() -> None:
    native_dbt_build_step(native_dbt_only_start())


daily_schedule = ScheduleDefinition(
    job=daily_elt_job,
    cron_schedule="0 7 * * *",
    execution_timezone="America/New_York",
)


defs = Definitions(
    jobs=[daily_elt_job, native_dbt_only_job],
    schedules=[daily_schedule],
)
