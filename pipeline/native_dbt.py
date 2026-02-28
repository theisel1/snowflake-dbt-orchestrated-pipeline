from __future__ import annotations

import argparse
import logging
import os
import re
from dataclasses import dataclass

from pipeline.snowflake_utils import SnowflakeConfig, get_snowflake_connection

LOGGER = logging.getLogger(__name__)

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9_.\"]+$")
_REF_PATH_PATTERN = re.compile(r"^[A-Za-z0-9_./-]+$")
_TARGET_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class NativeDbtExecutionResult:
    project_fqn: str
    args: str
    query_id: str | None


@dataclass(frozen=True)
class NativeDbtDeploymentResult:
    project_fqn: str
    git_repo_fqn: str
    source_location: str


def _validate_identifier(value: str, label: str) -> str:
    candidate = value.strip()
    if not candidate:
        raise ValueError(f"{label} cannot be empty")
    if not _IDENTIFIER_PATTERN.match(candidate):
        raise ValueError(
            f"{label} contains unsupported characters: {candidate!r}. "
            "Use a fully-qualified Snowflake identifier."
        )
    return candidate


def _validate_ref_path(value: str, label: str) -> str:
    candidate = value.strip().strip("/")
    if not candidate:
        raise ValueError(f"{label} cannot be empty")
    if not _REF_PATH_PATTERN.match(candidate):
        raise ValueError(
            f"{label} contains unsupported characters: {candidate!r}. "
            "Use only letters, digits, _, -, ., and /."
        )
    return candidate


def _validate_target(value: str, label: str) -> str:
    candidate = value.strip()
    if not candidate:
        raise ValueError(f"{label} cannot be empty")
    if not _TARGET_PATTERN.match(candidate):
        raise ValueError(
            f"{label} contains unsupported characters: {candidate!r}. "
            "Use only letters, digits, underscores, and dashes."
        )
    return candidate


def _external_integrations_clause(integrations: list[str]) -> str:
    if not integrations:
        return ""

    cleaned = [
        _validate_identifier(value, "external access integration")
        for value in integrations
    ]
    return f" external_access_integrations = ({', '.join(cleaned)})"


def deploy_dbt_project_from_git(
    *,
    project_fqn: str,
    git_repo_fqn: str,
    branch: str,
    project_root: str,
    default_target: str,
    external_access_integrations: list[str] | None = None,
) -> NativeDbtDeploymentResult:
    """Create or replace a Snowflake DBT PROJECT object from a Snowflake GIT REPOSITORY object."""
    project_fqn = _validate_identifier(project_fqn, "project_fqn")
    git_repo_fqn = _validate_identifier(git_repo_fqn, "git_repo_fqn")
    branch = _validate_ref_path(branch, "branch")
    project_root = _validate_ref_path(project_root, "project_root")
    default_target = _validate_target(default_target, "default_target")

    source_location = f"@{git_repo_fqn}/branches/{branch}/{project_root}"
    integration_clause = _external_integrations_clause(
        external_access_integrations or []
    )

    create_sql = (
        f"create or replace dbt project {project_fqn} "
        f"from '{source_location}' "
        f"default_target = '{default_target}'"
        f"{integration_clause}"
    )

    config = SnowflakeConfig.from_env()
    with get_snowflake_connection(config) as connection:
        with connection.cursor() as cursor:
            LOGGER.info("Fetching latest refs from %s", git_repo_fqn)
            cursor.execute(f"alter git repository {git_repo_fqn} fetch")

            LOGGER.info(
                "Deploying DBT PROJECT object %s from %s", project_fqn, source_location
            )
            cursor.execute(create_sql)

    return NativeDbtDeploymentResult(
        project_fqn=project_fqn,
        git_repo_fqn=git_repo_fqn,
        source_location=source_location,
    )


def execute_dbt_project(*, project_fqn: str, args: str) -> NativeDbtExecutionResult:
    """Execute a Snowflake DBT PROJECT object natively with EXECUTE DBT PROJECT."""
    project_fqn = _validate_identifier(project_fqn, "project_fqn")
    args = args.strip()
    if not args:
        raise ValueError("args cannot be empty")

    sql = f"execute dbt project {project_fqn} args = %s"

    config = SnowflakeConfig.from_env()
    with get_snowflake_connection(config) as connection:
        with connection.cursor() as cursor:
            LOGGER.info(
                "Executing native dbt project %s with args=%s", project_fqn, args
            )
            cursor.execute(sql, (args,))
            query_id = cursor.sfqid

    return NativeDbtExecutionResult(
        project_fqn=project_fqn, args=args, query_id=query_id
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manage native Snowflake DBT PROJECT execution"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    execute_parser = subparsers.add_parser("execute", help="Run EXECUTE DBT PROJECT")
    execute_parser.add_argument(
        "--project-fqn",
        default=os.getenv(
            "SNOWFLAKE_DBT_PROJECT_FQN", "PORTFOLIO_DB.MARTS.TRIPS_DBT_PROJECT"
        ),
        help="Fully-qualified Snowflake DBT PROJECT object name",
    )
    execute_parser.add_argument(
        "--args",
        default=os.getenv("SNOWFLAKE_DBT_ARGS", "build --target prod"),
        help="dbt CLI args string passed to EXECUTE DBT PROJECT",
    )

    deploy_parser = subparsers.add_parser(
        "deploy",
        help="Create or replace DBT PROJECT from a Snowflake GIT REPOSITORY object",
    )
    deploy_parser.add_argument(
        "--project-fqn",
        default=os.getenv(
            "SNOWFLAKE_DBT_PROJECT_FQN", "PORTFOLIO_DB.MARTS.TRIPS_DBT_PROJECT"
        ),
        help="Fully-qualified Snowflake DBT PROJECT object name",
    )
    deploy_parser.add_argument(
        "--git-repo-fqn",
        default=os.getenv(
            "SNOWFLAKE_DBT_GIT_REPO_FQN", "PORTFOLIO_DB.RAW.PORTFOLIO_GIT_REPO"
        ),
        help="Fully-qualified Snowflake GIT REPOSITORY object name",
    )
    deploy_parser.add_argument(
        "--branch",
        default=os.getenv("SNOWFLAKE_DBT_BRANCH", "main"),
        help="Git branch inside the Snowflake GIT REPOSITORY object",
    )
    deploy_parser.add_argument(
        "--project-root",
        default=os.getenv("SNOWFLAKE_DBT_PROJECT_ROOT", "dbt"),
        help="Path to the dbt project root within the repository",
    )
    deploy_parser.add_argument(
        "--default-target",
        default=os.getenv("SNOWFLAKE_DBT_DEFAULT_TARGET", "prod"),
        help="Default target profile for the Snowflake DBT PROJECT object",
    )
    deploy_parser.add_argument(
        "--external-access-integration",
        action="append",
        default=[],
        help="External access integration(s) for dbt deps/downloads (repeat flag for multiple)",
    )

    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def main() -> None:
    configure_logging()
    args = parse_args()

    if args.command == "execute":
        result = execute_dbt_project(project_fqn=args.project_fqn, args=args.args)
        LOGGER.info(
            "Native dbt execution completed for %s (query_id=%s)",
            result.project_fqn,
            result.query_id,
        )
        return

    if args.command == "deploy":
        env_integrations = os.getenv("SNOWFLAKE_DBT_EXTERNAL_ACCESS_INTEGRATIONS", "")
        integrations = list(args.external_access_integration)
        if env_integrations:
            integrations.extend(
                [
                    value.strip()
                    for value in env_integrations.split(",")
                    if value.strip()
                ]
            )

        result = deploy_dbt_project_from_git(
            project_fqn=args.project_fqn,
            git_repo_fqn=args.git_repo_fqn,
            branch=args.branch,
            project_root=args.project_root,
            default_target=args.default_target,
            external_access_integrations=integrations,
        )
        LOGGER.info(
            "Deployed %s from %s",
            result.project_fqn,
            result.source_location,
        )
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
