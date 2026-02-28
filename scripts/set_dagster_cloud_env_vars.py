from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import requests
from dotenv import dotenv_values

MUTATION = """
mutation CreateOrUpdateSecretForScopes(
  $secretName: String!
  $secretValue: String!
  $scopes: SecretScopesInput!
  $locationName: String
) {
  createOrUpdateSecretForScopes(
    secretName: $secretName
    secretValue: $secretValue
    scopes: $scopes
    locationName: $locationName
  ) {
    __typename
    ... on CreateOrUpdateSecretSuccess {
      secret {
        secretName
      }
    }
    ... on SecretAlreadyExistsError {
      message
    }
    ... on TooManySecretsError {
      message
    }
    ... on InvalidSecretInputError {
      message
    }
    ... on PythonError {
      message
      stack
    }
    ... on UnauthorizedError {
      message
    }
  }
}
"""

REQUIRED_BASE_KEYS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_AUTHENTICATOR",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA_RAW",
    "SNOWFLAKE_SCHEMA_STAGING",
    "SNOWFLAKE_SCHEMA_MARTS",
    "SNOWFLAKE_DBT_PROJECT_FQN",
    "SNOWFLAKE_DBT_ARGS",
]

DEFAULT_VALUES = {
    "SNOWFLAKE_AUTHENTICATOR": "snowflake",
    "SNOWFLAKE_DATABASE": "PORTFOLIO_DB",
    "SNOWFLAKE_SCHEMA_RAW": "RAW",
    "SNOWFLAKE_SCHEMA_STAGING": "STAGING",
    "SNOWFLAKE_SCHEMA_MARTS": "MARTS",
    "SNOWFLAKE_DBT_PROJECT_FQN": "PORTFOLIO_DB.MARTS.TRIPS_DBT_PROJECT",
    "SNOWFLAKE_DBT_ARGS": "build --target prod",
}


@dataclass(frozen=True)
class DagsterCloudConfig:
    api_token: str
    organization: str
    deployment: str

    @property
    def graphql_url(self) -> str:
        return f"https://{self.organization}.dagster.cloud/graphql"


class DagsterCloudSyncError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create/update Dagster Cloud environment variables for Snowflake runtime. "
            "Values are read from shell env first, then from a dotenv file."
        )
    )
    parser.add_argument(
        "--dotenv-path",
        type=Path,
        default=Path(".env"),
        help="Path to dotenv file with Snowflake values",
    )
    parser.add_argument(
        "--organization",
        default=os.getenv("DAGSTER_CLOUD_ORGANIZATION", "th"),
        help="Dagster Cloud organization slug",
    )
    parser.add_argument(
        "--deployment",
        default=os.getenv("DAGSTER_CLOUD_DEPLOYMENT", "prod"),
        help="Dagster Cloud full deployment name",
    )
    parser.add_argument(
        "--token-env-var",
        default="DAGSTER_CLOUD_API_TOKEN",
        help="Environment variable name containing Dagster Cloud API token",
    )
    parser.add_argument(
        "--scope",
        action="append",
        choices=["full", "branch", "local"],
        default=["full", "branch"],
        help=(
            "Secret scope in Dagster Cloud. Repeat for multiple values. "
            "Defaults to full + branch."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be synced without calling Dagster Cloud",
    )
    return parser.parse_args()


def load_sources(dotenv_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if dotenv_path.exists():
        for key, value in dotenv_values(dotenv_path).items():
            if value is not None:
                values[key] = value

    # Shell values should override dotenv values when both are present.
    for key, value in os.environ.items():
        if value:
            values[key] = value

    return values


def resolve_dagster_cloud_config(args: argparse.Namespace) -> DagsterCloudConfig:
    api_token = os.getenv(args.token_env_var, "").strip()
    if not api_token:
        raise DagsterCloudSyncError(
            f"Missing Dagster API token. Set {args.token_env_var} before running sync."
        )

    organization = args.organization.strip()
    deployment = args.deployment.strip()
    if not organization or not deployment:
        raise DagsterCloudSyncError("organization and deployment are required")

    return DagsterCloudConfig(
        api_token=api_token,
        organization=organization,
        deployment=deployment,
    )


def resolve_env_vars(source_values: dict[str, str]) -> dict[str, str]:
    merged_values = {**DEFAULT_VALUES, **source_values}

    missing = [key for key in REQUIRED_BASE_KEYS if not merged_values.get(key)]
    if missing:
        raise DagsterCloudSyncError(
            "Missing required Snowflake vars: " + ", ".join(sorted(missing))
        )

    auth = merged_values["SNOWFLAKE_AUTHENTICATOR"].strip().lower()
    resolved = {key: merged_values[key] for key in REQUIRED_BASE_KEYS}

    if auth == "oauth":
        token = merged_values.get("SNOWFLAKE_TOKEN", "").strip()
        # Backward-compatible fallback for setups that still keep PAT in SNOWFLAKE_PASSWORD.
        token = token or merged_values.get("SNOWFLAKE_PASSWORD", "").strip()
        if not token:
            raise DagsterCloudSyncError(
                "SNOWFLAKE_TOKEN is required when SNOWFLAKE_AUTHENTICATOR=oauth"
            )
        resolved["SNOWFLAKE_TOKEN"] = token
    else:
        password = merged_values.get("SNOWFLAKE_PASSWORD", "").strip()
        if not password:
            raise DagsterCloudSyncError(
                "SNOWFLAKE_PASSWORD is required for non-oauth authenticator"
            )
        resolved["SNOWFLAKE_PASSWORD"] = password

    return resolved


def build_scopes(scope_values: list[str]) -> dict[str, bool]:
    scopes = set(scope_values)
    return {
        "fullDeploymentScope": "full" in scopes,
        "allBranchDeploymentsScope": "branch" in scopes,
        "localDeploymentScope": "local" in scopes,
    }


def sync_one_secret(
    config: DagsterCloudConfig,
    *,
    name: str,
    value: str,
    scopes: dict[str, bool],
) -> None:
    headers = {
        "Content-Type": "application/json",
        "Dagster-Cloud-Api-Token": config.api_token,
        "Dagster-Cloud-Organization": config.organization,
        "Dagster-Cloud-Deployment": config.deployment,
    }
    payload = {
        "query": MUTATION,
        "variables": {
            "secretName": name,
            "secretValue": value,
            "scopes": scopes,
            # locationName=None means deployment-global secret (all code locations)
            "locationName": None,
        },
    }

    response = requests.post(
        config.graphql_url,
        headers=headers,
        json=payload,
        timeout=30,
    )
    if response.status_code != 200:
        raise DagsterCloudSyncError(
            f"HTTP {response.status_code} while syncing {name}: {response.text[:300]}"
        )

    body = response.json()
    if body.get("errors"):
        raise DagsterCloudSyncError(
            f"GraphQL error while syncing {name}: {json.dumps(body['errors'])}"
        )

    result = body.get("data", {}).get("createOrUpdateSecretForScopes", {})
    typename = result.get("__typename")
    if typename != "CreateOrUpdateSecretSuccess":
        message = result.get("message", "Unknown Dagster Cloud error")
        raise DagsterCloudSyncError(
            f"Failed syncing {name} ({typename or 'UnknownType'}): {message}"
        )


def main() -> None:
    args = parse_args()

    try:
        config = resolve_dagster_cloud_config(args)
        source_values = load_sources(args.dotenv_path)
        env_vars = resolve_env_vars(source_values)
        scopes = build_scopes(args.scope)
    except DagsterCloudSyncError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    keys = sorted(env_vars)
    print(
        "Preparing Dagster Cloud env var sync for "
        f"{config.organization}/{config.deployment} with scopes={args.scope}"
    )
    print(f"Variables to sync: {', '.join(keys)}")

    if args.dry_run:
        print("Dry run enabled. No API calls were made.")
        return

    for key in keys:
        sync_one_secret(config, name=key, value=env_vars[key], scopes=scopes)
        print(f"Synced: {key}")

    print("Dagster Cloud environment variable sync complete.")


if __name__ == "__main__":
    main()
