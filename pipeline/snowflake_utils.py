from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    role: str
    warehouse: str
    authenticator: str = "snowflake"
    password: str | None = None
    token: str | None = None
    database: str = "PORTFOLIO_DB"
    raw_schema: str = "RAW"
    staging_schema: str = "STAGING"
    marts_schema: str = "MARTS"
    query_tag: str = "snowflake-dbt-orchestrated-pipeline"

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        required_keys = [
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_ROLE",
            "SNOWFLAKE_WAREHOUSE",
        ]
        missing = [key for key in required_keys if not os.getenv(key)]
        if missing:
            missing_keys = ", ".join(missing)
            raise EnvironmentError(
                f"Missing Snowflake environment variables: {missing_keys}. "
                "Copy .env.example to .env and set the values."
            )

        authenticator = (
            os.getenv("SNOWFLAKE_AUTHENTICATOR", "snowflake").strip().lower()
        )
        password = os.getenv("SNOWFLAKE_PASSWORD")
        token = os.getenv("SNOWFLAKE_TOKEN")

        if authenticator == "oauth":
            # Backward-compatible fallback: allow PAT in SNOWFLAKE_PASSWORD when token is omitted.
            token = token or password
            if not token:
                raise EnvironmentError(
                    "SNOWFLAKE_TOKEN is required when SNOWFLAKE_AUTHENTICATOR=oauth."
                )
            password = None
        elif not password:
            raise EnvironmentError(
                "SNOWFLAKE_PASSWORD is required when using password-based Snowflake auth."
            )

        return cls(
            account=_normalize_account_identifier(os.environ["SNOWFLAKE_ACCOUNT"]),
            user=os.environ["SNOWFLAKE_USER"],
            role=os.environ["SNOWFLAKE_ROLE"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            authenticator=authenticator,
            password=password,
            token=token,
            database=os.getenv("SNOWFLAKE_DATABASE", "PORTFOLIO_DB"),
            raw_schema=os.getenv("SNOWFLAKE_SCHEMA_RAW", "RAW"),
            staging_schema=os.getenv("SNOWFLAKE_SCHEMA_STAGING", "STAGING"),
            marts_schema=os.getenv("SNOWFLAKE_SCHEMA_MARTS", "MARTS"),
            query_tag=os.getenv(
                "SNOWFLAKE_QUERY_TAG", "snowflake-dbt-orchestrated-pipeline"
            ),
        )


def _normalize_account_identifier(account: str) -> str:
    normalized = account.strip()
    if normalized.startswith("https://"):
        normalized = normalized.removeprefix("https://")
    if normalized.startswith("http://"):
        normalized = normalized.removeprefix("http://")
    normalized = normalized.rstrip("/")
    if normalized.endswith(".snowflakecomputing.com"):
        normalized = normalized.removesuffix(".snowflakecomputing.com")
    return normalized


def get_snowflake_connection(
    config: SnowflakeConfig,
) -> snowflake.connector.SnowflakeConnection:
    connect_kwargs: dict[str, object] = dict(
        account=config.account,
        user=config.user,
        role=config.role,
        warehouse=config.warehouse,
        database=config.database,
        session_parameters={"QUERY_TAG": config.query_tag},
    )
    if config.authenticator == "oauth":
        connect_kwargs["authenticator"] = "oauth"
        connect_kwargs["token"] = config.token
    else:
        connect_kwargs["password"] = config.password
        if config.authenticator != "snowflake":
            connect_kwargs["authenticator"] = config.authenticator

    return snowflake.connector.connect(**connect_kwargs)
