from __future__ import annotations

import argparse
import logging
from pathlib import Path
from uuid import uuid4

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from pipeline.snowflake_utils import (
    REPO_ROOT,
    SnowflakeConfig,
    get_snowflake_connection,
)

LOGGER = logging.getLogger(__name__)

EXPECTED_COLUMNS = [
    "trip_id",
    "pickup_ts",
    "dropoff_ts",
    "vendor_id",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "pickup_borough",
    "dropoff_borough",
    "payment_type",
    "load_ts",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load sample trips CSV into Snowflake RAW.TRIPS"
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--full-refresh",
        action="store_true",
        help="Truncate RAW.TRIPS and reload from CSV",
    )
    mode_group.add_argument(
        "--append", action="store_true", help="Append CSV rows to RAW.TRIPS"
    )
    parser.add_argument(
        "--csv-path",
        type=Path,
        default=REPO_ROOT / "data" / "sample_trips.csv",
        help="Path to the CSV input file",
    )
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def create_objects(cursor, config: SnowflakeConfig) -> None:
    statements = [
        f"create database if not exists {config.database}",
        f"create schema if not exists {config.database}.{config.raw_schema}",
        f"create schema if not exists {config.database}.{config.staging_schema}",
        f"create schema if not exists {config.database}.{config.marts_schema}",
        (
            f"""
            create table if not exists {config.database}.{config.raw_schema}.TRIPS (
                TRIP_ID string,
                PICKUP_TS timestamp_ntz,
                DROPOFF_TS timestamp_ntz,
                VENDOR_ID string,
                PASSENGER_COUNT integer,
                TRIP_DISTANCE float,
                FARE_AMOUNT float,
                TIP_AMOUNT float,
                TOTAL_AMOUNT float,
                PICKUP_BOROUGH string,
                DROPOFF_BOROUGH string,
                PAYMENT_TYPE string,
                LOAD_TS timestamp_ntz
            )
            """
        ),
        (
            f"""
            create table if not exists {config.database}.{config.raw_schema}.LOAD_LOG (
                LOAD_ID string,
                ROW_COUNT integer,
                STARTED_AT timestamp_ntz,
                FINISHED_AT timestamp_ntz,
                STATUS string
            )
            """
        ),
    ]

    for statement in statements:
        cursor.execute(statement)


def read_and_validate_csv(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    missing_columns = sorted(set(EXPECTED_COLUMNS).difference(df.columns))
    if missing_columns:
        raise ValueError(
            f"CSV is missing required columns: {', '.join(missing_columns)}"
        )

    df = df[EXPECTED_COLUMNS].copy()
    for ts_column in ["pickup_ts", "dropoff_ts", "load_ts"]:
        df[ts_column] = pd.to_datetime(df[ts_column], errors="coerce")
        if df[ts_column].isna().any():
            raise ValueError(f"Column '{ts_column}' contains invalid timestamp values")

    int_columns = ["passenger_count"]
    float_columns = ["trip_distance", "fare_amount", "tip_amount", "total_amount"]

    for col in int_columns:
        df[col] = pd.to_numeric(df[col], errors="raise").astype("int64")
    for col in float_columns:
        df[col] = pd.to_numeric(df[col], errors="raise").astype("float64")

    return df


def insert_load_log_start(cursor, config: SnowflakeConfig, load_id: str) -> None:
    cursor.execute(
        (
            f"""
            insert into {config.database}.{config.raw_schema}.LOAD_LOG
            (LOAD_ID, ROW_COUNT, STARTED_AT, FINISHED_AT, STATUS)
            values (%s, %s, current_timestamp(), %s, %s)
            """
        ),
        (load_id, 0, None, "RUNNING"),
    )


def update_load_log_end(
    cursor, config: SnowflakeConfig, load_id: str, row_count: int, status: str
) -> None:
    cursor.execute(
        (
            f"""
            update {config.database}.{config.raw_schema}.LOAD_LOG
            set ROW_COUNT = %s,
                FINISHED_AT = current_timestamp(),
                STATUS = %s
            where LOAD_ID = %s
            """
        ),
        (row_count, status, load_id),
    )


def run_ingestion(full_refresh: bool, csv_path: Path) -> str:
    config = SnowflakeConfig.from_env()
    load_id = str(uuid4())
    log_started = False

    LOGGER.info("Starting ingestion with load_id=%s", load_id)
    LOGGER.info("Mode: %s", "full-refresh" if full_refresh else "append")
    LOGGER.info("CSV source: %s", csv_path)

    connection = get_snowflake_connection(config)
    connection.autocommit(False)

    try:
        with connection.cursor() as cursor:
            create_objects(cursor, config)
            insert_load_log_start(cursor, config, load_id)
            log_started = True

            if full_refresh:
                LOGGER.info(
                    "Truncating %s.%s.TRIPS", config.database, config.raw_schema
                )
                cursor.execute(
                    f"truncate table {config.database}.{config.raw_schema}.TRIPS"
                )

            df = read_and_validate_csv(csv_path)
            df.columns = [column.upper() for column in df.columns]

            success, chunk_count, row_count, _ = write_pandas(
                connection,
                df,
                table_name="TRIPS",
                schema=config.raw_schema,
                database=config.database,
                auto_create_table=False,
                overwrite=False,
                quote_identifiers=False,
            )
            if not success:
                raise RuntimeError(
                    "write_pandas reported failure while loading RAW.TRIPS"
                )

            LOGGER.info("Loaded %s rows across %s chunk(s)", row_count, chunk_count)
            update_load_log_end(cursor, config, load_id, int(row_count), "SUCCESS")

        connection.commit()
        LOGGER.info("Ingestion completed successfully with load_id=%s", load_id)
        return load_id

    except Exception:
        connection.rollback()
        if log_started:
            try:
                with connection.cursor() as cursor:
                    update_load_log_end(cursor, config, load_id, 0, "FAILED")
                connection.commit()
            except Exception:
                LOGGER.exception(
                    "Failed to update LOAD_LOG status after ingestion error"
                )

        LOGGER.exception("Ingestion failed for load_id=%s", load_id)
        raise

    finally:
        connection.close()


def main() -> None:
    configure_logging()
    args = parse_args()

    full_refresh = args.full_refresh or not args.append
    run_ingestion(full_refresh=full_refresh, csv_path=args.csv_path)


if __name__ == "__main__":
    main()
