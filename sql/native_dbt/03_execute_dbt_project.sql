-- Run dbt natively in Snowflake.

execute dbt project PORTFOLIO_DB.MARTS.TRIPS_DBT_PROJECT
  args = 'build --target prod';

-- Optional docs generation:
-- execute dbt project PORTFOLIO_DB.MARTS.TRIPS_DBT_PROJECT
--   args = 'docs generate --target prod';
