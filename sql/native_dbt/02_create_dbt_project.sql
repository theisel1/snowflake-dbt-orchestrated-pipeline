-- Create a native Snowflake DBT PROJECT object from the Git repository.
-- The dbt project root in this repo is /dbt.

create or replace dbt project PORTFOLIO_DB.MARTS.TRIPS_DBT_PROJECT
  from '@PORTFOLIO_DB.RAW.PORTFOLIO_GIT_REPO/branches/main/dbt'
  default_target = 'prod';

-- Optional if your dbt packages/macros need outbound network access:
-- create or replace dbt project PORTFOLIO_DB.MARTS.TRIPS_DBT_PROJECT
--   from '@PORTFOLIO_DB.RAW.PORTFOLIO_GIT_REPO/branches/main/dbt'
--   default_target = 'prod'
--   external_access_integrations = (DBT_EXTERNAL_ACCESS_INT);
