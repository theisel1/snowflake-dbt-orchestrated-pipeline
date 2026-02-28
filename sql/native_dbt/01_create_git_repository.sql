-- Create a Snowflake GIT REPOSITORY object that points at your GitHub repo.
-- Replace <github_org_or_user>/<repo_name> if needed.

create or replace git repository PORTFOLIO_DB.RAW.PORTFOLIO_GIT_REPO
  origin = 'https://github.com/<github_org_or_user>/<repo_name>.git'
  api_integration = GITHUB_INTEGRATION;

-- Refresh refs after first creation or after new commits.
alter git repository PORTFOLIO_DB.RAW.PORTFOLIO_GIT_REPO fetch;
