PYTHON ?= python3.11
VENV ?= .venv
BIN := $(VENV)/bin
PIP := $(BIN)/pip
PY := $(BIN)/python
DBT := $(BIN)/dbt
DAGSTER := $(BIN)/dagster
RUFF := $(BIN)/ruff
SQLFLUFF := $(BIN)/sqlfluff

.PHONY: venv install generate-data ingest dbt-deps dbt-build native-dbt-deploy native-dbt-execute dagster-cloud-env-sync dagster-dev lint test

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then \
		$(BIN)/pre-commit install; \
	else \
		echo "Skipping pre-commit install (not a git repository)."; \
	fi

generate-data:
	$(PY) scripts/generate_sample_data.py --rows 10000 --output data/sample_trips.csv

ingest:
	@if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	$(PY) -m pipeline.ingest --full-refresh

dbt-deps:
	$(DBT) deps --project-dir dbt --profiles-dir dbt

dbt-build: dbt-deps
	@if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	$(DBT) build --project-dir dbt --profiles-dir dbt --target dev

native-dbt-deploy:
	@if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	$(PY) -m pipeline.native_dbt deploy

native-dbt-execute:
	@if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	$(PY) -m pipeline.native_dbt execute

dagster-cloud-env-sync:
	@if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	$(PY) scripts/set_dagster_cloud_env_vars.py --dotenv-path .env

dagster-dev:
	@if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	$(DAGSTER) dev -f orchestration/definitions.py

lint: dbt-deps
	$(RUFF) check .
	$(RUFF) format --check .
	$(SQLFLUFF) lint dbt/models --config sqlfluff/.sqlfluff

test: dbt-deps
	@if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	$(DBT) test --project-dir dbt --profiles-dir dbt --target dev
