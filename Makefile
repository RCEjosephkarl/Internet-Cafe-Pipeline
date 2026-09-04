# AIMternet-Cafe POC — see CLAUDE.md for the invariants a future session must not break.
SHELL       := /bin/bash
CONDA_ENV   ?= data_eng
CONDA_PREFIX_DIR ?= $(HOME)/.conda/envs/$(CONDA_ENV)
PY          := $(CONDA_PREFIX_DIR)/bin/python
PIP         := $(CONDA_PREFIX_DIR)/bin/pip
REPO_ROOT   := $(shell pwd)

# Logical paths from pipeline_plan_AWS.md §2. `make link` materialises them.
OPT_AIMTERNET ?= /opt/aimternet
OPT_AIRFLOW   ?= /opt/airflow
AIRFLOW_HOME  ?= $(HOME)/airflow

export PYTHONPATH := $(REPO_ROOT)/src

.PHONY: help env link test lint typecheck check migrate assumptions docs erd infra-plan validate \
        quarantine-demo manifest bronze \
        bootstrap load-rds load-dynamodb curate redshift \
        reconcile api metrics airflow streamlit clean

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

env: ## Install/refresh the conda environment and the editable package
	conda env update -n $(CONDA_ENV) -f environment.yml --prune
	$(PIP) install -e . --no-deps

link: ## Create the §2 logical paths (/opt/aimternet, /opt/airflow). Needs sudo.
	sudo mkdir -p $(OPT_AIMTERNET)/data $(OPT_AIMTERNET)/work $(OPT_AIMTERNET)/quarantine
	sudo chown -R $(USER):$(USER) $(OPT_AIMTERNET)
	@[ -e $(OPT_AIMTERNET)/data/raw-landing ] || ln -s $(REPO_ROOT)/data/raw-landing $(OPT_AIMTERNET)/data/raw-landing
	@[ -e $(OPT_AIRFLOW) ] || sudo ln -s $(AIRFLOW_HOME) $(OPT_AIRFLOW)
	mkdir -p $(AIRFLOW_HOME)/dags $(AIRFLOW_HOME)/input
	@[ -e $(AIRFLOW_HOME)/input/raw-landing ] || ln -s $(REPO_ROOT)/data/raw-landing $(AIRFLOW_HOME)/input/raw-landing
	@for f in $(REPO_ROOT)/dags/*.py; do \
	   [ -e "$$f" ] || continue; \
	   [ -e $(AIRFLOW_HOME)/dags/$$(basename $$f) ] || ln -s $$f $(AIRFLOW_HOME)/dags/$$(basename $$f); \
	 done
	@echo "linked: $(OPT_AIMTERNET)/data/raw-landing, $(OPT_AIRFLOW), $(AIRFLOW_HOME)/{dags,input}"

test: ## Run the test suite (AWS/RDS/Redshift-marked tests excluded)
	$(PY) -m pytest -m "not aws and not rds and not redshift"

test-all: ## Run every test, including the ones that need live AWS resources
	$(PY) -m pytest

lint: ## ruff
	$(PY) -m ruff check src tests dags streamlit_app

format: ## ruff --fix + format
	$(PY) -m ruff check --fix src tests dags streamlit_app
	$(PY) -m ruff format src tests dags

typecheck: ## mypy on src/aimternet
	$(PY) -m mypy

check: lint typecheck test ## lint + typecheck + test

migrate: ## Apply database migrations
	$(PY) -m aimternet.pipeline.cli migrate up

assumptions: ## Show every POC policy decision and its evidence
	$(PY) -m aimternet.pipeline.cli assumptions

docs: ## Regenerate the docs rendered from code (docs/assumptions.md)
	$(PY) -m aimternet.pipeline.cli assumptions --markdown > docs/assumptions.md
	@echo "wrote docs/assumptions.md"

erd: ## Regenerate docs/aimternet_erd.drawio from the DDL
	$(PY) scripts/gen_erd_drawio.py

infra-plan: ## terraform plan for infra/ — read-only; apply needs approval, destroy never
	cd infra && terraform init -input=false && terraform plan -input=false

validate: ## Stage C only: validate all 62 batches locally, no AWS needed
	$(PY) -m aimternet.pipeline.cli validate

quarantine-demo: ## Prove the reject path: inject defects into a temp copy and validate it
	$(PY) scripts/quarantine_demo.py

manifest: ## Stage A: inventory and checksum every source file
	$(PY) -m aimternet.pipeline.cli manifest --register

bronze: ## Stages A+B: inventory then copy the landing tree into S3 Bronze
	$(PY) -m aimternet.pipeline.cli bronze --verify

bootstrap: ## Full bootstrap: manifest -> Bronze -> validate -> RDS -> DynamoDB
	$(PY) -m aimternet.pipeline.cli bootstrap

load-rds: ## Load the validated source data into RDS PostgreSQL
	$(PY) -m aimternet.pipeline.cli load-rds

load-dynamodb: ## Create DynamoDB tables and load events + telemetry
	$(PY) -m aimternet.pipeline.cli load-dynamodb

curate: ## Silver + RDS export + Gold Parquet
	$(PY) -m aimternet.pipeline.cli curate

redshift: ## Redshift DDL + load Gold (COPY when available, else batched INSERT)
	$(PY) -m aimternet.pipeline.cli load-redshift

reconcile: ## Cross-layer reconciliation report
	$(PY) -m aimternet.pipeline.cli reconcile

api: ## Operational API on :8000 (the only thing streamlit/ talks to)
	$(PY) -m uvicorn aimternet.api.main:app --host 0.0.0.0 --port 8000

metrics: ## Metrics API — same app, mounted at /v1/metrics
	@echo "Metrics live under the same app as 'make api': http://localhost:8000/v1/metrics"

streamlit: ## Streamlit dashboard on :8501 (talks to the metrics API over HTTP only)
	$(PY) -m streamlit run streamlit_app/Home.py --server.port 8501

airflow: ## Airflow standalone (api-server + scheduler) on :8080
	AIRFLOW_HOME=$(AIRFLOW_HOME) $(CONDA_PREFIX_DIR)/bin/airflow standalone

clean: ## Remove caches and scratch output (never touches data/raw-landing)
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	find src tests -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
