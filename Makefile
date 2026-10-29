# robot-telemetry-daas — developer / demo entry points.
# Everything is runnable from a laptop; cluster targets shell out to Terraform/Ansible.

SHELL        := /bin/bash
.DEFAULT_GOAL := help

PYTHON       ?= python3
PIP          ?= $(PYTHON) -m pip
DATA_DIR     ?= data
SQLITE       ?= $(DATA_DIR)/rtdaas.sqlite
COMPOSE_DIR  := infra/compose
COMPOSE      := docker compose --project-directory $(COMPOSE_DIR) -f $(COMPOSE_DIR)/docker-compose.yml
IMAGE_PREFIX ?= ghcr.io/$(shell git config --get remote.origin.url 2>/dev/null | sed -E 's#.*[:/]([^/]+)/[^/]+(\.git)?$$#\1#' | tr '[:upper:]' '[:lower:]' | sed 's/^$$/local/')
IMAGE_TAG    ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
SIM_ROBOTS   ?= 2
SIM_SECONDS  ?= 120
JIG_TRIALS   ?= 2

.PHONY: help install test test-spark test-airflow lint format contracts synth local-pipeline quality api docs docs-build \
        images demo demo-down demo-logs tf-local tf-aws ansible clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------- development
install: ## Editable install with dev, API and video extras
	$(PIP) install -e ".[dev,api,video]"

test: ## Pure-Python unit tests (no Spark/Airflow/services needed)
	$(PYTHON) -m pytest -m "not spark and not airflow and not integration"

test-spark: ## Spark-marked tests (needs pip install -e .[spark] and Java 17)
	RTDAAS_SPARK_PACKAGES=0 $(PYTHON) -m pytest -m spark

test-airflow: ## Airflow DAG integrity tests (needs pip install -e .[airflow])
	AIRFLOW__CORE__LOAD_EXAMPLES=false $(PYTHON) -m pytest -m airflow

lint: ## ruff check + format check
	ruff check rtdaas tests && ruff format --check rtdaas tests

format: ## ruff format
	ruff format rtdaas tests && ruff check --fix rtdaas tests

contracts: ## Regenerate Avro from .msg and run the backward-compatibility check against the working tree base
	$(PYTHON) -m rtdaas.contracts.cli gen
	$(PYTHON) -m rtdaas.contracts.cli check --base contracts/avro --head contracts/avro --mode BACKWARD
	bash ros2/check-msgs-in-sync.sh

# ---------------------------------------------------------------- local (no cluster) demo
synth: ## Synthetic JIGSAWS dataset + simulated robot bags under $(DATA_DIR)
	mkdir -p $(DATA_DIR)
	$(PYTHON) -m rtdaas.jigsaws.cli synth --out $(DATA_DIR)/jigsaws --trials $(JIG_TRIALS)
	$(PYTHON) -m rtdaas.sim.cli run --robots $(SIM_ROBOTS) --duration $(SIM_SECONDS) --rate 50 --mcap-out $(DATA_DIR)/bags --no-realtime

local-pipeline: ## Serial pure-Python pipeline: bags + JIGSAWS -> sessions/KPIs/gestures/DQ -> SQLite mirror
	$(PYTHON) -m rtdaas.local.cli pipeline --bags $(DATA_DIR)/bags --jigsaws $(DATA_DIR)/jigsaws --out $(SQLITE)

quality: ## Run every quality suite against the SQLite mirror
	@for s in quality/checks/*.yml; do \
	  echo "== $$s"; $(PYTHON) -m rtdaas.quality.cli run --suite $$s --backend sqlite --db $(SQLITE) --out-table || true; \
	done

api: ## Serve the API + UI from the SQLite mirror on http://localhost:8080
	SERVING_BACKEND=sqlite SERVING_SQLITE_PATH=$(SQLITE) MEDIA_ROOT=$(DATA_DIR) $(PYTHON) -m rtdaas.serving.cli

docs: ## Serve the docs site locally
	mkdocs serve

docs-build: ## Build the docs site (strict)
	mkdocs build --strict

# ---------------------------------------------------------------- containers / full stack
images: ## Build all five images locally (tagged $(IMAGE_PREFIX)/rtdaas-<name>:$(IMAGE_TAG))
	@for img in api spark airflow sim bridge; do \
	  docker buildx build --load -f infra/docker/$$img.Dockerfile -t $(IMAGE_PREFIX)/rtdaas-$$img:$(IMAGE_TAG) . || exit 1; \
	done

demo: ## Full stack with docker compose (core + demo profiles), init, simulator, first DAG run
	bash scripts/demo.sh up

demo-down: ## Stop the compose stack (keeps volumes; add -v manually to wipe)
	$(COMPOSE) --profile core --profile demo --profile observability down

demo-logs: ## Tail compose logs
	$(COMPOSE) --profile core --profile demo logs -f --tail=100

# ---------------------------------------------------------------- multi-node
tf-local: ## Terraform: 4 libvirt VMs + Ansible inventory (infra/terraform/envs/local)
	cd infra/terraform/envs/local && terraform init -input=false && terraform apply -input=false

tf-aws: ## Terraform: EC2 nodes + S3 buckets + inventory (infra/terraform/envs/aws)
	cd infra/terraform/envs/aws && terraform init -input=false && terraform apply -input=false

ansible: ## Install k3s + Argo CD on the Terraform-provisioned nodes
	cd infra/ansible && ansible-galaxy install -r requirements.yml && ansible-playbook -i inventory/hosts.ini site.yml

clean: ## Remove local data, build artifacts and caches
	rm -rf $(DATA_DIR) site build dist .pytest_cache .ruff_cache .mypy_cache
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
