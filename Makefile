.DEFAULT_GOAL := help
PROFILE ?= full-lite
.PHONY: help doctor setup up down test lint fmt tf-plan kb-seed reindex freshness-demo eval eval-full eval-final gcp-final-test

help:      ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-15s %s\n", $$1, $$2}'
doctor:    ## Check prerequisites
	python scripts/doctor.py
setup:     ## Install Python dependencies (uv workspace)
	uv sync --all-packages --dev
up:        ## Start a profile: make up PROFILE=ingest|serve|full-lite
	docker compose --profile $(PROFILE) up -d
down:      ## Stop everything
	docker compose --profile ingest --profile serve --profile full-lite down
test:      ## Unit tests
	uv run pytest -q
lint:      ## Lint
	uv run ruff check .
fmt:       ## Format
	uv run ruff format .
tf-plan:   ## Terraform plan only (never apply outside the final test)
	cd infra/gcp && terraform init -backend=false && terraform validate
kb-seed reindex freshness-demo eval eval-full eval-final gcp-final-test:   ## Later milestones (PLAN.md §12)
	@echo "'$@' arrives in a later milestone — see PLAN.md §12"; exit 1
