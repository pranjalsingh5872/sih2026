# SIH26069 — National Weather Big Data Analytics Platform
# Developer entrypoints. `make help` lists everything.

SHELL := /bin/bash
COMPOSE := docker compose
API := http://localhost:8000
PREFIX := /api/v1
INGEST_KEY ?= dev-citizen-key
ADMIN_KEY ?= dev-admin-key

.DEFAULT_GOAL := help
.PHONY: help env up up-dev up-analytics down clean logs logs-api logs-normalizer \
        ps topics health ready info docs report report-nogeo report-photo \
        stream stream-raw unresolved deadletter test test-local lint shell psql \
        redis-cli rebuild

## ---------------------------------------------------------------- lifecycle --

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from the template if it does not exist
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example")

up: env ## Start the full pipeline (Kafka, Postgres, Redis, API, 4 workers)
	$(COMPOSE) up -d --build
	@echo "API      $(API)$(PREFIX)"
	@echo "Docs     $(API)/docs"
	@echo "Run 'make health' once the containers settle (~30s)."

up-dev: env ## Start the pipeline plus Kafka UI on :8080
	$(COMPOSE) --profile dev up -d --build

up-analytics: env ## Start the pipeline plus ClickHouse
	$(COMPOSE) --profile analytics up -d --build

down: ## Stop everything, keep volumes
	$(COMPOSE) down

clean: ## Stop everything and delete volumes (destroys all ingested data)
	$(COMPOSE) down -v

rebuild: ## Rebuild the backend image without cache
	$(COMPOSE) build --no-cache api

ps: ## Show container status
	$(COMPOSE) ps

## ------------------------------------------------------------- observation --

logs: ## Tail all logs
	$(COMPOSE) logs -f --tail=100

logs-api: ## Tail API logs only
	$(COMPOSE) logs -f --tail=100 api

logs-normalizer: ## Tail the normalizer worker (the fan-in stage)
	$(COMPOSE) logs -f --tail=100 worker-normalizer

topics: ## List Kafka topics and their partition counts
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server localhost:29092 --describe

health: ## Liveness probe
	@curl -sS $(API)$(PREFIX)/healthz | python3 -m json.tool

ready: ## Readiness probe with per-dependency status
	@curl -sS $(API)$(PREFIX)/readyz | python3 -m json.tool

info: ## Pipeline configuration (topics, sources, provider modes)
	@curl -sS $(API)$(PREFIX)/info | python3 -m json.tool

docs: ## Open the interactive API docs
	@echo "$(API)/docs"

## -------------------------------------------------------------- ingestion --

report: ## Submit a citizen report with GPS coordinates
	@curl -sS -X POST $(API)$(PREFIX)/incidents/report \
		-H "X-API-Key: $(INGEST_KEY)" \
		-H "Content-Type: application/json" \
		-d '{"description":"Knee deep water near Rajwada, cars are stuck","lat":22.7196,"lon":75.8577,"location_accuracy_m":9,"district":"Indore","state":"Madhya Pradesh"}' \
		| python3 -m json.tool

report-nogeo: ## Submit a report with no coordinates (exercises the fallback chain)
	@curl -sS -X POST $(API)$(PREFIX)/incidents/report \
		-H "X-API-Key: $(INGEST_KEY)" \
		-H "Content-Type: application/json" \
		-d '{"description":"Indore mein sadak par paani bhar gaya hai, madad chahiye"}' \
		| python3 -m json.tool

report-photo: ## Submit a report with a photo (set PHOTO=/path/to.jpg)
	@test -n "$(PHOTO)" || (echo "Usage: make report-photo PHOTO=/path/to/image.jpg"; exit 1)
	@curl -sS -X POST $(API)$(PREFIX)/incidents/report-with-media \
		-H "X-API-Key: $(INGEST_KEY)" \
		-F 'report={"description":"Underpass fully submerged","place_name":"Indore"}' \
		-F "photo=@$(PHOTO)" \
		| python3 -m json.tool

## ----------------------------------------------------------------- streams --

stream: ## Tail the unified output stream (this is the Phase 1 deliverable)
	$(COMPOSE) exec kafka kafka-console-consumer \
		--bootstrap-server localhost:29092 \
		--topic normalized-incident-stream --from-beginning --max-messages 20

stream-raw: ## Tail the raw ingestion stream
	$(COMPOSE) exec kafka kafka-console-consumer \
		--bootstrap-server localhost:29092 \
		--topic raw-weather-stream --from-beginning --max-messages 20

unresolved: ## Tail reports that could not be located (manual triage queue)
	$(COMPOSE) exec kafka kafka-console-consumer \
		--bootstrap-server localhost:29092 \
		--topic geo-unresolved-incidents --from-beginning --max-messages 20

deadletter: ## Tail messages the pipeline refused
	$(COMPOSE) exec kafka kafka-console-consumer \
		--bootstrap-server localhost:29092 \
		--topic incident-dead-letter --from-beginning --max-messages 20

## ------------------------------------------------------------------- dev ----

test: ## Run the test suite inside the container
	$(COMPOSE) run --rm --no-deps api pytest

test-local: ## Run the test suite on the host (needs backend/requirements.txt installed)
	cd backend && python -m pytest

shell: ## Shell into the API container
	$(COMPOSE) exec api /bin/bash

psql: ## Open psql against the Postgres container
	$(COMPOSE) exec postgres psql -U weather -d weatherdb

redis-cli: ## Open redis-cli against the Redis container
	$(COMPOSE) exec redis redis-cli
