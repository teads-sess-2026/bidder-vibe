.DEFAULT_GOAL := help
-include config.env
export
MVN          := ./mvnw
PORT         ?= 8080
BIDDER_ID    ?= teads-bidder
DB_HOST      ?= localhost
REDIS_HOST   ?= $(DB_HOST)
CREATIVE_BUDGET ?= 25.0
SCHEMA       := bidder_$(shell echo $(BIDDER_ID) | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/_/g')

.PHONY: help install build run run-team run-docker restart-bidder down test clean

help:
	@echo ""
	@echo "  Bidder — Summer School 2026"
	@echo ""
	@echo "  Infrastructure (Postgres/Kafka/Redis/Prometheus/Grafana) lives in the ssp"
	@echo "  repo now — run 'make run' there first. This repo only runs the bidder app,"
	@echo "  joining that stack's 'ss2026-net' Docker network."
	@echo ""
	@echo "  Bidder"
	@echo "    make install           Download all Maven dependencies"
	@echo "    make build             Compile and package (skip tests)"
	@echo "    make run               Build & run the bidder container on port 8080 (schema bidder_teads_bidder)"
	@echo "    make run-team          Custom port/id:  make run-team PORT=8081 BIDDER_ID=team-alpha"
	@echo "    make run-docker        Alias for 'make run'"
	@echo "    make restart-bidder    Reset this bidder's budget in Postgres+Redis and restart the bidder container"
	@echo "    make down              Stop the bidder container"
	@echo "    make test              Run unit tests"
	@echo "    make clean             Remove build artifacts"
	@echo ""

# ── Install / Build ──────────────────────────────────────────────────────────

install:
	$(MVN) dependency:resolve -q
	@echo "✓ Dependencies resolved"

build:
	$(MVN) clean package -DskipTests -q
	@echo "✓ Build complete"

# ── Run ───────────────────────────────────────────────────────────────────────

# Runs the "bidder" service from docker-compose.yml, joining the external
# "ss2026-net" network started by the ssp repo's `make run` (docker-compose.all.yml)
# — that must already be running, since Postgres/Kafka/Redis live there now.
# PORT/BIDDER_ID are exported by the `export` directive above, so docker compose
# picks them up automatically.
run: run-docker

run-team:
	docker compose up -d --build bidder
	@echo "✓ Bidder running in Docker on port $(PORT) (schema $(SCHEMA))"
	@docker compose logs -f bidder

run-docker:
	docker compose up -d --build bidder
	@echo "✓ Bidder running in Docker on port $${PORT:-8080}"
	@docker compose logs -f bidder

down:
	docker compose down

# Resets this bidder's remaining budget back to CREATIVE_BUDGET in Postgres, then restarts
# the bidder container. On boot, CreativeSeeder re-syncs Redis from the (now-reset) Postgres
# value, so both stores come back full. Requires the bidder to be running via `make run-docker`.
# Talks to the ss2026-postgres container directly (it's owned by the ssp repo's compose file,
# not this one).
restart-bidder:
	@echo "Resetting budget for bidder '$(BIDDER_ID)' (schema $(SCHEMA)) to $(CREATIVE_BUDGET)"
	docker exec -i ss2026-postgres psql -U bidder -d summerschool -c \
	  "UPDATE $(SCHEMA).creatives SET budget = $(CREATIVE_BUDGET) WHERE bidder_id = '$(BIDDER_ID)';"
	docker compose restart bidder
	@echo "✓ Bidder restarted — budget reset to $(CREATIVE_BUDGET) in Postgres and Redis"

# ── Test ──────────────────────────────────────────────────────────────────────

test:
	$(MVN) test

# ── Clean ─────────────────────────────────────────────────────────────────────

clean:
	$(MVN) clean -q
