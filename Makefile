.DEFAULT_GOAL := help
-include config.env
export
MVN          := ./mvnw
PORT         ?= 8080
BIDDER_ID    ?= teads-bidder
DB_HOST      ?= localhost
REDIS_HOST   ?= $(DB_HOST)

.PHONY: help infra-up infra-down infra-reset infra-logs infra-tailscale install build run run-team down test clean

help:
	@echo ""
	@echo "  Bidder — Summer School 2026"
	@echo ""
	@echo "  Infrastructure"
	@echo "    make infra-up          Start Postgres + Kafka + Kafka UI"
	@echo "    make infra-down        Stop infrastructure"
	@echo "    make infra-reset       Wipe all data and restart infrastructure"
	@echo "    make infra-logs        Tail infrastructure logs"
	@echo ""
	@echo "  Bidder"
	@echo "    make install           Download all Maven dependencies"
	@echo "    make build             Compile and package (skip tests)"
	@echo "    make run               Start bidder on port 8080 (Postgres, schema bidder_teads_bidder)"
	@echo "    make run-team          Custom port/id:  make run-team PORT=8081 BIDDER_ID=team-alpha [DB_HOST=100.x.x.x]"
	@echo "    make test              Run unit tests"
	@echo "    make clean             Remove build artifacts"
	@echo ""

# ── Infrastructure ────────────────────────────────────────────────────────────

infra-up:
	docker compose up -d
	@docker compose ps

infra-down:
	docker compose down -v --remove-orphans
	-docker rm -f ss2026-postgres ss2026-kafka ss2026-kafka-ui ss2026-redis ss2026-prometheus ss2026-grafana 2>/dev/null
	-lsof -ti:8080 | xargs kill -9 2>/dev/null || true

infra-reset:
	docker compose down -v
	docker compose up -d

infra-logs:
	docker compose logs -f

down: infra-down

infra-tailscale:
	@if [ -z "$(TAILSCALE_IP)" ]; then echo "Usage: make infra-tailscale TAILSCALE_IP=100.x.x.x"; exit 1; fi
	TAILSCALE_IP=$(TAILSCALE_IP) docker compose up -d --force-recreate kafka
	@echo "Kafka now advertising $(TAILSCALE_IP):9093"
	@echo "Remote bidders: KAFKA_BOOTSTRAP_SERVERS=$(TAILSCALE_IP):9093"

# ── Install / Build ──────────────────────────────────────────────────────────

install:
	$(MVN) dependency:resolve -q
	@echo "✓ Dependencies resolved"

build:
	$(MVN) clean package -DskipTests -q
	@echo "✓ Build complete"

# ── Run ───────────────────────────────────────────────────────────────────────

run:
	$(MVN) spring-boot:run \
	  -Dspring-boot.run.jvmArguments="-DDB_HOST=$(DB_HOST) -DREDIS_HOST=$(REDIS_HOST) -Dspring.kafka.bootstrap-servers=$(KAFKA_BOOTSTRAP_SERVERS)"

run-team:
	$(MVN) spring-boot:run \
	  -Dspring-boot.run.jvmArguments="-Dserver.port=$(PORT) -Dbidder.id=$(BIDDER_ID) -DDB_HOST=$(DB_HOST) -DREDIS_HOST=$(REDIS_HOST) -Dspring.kafka.bootstrap-servers=$(KAFKA_BOOTSTRAP_SERVERS)"

# ── Test ──────────────────────────────────────────────────────────────────────

test:
	$(MVN) test

# ── Clean ─────────────────────────────────────────────────────────────────────

clean:
	$(MVN) clean -q
