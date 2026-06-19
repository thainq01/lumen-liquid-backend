# ── env loader ────────────────────────────────────────────
ifneq (,$(wildcard .env))
include .env
export
endif

DATABASE_URL ?= postgres://lumen:lumen@localhost:5432/lumenliquid?sslmode=disable
MIGRATE := docker run --rm -v $(PWD)/migrations:/migrations --network host migrate/migrate \
		   -path=/migrations -database "$(DATABASE_URL)"

INFRA := docker compose -f docker-compose.infra.yml
PROD  := docker compose -f docker-compose.prod.yml

.PHONY: up down migrate-up migrate-down migrate-new tidy fmt vet test \
        run-indexer run-indexer-replay run-keeper psql redis-cli \
        prod-up prod-down prod-pull prod-logs

up:
	$(INFRA) up -d

down:
	$(INFRA) down

# ── migrations ────────────────────────────────────────────
migrate-up:
	$(MIGRATE) up

migrate-down:
	$(MIGRATE) down 1

# usage: make migrate-new name=add_something
migrate-new:
	@test -n "$(name)" || (echo "name= is required" && exit 1)
	$(MIGRATE) create -ext sql -dir /migrations -seq $(name)

# ── Go ────────────────────────────────────────────────────
tidy:
	go mod tidy

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go test ./...

# ── run ───────────────────────────────────────────────────
run-indexer:
	go run ./cmd/indexer

run-indexer-replay:
	go run ./cmd/indexer-replay

run-keeper:
	go run ./cmd/keeper

# ── shells ────────────────────────────────────────────────
psql:
	$(INFRA) exec postgres psql -U lumen -d lumenliquid

redis-cli:
	$(INFRA) exec redis redis-cli

# ── prod ──────────────────────────────────────────────────
prod-up:
	$(PROD) up -d

prod-down:
	$(PROD) down

prod-pull:
	$(PROD) pull

prod-logs:
	$(PROD) logs -f

prod-ps:
	$(PROD) ps
