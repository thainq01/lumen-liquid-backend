# LumenLiquid Backend

High-performance off-chain indexing, real-time WebSocket streaming, and automated keeper services for the **LumenLiquid** decentralized perpetuals protocol on Stellar (Soroban).

---

## Overview

The LumenLiquid backend bridges on-chain Soroban smart contracts and client frontends by providing:

1. **Event Indexing:** Ingests and decodes Soroban contract events (`getEvents`), projects them into PostgreSQL, and fans out updates via Redis Pub/Sub.
2. **Real-time Gateway:** Delivers sub-second trade updates, position states, and price feeds to frontends via WebSocket and REST APIs.
3. **Keeper Automation:** Evaluates and triggers liquidations, take-profit (TP), stop-loss (SL), and limit orders using Reflector Oracle price updates.

See [docs/implement_plan.md](docs/implement_plan.md) for detailed architecture and design specifications.

---

## Services & Architecture

```
                      Soroban Testnet RPC
                               │
                   ┌───────────┴───────────┐
                   │   Indexer (poller)    │  decodes opened/closed/placed/
                   │   - decodes XDR       │  executed/liq/tp_sl_executed/
                   │   - upserts to DB     │  deposit/withdraw/...
                   │   - publishes Redis   │
                   └───┬─────────────┬─────┘
                       │             │
               writes  │             │ pub/sub
                       ▼             ▼
                 ┌──────────┐   ┌─────────────────┐
                 │PostgreSQL│   │ WS Gateway      │ ── socket ──▶ Frontend
                 │ + history│   │ (rooms per pair │
                 └────▲─────┘   │  / per trader)  │
                      │         └─────────────────┘
                      │                 ▲
                      │                 │
               reads  │         ┌───────┴─────────┐
                      ├─────────│  REST API       │ ── HTTP ────▶ Frontend
                      │         └─────────────────┘
                      │
               reads  │   ┌──────────────────────────┐
                      └───│  Keeper                  │
                          │  - Binance WS feed       │
                          │  - Reflector gate        │ ── tx ──────▶ Soroban
                          │  - liq / TP-SL / limit   │
                          └──────────────────────────┘
```

### Binary Components (`cmd/`)

| Binary | Description |
|---|---|
| `cmd/api` | REST API (`go-chi/chi`) and WebSocket gateway (`nhooyr.io/websocket`) |
| `cmd/indexer` | Polling Soroban `getEvents`, decoding typed events, and projecting to DB & Redis |
| `cmd/pair-indexer` | Synchronizes trading pair metadata and fee group parameters from Pair Registry |
| `cmd/keeper` | Candidate detection + Reflector-gated execution of liquidations, TP/SL, and limit orders |
| `cmd/indexer-replay` | Deterministic replay and rebuild of projections from raw stored events |
| `cmd/migrate` | Standalone runner for database migrations |

---

## Project Structure

```
cmd/
  api/              REST and WebSocket server
  indexer/          Soroban event poller → DB & Redis
  pair-indexer/     Pair & group configuration synchronizer
  keeper/           Automated liquidation, TP/SL, limit order executor
  indexer-replay/   Event replay utility
internal/
  config/           Viper-based configuration loader
  db/               PostgreSQL connection pooling (`pgxpool`)
  events/           Typed Soroban event definitions and XDR decoder
  keeper/           Math, detection logic, Binance client, and transaction builder
  log/              Structured logging (`zerolog`)
  pubsub/           Redis pub/sub publisher and subscriber hub
  soroban/          JSON-RPC client, transaction builder, and SCVal decoders
  store/            Repository and database queries layer
  wsgateway/        WebSocket client connection hub and multiplexing
migrations/         SQL migration scripts (`golang-migrate`)
sql/queries/        SQL query templates for `sqlc`
docs/               Technical documentation and API guides
```

---

## Tech Stack

| Layer | Component |
|---|---|
| **Language** | Go 1.23 |
| **HTTP Framework** | `go-chi/chi` |
| **WebSocket** | `nhooyr.io/websocket` |
| **Database** | PostgreSQL 16 (`jackc/pgx/v5`) |
| **Cache & PubSub** | Redis 7 (`redis/go-redis/v9`) |
| **Blockchain Client** | Custom JSON-RPC + `stellar/go/xdr` |
| **Configuration** | `spf13/viper` |
| **Logging** | `rs/zerolog` |

---

## Quick Start

### 1. Prerequisites
- [Go 1.23+](https://go.dev/dl/)
- [Docker](https://www.docker.com/) & Docker Compose
- `make`

### 2. Configure Environment
```bash
cp .env.example .env
# Fill in your database credentials, Redis URL, and Soroban contract IDs
```

### 3. Start Infrastructure
```bash
make up             # Starts PostgreSQL 16 and Redis 7 in background
make migrate-up     # Applies database schema migrations
```

### 4. Run Services
```bash
# Start the event indexer
make run-indexer

# Start the keeper service (in a separate terminal)
make run-keeper

# Or run the API server
go run ./cmd/api
```

---

## Documentation

- **[Implementation Plan & Architecture](docs/implement_plan.md)**: Deep dive into the backend design, Reflector-as-gate model, and roadmap.
- **[REST & WebSocket API Guide](docs/api-ws-instruct.md)**: Endpoints, WebSocket payloads, subscription topics, and data formats.
- **[Trading Pair & Group Config API](docs/pair-group-api-curl.md)**: REST endpoints for reading configured pairs, leverage, fees, and groups.
- **[Blockchain Error Codes](docs/blockchain-trading-error-codes.md)**: Soroban contract simulation and submission error mappings.

---

## License

Proprietary — All rights reserved.
