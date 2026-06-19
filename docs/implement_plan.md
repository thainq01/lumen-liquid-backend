# LumenLiquid Backend — Implementation Plan

Off-chain services for the LumenLiquid perpetuals protocol on Soroban. The
on-chain contracts (`../lumenliquid-contracts`) handle settlement; this backend
handles indexing, real-time fan-out, history, and the keeper bot that triggers
liquidations / TP-SL / limit orders.

## Stack

| Layer            | Choice                                              |
| ---------------- | --------------------------------------------------- |
| Language         | Go 1.23                                             |
| HTTP             | `go-chi/chi`                                        |
| WebSocket        | `nhooyr.io/websocket` + in-process hub              |
| Soroban          | thin JSON-RPC client over `net/http` + `stellar/go/xdr` |
| DB               | PostgreSQL 16 + `sqlc`                              |
| Migrations       | `golang-migrate`                                    |
| Cache / PubSub   | Redis 7 + `go-redis/v9`                             |
| Queue            | `hibiken/asynq`                                     |
| Config / Log     | `spf13/viper` + `zerolog`                           |
| Metrics          | `prometheus/client_golang`                          |

## Services

Three binaries from one Go module, sharing `internal/`:

```
cmd/indexer   poll getEvents → DB projections + Redis pub
cmd/api       REST + WebSocket gateway
cmd/keeper    pricefeed + triggers + tx submitter
```

## Architecture

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
                │PostgreSQL│   │ WS Gateway      │ ── socket ──▶ FE
                │ + history│   │ (rooms per pair │
                └────▲─────┘   │  / per trader)  │
                     │         └─────────────────┘
                     │                 ▲
                     │                 │
              reads  │         ┌───────┴─────────┐
                     ├─────────│  REST API       │ ── HTTP ──▶ FE
                     │         └─────────────────┘
                     │
              reads  │   ┌──────────────────────────┐
                     └───│  Keeper                  │
                         │  - Binance WS feed       │
                         │  - Reflector gate        │ ── tx ──▶ Soroban
                         │  - liq / TP-SL / limit   │
                         └──────────────────────────┘
```

## Trigger model — Option B (Reflector-as-gate)

The contract re-reads Reflector inside `liquidate_trade` / `execute_tp_sl` /
`execute_limit_order`, so a tx submitted before Reflector itself updates will
revert with `NotLiquidatable` / `PriceMismatch`.

The keeper combines two clocks:

1. **External feed (Binance WS, ~10/s)** — fast intent. Binance ticks mark
   trades as *candidates* in Redis sorted sets:
   `candidates:{liq|tp|sl}:{long|short}:{pair_index}`. No tx submitted.
2. **Reflector poller (`simulateTransaction lastprice`, ~1/5s, free read)** —
   the gate. When Reflector's `PriceData.timestamp` advances, the on-chain
   oracle has officially moved. The trigger engine drains the candidate sets
   that agree with the new Reflector price and enqueues the matching contract
   call.

Net: zero monitoring tx, near-zero stale-price reverts.

## Phases

### Phase 0 — Foundations  (~3 days)

Goal: any service boots and talks to Postgres / Redis / Soroban RPC.

- Monorepo: `cmd/{indexer,api,keeper}`, `internal/`, `migrations/`,
  `sql/queries/`
- `docker-compose.yml`: Postgres 16, Redis 7
- `internal/soroban/` — JSON-RPC client (`getEvents`,
  `simulateTransaction`, `sendTransaction`, `getLatestLedger`)
- `internal/soroban/scval.go` — XDR → native decoder (Address, Symbol,
  i128, Vec, Map, structs)
- `internal/db/` — sqlc + golang-migrate wiring
- `internal/log/`, `internal/config/` (zerolog + viper),
  `/healthz`, Prometheus baseline
- Makefile: `make migrate`, `make sqlc`, `make run-*`

Exit: `go run ./cmd/indexer` connects, prints latest ledger, exits clean.

### Phase 1 — Indexer + schema  (~5 days)

Goal: every chain action shows up in Postgres within ~5 seconds.

- Migrations:
  - `trades` — live, only currently-open positions, indexed on
    `(pair_index, liq_price)` for fast keeper range scans.
  - `trade_history` — append-only, one row per closed trade with
    `close_reason ∈ {manual, tp, sl, liquidation}`.
  - `limit_orders`, `limit_order_history`
  - `trade_events` — raw decoded events, never modified
    (`(tx_hash, event_index)` PK), the audit log + replay source.
  - `vault_positions`, `pair_oi`, `indexer_cursor`
- `internal/events/` — typed structs + decoder for every topic:
  - PM: `opened`, `closed`, `placed`, `executed`, `canceled`,
    `updated_limit`, `updated_tp_sl`, `tp_sl_executed`, `liq`
  - Vault: `deposit`, `withdraw`, `settle`, `take_collat`, `bad_debt`
  - Registry: `pair_added`, `pair_updated`, `pair_disabled`,
    `funding_rate`, `group_added`, `group_updated`, `open_fee`, `close_fee`
- Cursor-based polling loop (`getEvents` every 2-3 s, persist `last_ledger`)
- Projection handlers — each event upserts the right table; **at `opened`,
  precompute `liq_price` via the ported math and store on the row**
- Redis publish: `events:pair:{i}`, `events:trader:{addr}`, `events:global`
- Idempotency on `(tx_hash, event_index)`
- `cmd/indexer-replay` — rebuild projections from `trade_events`

Exit: open a trade on testnet → row in `trades` with correct `liq_price`
within 5 s. Close it → row moves to `trade_history` with the right
`close_reason`.

### Phase 2 — Keeper  (~5 days, protocol-critical)

Goal: TP/SL, liquidation, limit orders fire automatically.

- `internal/keeper/math.go` — port of `liq.rs` (`liquidation_price`,
  `is_liquidatable`) using `math/big.Int`; cross-tested vs Rust unit tests.
- `internal/pricefeed/binance.go` — WS client for `btcusdt@trade`, scaled to
  PRICE_SCALE (1e10), publishes `price_external:BTC`.
- `internal/pricefeed/reflector.go` — poll Reflector `lastprice` via
  `simulateTransaction` every 4-5 s, publish `reflector_tick:BTC` only when
  `PriceData.timestamp` changes.
- `internal/triggers/`
  - On Binance tick: SQL range query `trades` for crossed
    `liq_price`/`tp_price`/`sl_price`, ZADD into the matching candidate set.
  - On Reflector tick: ZRANGEBYSCORE candidate sets, enqueue submit jobs.
- `internal/keeper/submit.go` — asynq worker, single keeper account,
  in-process sequence-number mutex, exponential backoff,
  drop-on-`NotLiquidatable`/`PriceMismatch`.
- Three handlers: `liquidate_trade`, `execute_tp_sl`, `execute_limit_order`.

Exit: open a BTC long with tight SL on testnet → keeper waits for Reflector
to confirm → fires `execute_tp_sl` → trade lands in `trade_history` with
`close_reason='sl'`. No reverts on stale-price grounds.

### Phase 3 — API + WebSocket gateway  (~5 days)

Goal: frontend gets realtime + history.

- REST (chi):
  - `GET /pairs`, `GET /pairs/:i`
  - `GET /traders/:addr/trades` (open)
  - `GET /traders/:addr/history?cursor=` (closed, paged by `closed_at`)
  - `GET /traders/:addr/trades/:pair/:idx` (single + live PnL via
    `simulateTransaction`)
  - `GET /vault/stats`
  - `GET /prices`
- WS (`nhooyr.io/websocket` + hub):
  - Channels: `pair:{i}`, `trader:{addr}`, `prices` (throttled to ~5/s)
  - Subscribers tail Redis pub/sub from indexer + price feeds
- Auth: public read on `pair:*` + `prices`. Per-trader sub gated by signed
  challenge (Stellar key signs nonce → short-lived JWT).

Exit: FE opens WS, subscribes to `pair:0` + `prices` + `trader:GA…`, sees a
trade open in real time, paginates closed-trade history via REST.

### Phase 4 — Hardening  (~5 days)

- Idempotency / replay tests (kill indexer mid-stream, restart, verify
  projections converge)
- Drift monitor: alert if `lastIndexedLedger - latestLedger > 100`
- Keeper metrics: `liquidations_fired`, `tx_reverted{reason}`,
  `submit_latency_seconds`, `candidates_pending`
- Rate-limit middleware on REST + WS
- WS connection limits + heartbeat
- Backpressure: drop tick fanout under Redis lag, never trade events
- CI: `go test`, lint, sqlc verify, migration up/down test
- Staging deploy: 3 binaries behind a load balancer, single Redis, single
  Postgres

Exit: 100 open trades / 5 pairs, oracle drops 30 %, every keeper trigger
fires within 2 ledgers of Reflector update, no projection drift,
p99 WS latency < 500 ms.

### Phase 5 — Post-MVP

- Reflector subscription webhooks (Option B Flavor 2) — replaces the poller,
  needs a public HTTPS endpoint and Reflector credits.
- Multi-pair feeds (ETH, SOL, …).
- Multi-keeper sharding (`pair_index % N` per keeper key).
- Admin dashboard: TVL, OI, bad-debt pool, keeper health.
- Pre-aggregated leaderboards (materialized views).
- Liquidation rewards if/when the contract adds them.

## Dependency graph

```
Phase 0 ─► Phase 1 ─┬─► Phase 2 ─┐
                    │            ├─► Phase 4 ─► Phase 5
                    └─► Phase 3 ─┘
```

Phase 2 and Phase 3 are independent once Phase 1 ships — they can run in
parallel.

## Schema (Phase 1)

```sql
trades              (trader, pair_index, trade_index)  PK
                    is_long, leverage, collateral, open_price,
                    tp_price, sl_price, liq_price (precomputed),
                    opened_at, opened_tx
                    INDEX (pair_index, liq_price) WHERE is_long
                    INDEX (pair_index, liq_price) WHERE NOT is_long

trade_history       id bigserial PK
                    trader, pair_index, trade_index,
                    is_long, leverage, collateral,
                    open_price, close_price, tp_price, sl_price,
                    realized_pnl, open_fee, close_fee,
                    close_reason ('manual'|'tp'|'sl'|'liquidation'),
                    opened_at, opened_tx, closed_at, closed_tx
                    INDEX (trader, closed_at DESC)

limit_orders        (trader, pair_index, limit_index)  PK
limit_order_history append-only

trade_events        (tx_hash, event_index) PK
                    ledger, contract_id, topic, trader, pair_index,
                    trade_index, data jsonb, occurred_at
                    INDEX (trader, occurred_at DESC)

vault_positions     (depositor) PK   shares, deposited
pair_oi             (pair_index) PK  long_oi, short_oi
indexer_cursor      single row, last_ledger
```

## Event → projection mapping

| Topic              | trades   | trade_history             | trade_events |
| ------------------ | -------- | ------------------------- | ------------ |
| opened             | INSERT   | —                         | INSERT       |
| updated_tp_sl      | UPDATE   | —                         | INSERT       |
| closed             | DELETE   | INSERT, reason='manual'   | INSERT       |
| tp_sl_executed     | DELETE   | INSERT, reason='tp'/'sl'  | INSERT       |
| liq                | DELETE   | INSERT, reason='liquidation' | INSERT    |
| placed             | —        | (limit_orders INSERT)     | INSERT       |
| executed           | INSERT   | (limit_orders DELETE)     | INSERT       |
| canceled           | —        | (limit_orders DELETE)     | INSERT       |
| updated_limit      | —        | (limit_orders UPDATE)     | INSERT       |
| Vault deposit/withdraw/settle | —    | —              | INSERT       |
| Registry events    | —        | —                         | INSERT       |

## Open decisions to lock before Phase 2

1. Binance vs other external feed — Binance recommended, lowest divergence
   from Reflector CEX oracle.
2. SQL range query vs Redis sorted-set watermarks — start SQL, switch to
   sorted-set when a pair gets hot.
3. WS auth model for per-trader subscriptions — signed-nonce recommended.
4. Single keeper key vs sharded — single + mutex for MVP.
