# LumenLiquid Backend

Off-chain services for the LumenLiquid perpetuals protocol on Soroban.

See `docs/implement_plan.md` for the full plan.

## Quick start

```bash
cp .env.example .env           # edit contract IDs
make up                        # postgres + redis
make migrate-up                # run schema migrations
make run-indexer               # start polling Soroban events
```

## Layout

```
cmd/
  indexer/          poll Soroban events → DB + Redis
  indexer-replay/   rebuild projections from raw trade_events
internal/
  config/           viper-based config loader
  log/              zerolog wrapper
  soroban/          JSON-RPC client + scval decoder
  events/           typed event structs + decoder
  db/               pgxpool wrapper
  store/            repositories (typed CRUD on top of db/)
  pubsub/           redis publisher
  keeper/           ported math (liq_price, fees) for indexer precompute
migrations/         golang-migrate SQL files
sql/queries/        sqlc query files (Phase 2+)
```

## Status

- [x] Phase 0 — foundations
- [x] Phase 1 — indexer + schema
- [ ] Phase 2 — keeper
- [ ] Phase 3 — API + WebSocket
- [ ] Phase 4 — hardening
