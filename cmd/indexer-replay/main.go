// indexer-replay rebuilds the projection tables (trades, trade_history,
// limit_orders, limit_order_history, vault_positions) by replaying every row
// in `trade_events` in chronological order. Run after a projection-logic fix.
//
// trade_events itself is never modified.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5"

	"github.com/lumenliquid/backend/internal/config"
	"github.com/lumenliquid/backend/internal/db"
	"github.com/lumenliquid/backend/internal/events"
	"github.com/lumenliquid/backend/internal/log"
	"github.com/lumenliquid/backend/internal/store"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	logger := log.Init(cfg.LogLevel)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("db open")
	}
	defer pool.Close()
	repo := store.New(pool)

	logger.Info().Msg("truncating projection tables")
	if _, err := pool.Exec(ctx, `
		TRUNCATE trades, trade_history, limit_orders, limit_order_history, vault_positions, pair_oi`,
	); err != nil {
		logger.Fatal().Err(err).Msg("truncate")
	}

	rows, err := pool.Query(ctx, `
		SELECT tx_hash, event_index, data
		  FROM trade_events
		 ORDER BY ledger ASC, occurred_at ASC, tx_hash ASC, event_index ASC`)
	if err != nil {
		logger.Fatal().Err(err).Msg("scan trade_events")
	}
	defer rows.Close()

	var n int
	for rows.Next() {
		var txHash string
		var idx uint32
		var raw []byte
		if err := rows.Scan(&txHash, &idx, &raw); err != nil {
			logger.Fatal().Err(err).Msg("row scan")
		}
		var e events.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			logger.Warn().Err(err).Str("tx", txHash).Msg("unmarshal event; skipping")
			continue
		}
		if err := repo.Apply(ctx, e, idx); err != nil {
			logger.Error().Err(err).Str("topic", e.Topic).Str("tx", e.TxHash).Msg("replay")
			continue
		}
		n++
	}
	if err := rows.Err(); err != nil && err != pgx.ErrNoRows {
		logger.Error().Err(err).Msg("rows err")
	}
	logger.Info().Int("count", n).Msg("replay complete")
}
