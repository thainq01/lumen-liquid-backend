package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"github.com/lumenliquid/backend/internal/config"
	"github.com/lumenliquid/backend/internal/db"
	"github.com/lumenliquid/backend/internal/events"
	"github.com/lumenliquid/backend/internal/log"
	"github.com/lumenliquid/backend/internal/pubsub"
	"github.com/lumenliquid/backend/internal/soroban"
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

	rdb, err := pubsub.OpenRedis(ctx, cfg.RedisURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("redis open")
	}
	defer rdb.Close()
	pub := pubsub.NewPublisher(rdb)

	rpc := soroban.New(cfg.SorobanRPCURL)
	repo := store.New(pool)

	contracts := collectContractIDs(cfg)
	if len(contracts) == 0 {
		logger.Fatal().Msg("at least one of PM/Vault/Registry contract id must be set")
	}
	logger.Info().Strs("contracts", contracts).
		Str("rpc", cfg.SorobanRPCURL).
		Dur("poll", cfg.IndexerPollInterval).
		Msg("indexer starting")

	startLedger, cursor, err := decideStartCursor(ctx, repo, rpc, cfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("decide start cursor")
	}
	logger.Info().Uint64("start_ledger", uint64(startLedger)).Str("cursor", cursor).Msg("starting cursor")

	tick := time.NewTicker(cfg.IndexerPollInterval)
	defer tick.Stop()

	for {
		if err := pollOnce(ctx, rpc, repo, pub, logger, contracts, &startLedger, &cursor, cfg); err != nil {
			if errors.Is(err, context.Canceled) {
				logger.Info().Msg("shutdown")
				return
			}
			// Check for rate-limit or RPC errors
			var httpErr *soroban.HTTPError
			if errors.As(err, &httpErr) {
				if httpErr.IsRateLimit() {
					logger.Warn().
						Int("status", httpErr.StatusCode).
						Str("body", httpErr.Body).
						Msg("RPC rate limited - consider increasing poll interval")
				} else {
					logger.Error().
						Int("status", httpErr.StatusCode).
						Str("body", httpErr.Body).
						Msg("RPC HTTP error")
				}
			} else {
				logger.Error().Err(err).Msg("poll iteration failed")
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func collectContractIDs(cfg *config.Config) []string {
	var ids []string
	for _, id := range []string{cfg.PMContractID, cfg.VaultContractID, cfg.RegistryContractID} {
		id = strings.TrimSpace(id)
		if id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

func decideStartCursor(ctx context.Context, repo *store.Repo, rpc *soroban.Client, cfg *config.Config) (uint32, string, error) {
	last, cursor, err := repo.ReadCursor(ctx)
	if err != nil {
		return 0, "", err
	}
	if last > 0 || cursor != "" {
		return uint32(last), cursor, nil
	}
	if cfg.IndexerStartLedger > 0 {
		return cfg.IndexerStartLedger, "", nil
	}
	latest, err := rpc.GetLatestLedger(ctx)
	if err != nil {
		return 0, "", fmt.Errorf("get latest ledger: %w", err)
	}
	return latest.Sequence, "", nil
}

func pollOnce(
	ctx context.Context,
	rpc *soroban.Client,
	repo *store.Repo,
	pub *pubsub.Publisher,
	logger zerolog.Logger,
	contracts []string,
	startLedger *uint32,
	cursor *string,
	cfg *config.Config,
) error {
	params := soroban.GetEventsParams{
		Filters: []soroban.EventFilter{{
			Type:        soroban.EventFilterContract,
			ContractIDs: contracts,
		}},
		Pagination: &soroban.Pagination{Limit: 200},
	}
	if *cursor != "" {
		params.Pagination.Cursor = *cursor
	} else {
		params.StartLedger = *startLedger
	}

	resp, err := rpc.GetEvents(ctx, params)
	if err != nil {
		return err
	}
	if len(resp.Events) == 0 {
		return nil
	}

	logger.Debug().Int("n", len(resp.Events)).Msg("got events")

	for i, ev := range resp.Events {
		decoded, err := events.Decode(ev, cfg.PMContractID, cfg.VaultContractID, cfg.RegistryContractID)
		if err != nil {
			logger.Warn().Err(err).Str("tx", ev.TxHash).Msg("decode event")
			continue
		}
		if err := repo.Apply(ctx, decoded, uint32(i)); err != nil {
			logger.Error().Err(err).Str("topic", decoded.Topic).Str("tx", decoded.TxHash).Msg("apply event")
			continue
		}
		logEvent(logger, decoded)
		fanout(ctx, pub, decoded, logger)
		if uint32(decoded.Ledger) > *startLedger {
			*startLedger = uint32(decoded.Ledger)
		}
	}

	if resp.Cursor != "" {
		*cursor = resp.Cursor
	}
	if err := repo.WriteCursor(ctx, uint64(*startLedger), *cursor); err != nil {
		return fmt.Errorf("write cursor: %w", err)
	}
	return nil
}

func logEvent(logger zerolog.Logger, e events.Event) {
	// Calculate event age (RPC lag)
	var lagMs int64
	if t, err := time.Parse(time.RFC3339, e.OccurredAt); err == nil {
		lagMs = time.Since(t).Milliseconds()
	}

	switch e.Topic {
	case "opened":
		logger.Info().
			Str("tx", e.TxHash).
			Str("trader", e.Trader).
			Int("pair", int(e.Trade.PairIndex)).
			Int("trade_idx", int(*e.TradeIndex)).
			Bool("long", e.Trade.IsLong).
			Int("lev", int(e.Trade.Leverage)).
			Str("collateral", e.Trade.Collateral.String()).
			Int64("lag_ms", lagMs).
			Msg("trade opened")
	case "closed":
		logger.Info().
			Str("tx", e.TxHash).
			Str("trader", e.Trader).
			Int("trade_idx", int(*e.TradeIndex)).
			Str("reason", "manual").
			Int64("lag_ms", lagMs).
			Msg("trade closed")
	case "tp_sl_executed":
		logger.Info().
			Str("tx", e.TxHash).
			Str("trader", e.Trader).
			Int("trade_idx", int(*e.TradeIndex)).
			Str("reason", "tp/sl").
			Int64("lag_ms", lagMs).
			Msg("trade closed")
	case "liq":
		logger.Info().
			Str("tx", e.TxHash).
			Str("trader", e.Trader).
			Int("trade_idx", int(*e.TradeIndex)).
			Str("reason", "liquidation").
			Int64("lag_ms", lagMs).
			Msg("trade closed")
	}
}

func fanout(ctx context.Context, pub *pubsub.Publisher, e events.Event, logger zerolog.Logger) {
	channels := []string{"events:global"}
	if e.Trader != "" {
		channels = append(channels, "events:trader:"+e.Trader)
	}
	if e.PairIndex != nil {
		channels = append(channels, fmt.Sprintf("events:pair:%d", *e.PairIndex))
	}
	for _, ch := range channels {
		if err := pub.Publish(ctx, ch, e); err != nil {
			logger.Warn().Err(err).Str("ch", ch).Msg("redis publish")
		}
	}
}
