package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/lumenliquid/backend/internal/config"
	"github.com/lumenliquid/backend/internal/db"
	"github.com/lumenliquid/backend/internal/log"
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

	if cfg.RegistryContractID == "" {
		logger.Fatal().Msg("REGISTRY_CONTRACT_ID is required")
	}

	logger.Info().Str("registry", cfg.RegistryContractID).Msg("pair-indexer starting")

	// Fetch pairs_count
	count, err := callContract(ctx, logger, cfg, "pairs_count")
	if err != nil {
		logger.Fatal().Err(err).Msg("call pairs_count")
	}
	var pairsCount uint32
	if err := json.Unmarshal(count, &pairsCount); err != nil {
		logger.Fatal().Err(err).Msg("parse pairs_count")
	}
	logger.Info().Uint32("count", pairsCount).Msg("found pairs")

	// Fetch each pair and its group
	groupsSeen := make(map[uint32]bool)
	for i := uint32(0); i < pairsCount; i++ {
		// Pace invocations to avoid bursting the RPC rate limit.
		select {
		case <-ctx.Done():
			logger.Info().Msg("shutdown")
			return
		case <-time.After(250 * time.Millisecond):
		}

		pairRaw, err := callContract(ctx, logger, cfg, "get_pair", "--pair_index", fmt.Sprint(i))
		if err != nil {
			logger.Error().Err(err).Uint32("pair", i).Msg("get_pair failed")
			continue
		}

		var pair PairInfo
		if err := json.Unmarshal(pairRaw, &pair); err != nil {
			logger.Error().Err(err).Uint32("pair", i).Msg("parse pair")
			continue
		}

		// Fetch depth
		depthRaw, err := callContract(ctx, logger, cfg, "get_depth", "--pair_index", fmt.Sprint(i))
		if err != nil {
			logger.Warn().Err(err).Uint32("pair", i).Msg("get_depth failed, using 0")
		}
		var depth string = "0"
		if err == nil {
			json.Unmarshal(depthRaw, &depth)
		}

		// Upsert pair
		if err := upsertPair(ctx, pool, i, &pair, depth); err != nil {
			logger.Error().Err(err).Uint32("pair", i).Msg("upsert pair")
			continue
		}
		logger.Info().Uint32("pair", i).Str("symbol", pair.Symbol).Msg("synced pair")

		// Fetch group if not seen
		if !groupsSeen[pair.GroupIndex] {
			groupRaw, err := callContract(ctx, logger, cfg, "get_group", "--group_index", fmt.Sprint(pair.GroupIndex))
			if err != nil {
				logger.Error().Err(err).Uint32("group", pair.GroupIndex).Msg("get_group failed")
				continue
			}
			var group Group
			if err := json.Unmarshal(groupRaw, &group); err != nil {
				logger.Error().Err(err).Uint32("group", pair.GroupIndex).Msg("parse group")
				continue
			}
			if err := upsertGroup(ctx, pool, pair.GroupIndex, &group); err != nil {
				logger.Error().Err(err).Uint32("group", pair.GroupIndex).Msg("upsert group")
				continue
			}
			groupsSeen[pair.GroupIndex] = true
			logger.Info().Uint32("group", pair.GroupIndex).Str("name", group.Name).Msg("synced group")
		}
	}

	logger.Info().Int("pairs", int(pairsCount)).Int("groups", len(groupsSeen)).Msg("sync complete")
}

// callContract invokes a read-only contract method via `stellar contract invoke`.
// Requires `stellar` CLI in PATH. Retries with exponential backoff on any CLI
// failure, treating 429/rate-limit/timeout signals as the expected transient case.
func callContract(ctx context.Context, logger zerolog.Logger, cfg *config.Config, method string, args ...string) ([]byte, error) {
	const maxAttempts = 6
	var backoff time.Duration

	cmdArgs := []string{
		"contract", "invoke",
		"--id", cfg.RegistryContractID,
		"--source-account", cfg.StellarSourceAccount,
		"--rpc-url", cfg.SorobanRPCURL,
		"--network-passphrase", cfg.NetworkPassphrase,
		"--",
		method,
	}
	cmdArgs = append(cmdArgs, args...)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var stdout, stderr bytes.Buffer
		cmd := exec.CommandContext(ctx, "stellar", cmdArgs...)
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		err := cmd.Run()
		if err == nil {
			return bytes.TrimSpace(stdout.Bytes()), nil
		}

		stderrStr := strings.TrimSpace(stderr.String())
		stdoutStr := strings.TrimSpace(stdout.String())
		retryable := isRetryableOutput(stderrStr) || isRetryableOutput(stdoutStr)

		// Prefer stderr for diagnostics; fall back to stdout if stderr is empty.
		diagStr := stderrStr
		if diagStr == "" {
			diagStr = stdoutStr
		}

		if attempt == maxAttempts || !retryable {
			return nil, fmt.Errorf("%s (attempt %d/%d): %w: %s", method, attempt, maxAttempts, err, diagStr)
		}

		backoff = nextBackoff(backoff)
		logger.Warn().
			Str("method", method).
			Int("attempt", attempt).
			Dur("backoff", backoff).
			Str("output", stderrStr).
			Msg("callContract retrying after transient error")

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
	}

	// unreachable
	return nil, fmt.Errorf("%s: max attempts exceeded", method)
}

// isRetryableOutput reports whether the stellar CLI output indicates a transient
// error that is worth retrying (rate limit, rejection, network timeout).
func isRetryableOutput(out string) bool {
	lowered := strings.ToLower(out)
	for _, signal := range []string{"429", "rejected", "too many requests", "rate limit", "timeout", "connection refused", "context deadline exceeded"} {
		if strings.Contains(lowered, signal) {
			return true
		}
	}
	return false
}

// nextBackoff doubles the backoff duration, starting at 1s and capping at 30s.
func nextBackoff(current time.Duration) time.Duration {
	if current == 0 {
		return 1 * time.Second
	}
	next := current * 2
	if next > 30*time.Second {
		return 30 * time.Second
	}
	return next
}

type PairInfo struct {
	Symbol            string          `json:"symbol"`
	ReflectorAsset    ReflectorAsset  `json:"reflector_asset"`
	GroupIndex        uint32          `json:"group_index"`
	SpreadP           string          `json:"spread_p"`
	MinLeverage       uint32          `json:"min_leverage"`
	MaxLeverage       uint32          `json:"max_leverage"`
	MinLevPosUsdc     string          `json:"min_lev_pos_usdc"`
	MaxOiUsdc         string          `json:"max_oi_usdc"`
	MaxNegPnlP        string          `json:"max_neg_pnl_p"`
	LiqThresholdP     uint32          `json:"liq_threshold_p"`
	MaxGainP          uint32          `json:"max_gain_p"`
	Disabled          bool            `json:"disabled"`
}

type ReflectorAsset struct {
	Stellar *string `json:"Stellar,omitempty"`
	Other   *string `json:"Other,omitempty"`
}

type Group struct {
	Name               string `json:"name"`
	MaxCollateralUsdc  string `json:"max_collateral_usdc"`
	OpenFeeP           string `json:"open_fee_p"`
	CloseFeeP          string `json:"close_fee_p"`
}

func upsertPair(ctx context.Context, pool *pgxpool.Pool, idx uint32, p *PairInfo, depth string) error {
	assetType, assetVal := "other", ""
	if p.ReflectorAsset.Stellar != nil {
		assetType, assetVal = "stellar", *p.ReflectorAsset.Stellar
	} else if p.ReflectorAsset.Other != nil {
		assetType, assetVal = "other", *p.ReflectorAsset.Other
	}

	_, err := pool.Exec(ctx, `
		INSERT INTO pairs (
		  pair_index, symbol, reflector_asset_type, reflector_asset,
		  group_index, spread_p, min_leverage, max_leverage,
		  min_lev_pos_usdc, max_oi_usdc, max_neg_pnl_p,
		  liq_threshold_p, max_gain_p, disabled, one_percent_depth, synced_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,now())
		ON CONFLICT (pair_index) DO UPDATE
		  SET symbol               = EXCLUDED.symbol,
		      reflector_asset_type = EXCLUDED.reflector_asset_type,
		      reflector_asset      = EXCLUDED.reflector_asset,
		      group_index          = EXCLUDED.group_index,
		      spread_p             = EXCLUDED.spread_p,
		      min_leverage         = EXCLUDED.min_leverage,
		      max_leverage         = EXCLUDED.max_leverage,
		      min_lev_pos_usdc     = EXCLUDED.min_lev_pos_usdc,
		      max_oi_usdc          = EXCLUDED.max_oi_usdc,
		      max_neg_pnl_p        = EXCLUDED.max_neg_pnl_p,
		      liq_threshold_p      = EXCLUDED.liq_threshold_p,
		      max_gain_p           = EXCLUDED.max_gain_p,
		      disabled             = EXCLUDED.disabled,
		      one_percent_depth    = EXCLUDED.one_percent_depth,
		      synced_at            = EXCLUDED.synced_at`,
		idx, p.Symbol, assetType, assetVal,
		p.GroupIndex, p.SpreadP, p.MinLeverage, p.MaxLeverage,
		p.MinLevPosUsdc, p.MaxOiUsdc, p.MaxNegPnlP,
		p.LiqThresholdP, p.MaxGainP, p.Disabled, depth,
	)
	return err
}

func upsertGroup(ctx context.Context, pool *pgxpool.Pool, idx uint32, g *Group) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO pair_groups (
		  group_index, name, max_collateral_usdc, open_fee_p, close_fee_p, synced_at
		) VALUES ($1,$2,$3,$4,$5,now())
		ON CONFLICT (group_index) DO UPDATE
		  SET name                = EXCLUDED.name,
		      max_collateral_usdc = EXCLUDED.max_collateral_usdc,
		      open_fee_p          = EXCLUDED.open_fee_p,
		      close_fee_p         = EXCLUDED.close_fee_p,
		      synced_at           = EXCLUDED.synced_at`,
		idx, g.Name, g.MaxCollateralUsdc, g.OpenFeeP, g.CloseFeeP,
	)
	return err
}
