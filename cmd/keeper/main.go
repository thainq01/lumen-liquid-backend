// keeper watches Binance aggTrade prices, detects trades that have crossed
// their liquidation or TP/SL thresholds, and submits the matching contract
// call once the on-chain oracle confirms the condition.
//
// Two-stage design: Binance WS is the low-latency trigger; the on-chain oracle
// is the execution gate. A triggered trade is queued and re-simulated each tick
// until the contract's own oracle read agrees, then the tx is sent.
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/lumenliquid/backend/internal/config"
	"github.com/lumenliquid/backend/internal/db"
	"github.com/lumenliquid/backend/internal/keeper"
	"github.com/lumenliquid/backend/internal/log"
	"github.com/lumenliquid/backend/internal/pubsub"
	"github.com/lumenliquid/backend/internal/soroban"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/xdr"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(1)
	}
	logger := log.Init(cfg.LogLevel)

	if cfg.KeeperSecret == "" {
		logger.Fatal().Msg("KEEPER_SECRET must be set")
	}
	if cfg.PMContractID == "" {
		logger.Fatal().Msg("PM_CONTRACT_ID must be set")
	}
	if cfg.PairSymbolMap == "" {
		logger.Fatal().Msg("PAIR_SYMBOL_MAP must be set (e.g. BTCUSDT:0,ETHUSDT:1)")
	}

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

	rpc := soroban.New(cfg.SorobanRPCURL)

	// Trade state: load from DB, keep fresh via Redis.
	state := keeper.NewTradeState(logger)
	if err := state.LoadFromDB(ctx, pool); err != nil {
		logger.Fatal().Err(err).Msg("load trades")
	}
	go state.SubscribeRedis(ctx, rdb, logger)

	// Tx builder: needs the keeper account's current sequence number.
	keeperKP, err := keeperAddress(cfg.KeeperSecret)
	if err != nil {
		logger.Fatal().Err(err).Msg("parse keeper secret")
	}
	seq, err := fetchSequence(ctx, rpc, keeperKP)
	if err != nil {
		logger.Fatal().Err(err).Msg("fetch keeper sequence")
	}
	logger.Info().Str("keeper", keeperKP).Int64("seq", seq).Msg("keeper account loaded")

	txb, err := keeper.NewTxBuilder(cfg.KeeperSecret, cfg.NetworkPassphrase, cfg.PMContractID, seq)
	if err != nil {
		logger.Fatal().Err(err).Msg("init tx builder")
	}

	executor := keeper.NewExecutor(
		rpc, txb, state, cfg.NetworkPassphrase,
		cfg.KeeperPollInterval, cfg.KeeperMaxRetries, logger,
	)
	go executor.Run(ctx)

	// Binance WS price feed.
	binance := keeper.NewBinanceClient(cfg.BinanceWSURL, cfg.PairSymbolMap, logger)
	go binance.Run(ctx)

	logger.Info().
		Int("trades", state.Count()).
		Str("pair_map", cfg.PairSymbolMap).
		Msg("keeper running")

	// Hot path: Binance price → detect → enqueue.
	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("shutdown")
			return
		case tick, ok := <-binance.Prices():
			if !ok {
				logger.Info().Msg("binance feed closed")
				return
			}
			actions := keeper.Detect(tick.PairIndex, tick.Price, state)
			if len(actions) > 0 {
				executor.Enqueue(actions)
			}
		}
	}
}

func keeperAddress(secret string) (string, error) {
	kp, err := keypair.ParseFull(secret)
	if err != nil {
		return "", err
	}
	return kp.Address(), nil
}

// fetchSequence reads the keeper account's current sequence from Soroban RPC
// via getLedgerEntries on the account ledger key.
func fetchSequence(ctx context.Context, rpc *soroban.Client, address string) (int64, error) {
	var aid xdr.AccountId
	if err := aid.SetAddress(address); err != nil {
		return 0, err
	}
	key := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeAccount,
		Account: &xdr.LedgerKeyAccount{
			AccountId: aid,
		},
	}
	keyBytes, err := key.MarshalBinary()
	if err != nil {
		return 0, err
	}
	keyB64 := base64.StdEncoding.EncodeToString(keyBytes)

	res, err := rpc.GetLedgerEntries(ctx, []string{keyB64})
	if err != nil {
		return 0, err
	}
	if len(res.Entries) == 0 {
		return 0, fmt.Errorf("keeper account not found on-chain: %s", address)
	}

	entryRaw, err := base64.StdEncoding.DecodeString(res.Entries[0].XDR)
	if err != nil {
		return 0, err
	}
	var entry xdr.LedgerEntryData
	if err := entry.UnmarshalBinary(entryRaw); err != nil {
		return 0, err
	}
	acct, ok := entry.GetAccount()
	if !ok {
		return 0, fmt.Errorf("ledger entry is not an account")
	}
	return int64(acct.SeqNum), nil
}
