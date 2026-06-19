package keeper

import (
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/lumenliquid/backend/internal/soroban"
	"github.com/rs/zerolog"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

type pendingItem struct {
	Action    PendingAction
	Retries   int
	EnqueueAt time.Time
}

type Executor struct {
	rpc        *soroban.Client
	txb        *TxBuilder
	state      *TradeState
	logger     zerolog.Logger
	maxRetries int
	interval   time.Duration
	networkPassphrase string

	mu      sync.Mutex
	pending map[TradeKey]*pendingItem
}

func NewExecutor(
	rpc *soroban.Client,
	txb *TxBuilder,
	state *TradeState,
	networkPassphrase string,
	interval time.Duration,
	maxRetries int,
	logger zerolog.Logger,
) *Executor {
	return &Executor{
		rpc:               rpc,
		txb:               txb,
		state:             state,
		logger:            logger,
		maxRetries:        maxRetries,
		interval:          interval,
		networkPassphrase: networkPassphrase,
		pending:           make(map[TradeKey]*pendingItem),
	}
}

func (e *Executor) Enqueue(actions []PendingAction) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, a := range actions {
		if _, exists := e.pending[a.Key]; !exists {
			e.pending[a.Key] = &pendingItem{
				Action:    a,
				EnqueueAt: time.Now(),
			}
			e.logger.Info().
				Str("type", string(a.Type)).
				Str("trader", a.Key.Trader).
				Int("pair", a.Key.PairIndex).
				Int("trade_idx", a.Key.TradeIndex).
				Msg("executor: enqueued")
		}
	}
}

func (e *Executor) Remove(key TradeKey) {
	e.mu.Lock()
	delete(e.pending, key)
	e.mu.Unlock()
}

func (e *Executor) PendingCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.pending)
}

func (e *Executor) Run(ctx context.Context) {
	tick := time.NewTicker(e.interval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			e.processPending(ctx)
		}
	}
}

func (e *Executor) processPending(ctx context.Context) {
	e.mu.Lock()
	items := make([]*pendingItem, 0, len(e.pending))
	for _, item := range e.pending {
		items = append(items, item)
	}
	e.mu.Unlock()

	for _, item := range items {
		if ctx.Err() != nil {
			return
		}
		sent := e.tryExecute(ctx, item)
		if sent {
			// One tx submitted per tick — wait for next tick to avoid seq conflicts.
			return
		}
	}
}

func (e *Executor) tryExecute(ctx context.Context, item *pendingItem) bool {
	a := item.Action
	logger := e.logger.With().
		Str("type", string(a.Type)).
		Str("trader", a.Key.Trader).
		Int("pair", a.Key.PairIndex).
		Int("trade_idx", a.Key.TradeIndex).
		Logger()

	// Build unsigned tx for simulation
	var unsignedXDR string
	var err error
	switch a.Type {
	case ActionLiquidate:
		unsignedXDR, err = e.txb.BuildLiquidateTx(a.Key.Trader, a.Key.PairIndex, a.Key.TradeIndex)
	case ActionTpSl:
		unsignedXDR, err = e.txb.BuildExecuteTpSlTx(a.Key.Trader, a.Key.PairIndex, a.Key.TradeIndex)
	}
	if err != nil {
		logger.Error().Err(err).Msg("executor: build tx failed")
		e.dropItem(item, "build_error")
		return false
	}

	// Simulate
	sim, err := e.rpc.SimulateTransaction(ctx, unsignedXDR)
	if err != nil {
		logger.Warn().Err(err).Msg("executor: simulate RPC error")
		item.Retries++
		if item.Retries >= e.maxRetries {
			e.dropItem(item, "max_retries_rpc")
		}
		return false
	}

	if !sim.IsSuccess() {
		item.Retries++
		if isTradeNotFound(sim.Error) {
			logger.Info().Msg("executor: trade not found (already closed), dropping")
			e.dropItem(item, "trade_not_found")
			// Remove from state too, else the detector re-enqueues it every tick
			// until the indexer's close event arrives via Redis.
			e.state.Remove(a.Key)
			return false
		}
		if isPriceMismatch(sim.Error) {
			// PriceMismatch (#21) means the on-chain Reflector oracle hasn't
			// crossed the TP/SL threshold yet — the contract gate is working as
			// designed. Don't count this as a real error or burn retries; just
			// drop from the pending queue. The detector re-enqueues on the next
			// Binance tick once the condition is actually met.
			logger.Debug().Msg("executor: price mismatch (oracle not confirmed), dropping for re-detect")
			e.dropItem(item, "price_mismatch")
			return false
		}
		if item.Retries >= e.maxRetries {
			logger.Warn().Str("sim_error", sim.Error).Int("retries", item.Retries).Msg("executor: max retries, dropping")
			e.dropItem(item, "max_retries_sim")
			return false
		}
		logger.Debug().Str("sim_error", sim.Error).Int("retry", item.Retries).Msg("executor: oracle not ready, will retry")
		return false
	}

	// Simulation passed — fetch fresh sequence, prepare, and send
	preparedXDR, err := e.prepareTx(ctx, unsignedXDR, sim)
	if err != nil {
		logger.Error().Err(err).Msg("executor: prepare tx failed")
		e.dropItem(item, "prepare_error")
		return false
	}

	sendRes, err := e.rpc.SendTransaction(ctx, preparedXDR)
	if err != nil {
		logger.Error().Err(err).Msg("executor: send tx failed")
		item.Retries++
		if item.Retries >= e.maxRetries {
			e.dropItem(item, "max_retries_send")
		}
		return false
	}

	switch sendRes.Status {
	case "PENDING", "TRY_AGAIN_LATER":
		logger.Info().Str("hash", sendRes.Hash).Str("status", sendRes.Status).Msg("executor: tx submitted")
		e.dropItem(item, "submitted")
		return true
	case "DUPLICATE":
		logger.Info().Str("hash", sendRes.Hash).Msg("executor: tx duplicate")
		e.dropItem(item, "duplicate")
		return true
	case "ERROR":
		logger.Warn().Str("hash", sendRes.Hash).Str("error_xdr", sendRes.ErrorResultXDR).Msg("executor: tx error")
		item.Retries++
		if item.Retries >= e.maxRetries {
			e.dropItem(item, "tx_error_max_retries")
		}
	}
	return false
}

func (e *Executor) dropItem(item *pendingItem, reason string) {
	e.mu.Lock()
	delete(e.pending, item.Action.Key)
	e.mu.Unlock()
	e.logger.Debug().
		Str("reason", reason).
		Str("trader", item.Action.Key.Trader).
		Int("trade_idx", item.Action.Key.TradeIndex).
		Msg("executor: dropped from queue")
}

// prepareTx rebuilds the transaction from scratch with simulation results
// (auth entries, soroban data, resource fee) and a fresh sequence number.
func (e *Executor) prepareTx(ctx context.Context, unsignedXDR string, sim *soroban.SimResult) (string, error) {
	// Decode the original envelope to extract the invoke args
	raw, err := base64.StdEncoding.DecodeString(unsignedXDR)
	if err != nil {
		return "", fmt.Errorf("decode tx xdr: %w", err)
	}
	var envelope xdr.TransactionEnvelope
	if err := envelope.UnmarshalBinary(raw); err != nil {
		return "", fmt.Errorf("unmarshal envelope: %w", err)
	}

	v1, ok := envelope.GetV1()
	if !ok || len(v1.Tx.Operations) == 0 {
		return "", fmt.Errorf("no v1 tx or operations")
	}
	ihf := v1.Tx.Operations[0].Body.InvokeHostFunctionOp
	if ihf == nil {
		return "", fmt.Errorf("not an InvokeHostFunction op")
	}

	// Decode auth from simulation
	var authEntries []xdr.SorobanAuthorizationEntry
	if len(sim.Results) > 0 {
		for _, authB64 := range sim.Results[0].Auth {
			authRaw, err := base64.StdEncoding.DecodeString(authB64)
			if err != nil {
				continue
			}
			var ae xdr.SorobanAuthorizationEntry
			if err := ae.UnmarshalBinary(authRaw); err != nil {
				continue
			}
			authEntries = append(authEntries, ae)
		}
	}

	// Decode soroban transaction data
	sdRaw, err := base64.StdEncoding.DecodeString(sim.TransactionData)
	if err != nil {
		return "", fmt.Errorf("decode soroban data: %w", err)
	}
	var sd xdr.SorobanTransactionData
	if err := sd.UnmarshalBinary(sdRaw); err != nil {
		return "", fmt.Errorf("unmarshal soroban data: %w", err)
	}

	// Fetch fresh sequence
	freshSeq, err := e.fetchSequence(ctx)
	if err != nil {
		return "", fmt.Errorf("fetch sequence: %w", err)
	}

	// Rebuild tx from scratch with auth + soroban data
	minFee, _ := strconv.ParseInt(sim.MinResourceFee, 10, 64)
	op := &txnbuild.InvokeHostFunction{
		HostFunction:  ihf.HostFunction,
		Auth:          authEntries,
		SourceAccount: e.txb.Address(),
		Ext:           xdr.TransactionExt{V: 1, SorobanData: &sd},
	}
	account := txnbuild.SimpleAccount{AccountID: e.txb.Address(), Sequence: freshSeq + 1}
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: false,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              100 + minFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(60)},
	})
	if err != nil {
		return "", fmt.Errorf("build prepared tx: %w", err)
	}
	tx, err = tx.Sign(e.networkPassphrase, e.txb.kp)
	if err != nil {
		return "", fmt.Errorf("sign prepared tx: %w", err)
	}

	finalBytes, err := tx.MarshalBinary()
	if err != nil {
		return "", fmt.Errorf("marshal prepared tx: %w", err)
	}
	return base64.StdEncoding.EncodeToString(finalBytes), nil
}

func (e *Executor) fetchSequence(ctx context.Context) (int64, error) {
	var aid xdr.AccountId
	if err := aid.SetAddress(e.txb.Address()); err != nil {
		return 0, err
	}
	key := xdr.LedgerKey{
		Type:    xdr.LedgerEntryTypeAccount,
		Account: &xdr.LedgerKeyAccount{AccountId: aid},
	}
	keyBytes, err := key.MarshalBinary()
	if err != nil {
		return 0, err
	}
	keyB64 := base64.StdEncoding.EncodeToString(keyBytes)

	res, err := e.rpc.GetLedgerEntries(ctx, []string{keyB64})
	if err != nil {
		return 0, err
	}
	if len(res.Entries) == 0 {
		return 0, fmt.Errorf("account not found")
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
		return 0, fmt.Errorf("not an account entry")
	}
	return int64(acct.SeqNum), nil
}

func isTradeNotFound(simErr string) bool {
	// Contract returns TradeNotFound as Error(Contract, #17) — match either the
	// name or the numeric code, since the sim error string may carry either form.
	return simErr != "" && (strings.Contains(simErr, "TradeNotFound") || strings.Contains(simErr, "#17"))
}

func isPriceMismatch(simErr string) bool {
	// Contract returns PriceMismatch as Error(Contract, #21) when the on-chain
	// Reflector price hasn't crossed the threshold yet. Match name or code.
	return simErr != "" && (strings.Contains(simErr, "PriceMismatch") || strings.Contains(simErr, "#21"))
}
