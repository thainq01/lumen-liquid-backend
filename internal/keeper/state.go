package keeper

import (
	"context"
	"encoding/json"
	"math/big"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lumenliquid/backend/internal/events"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

type TradeKey struct {
	Trader     string
	PairIndex  int
	TradeIndex int
}

type TradeEntry struct {
	Key             TradeKey
	IsLong          bool
	Leverage        uint32
	Collateral      *big.Int
	OpenPrice       *big.Int
	AccRolloverOpen *big.Int
	AccFundingOpen  *big.Int
	LiqThresholdP   uint32
	LiqPrice        *big.Int
	TpPrice         *big.Int
	SlPrice         *big.Int
}

// LimitKey identifies an open limit order. TradeIndex field is reused as the
// limit_index so the executor's pending map can hold both without collision
// (disambiguated by PendingAction.Type).
type LimitKey struct {
	Trader     string
	PairIndex  int
	LimitIndex int
}

type LimitEntry struct {
	Key        LimitKey
	IsLong     bool
	LimitPrice *big.Int
}

type TradeState struct {
	mu     sync.RWMutex
	trades map[TradeKey]*TradeEntry
	limits map[LimitKey]*LimitEntry
	fees   map[int]*PairFeeState
	logger zerolog.Logger
}

type PairFeeState struct {
	PairIndex         int
	AccRollover       *big.Int
	AccFundingLong    *big.Int
	AccFundingShort   *big.Int
	ProjectedAtLedger uint64
}

func NewTradeState(logger zerolog.Logger) *TradeState {
	return &TradeState{
		trades: make(map[TradeKey]*TradeEntry),
		limits: make(map[LimitKey]*LimitEntry),
		fees:   make(map[int]*PairFeeState),
		logger: logger,
	}
}

func (s *TradeState) LoadFromDB(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT t.trader, t.pair_index, t.trade_index, t.is_long, t.leverage,
		       t.collateral, t.open_price, t.acc_rollover_open, t.acc_funding_open,
		       t.tp_price, t.sl_price, t.liq_price, COALESCE(p.liq_threshold_p, t.liq_threshold_p)
		FROM trades t
		LEFT JOIN pairs p ON p.pair_index = t.pair_index`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var n int
	for rows.Next() {
		var trader string
		var pairIdx, tradeIdx int
		var leverage uint32
		var isLong bool
		var liqThresholdP uint32
		var collateral, openPrice, accRolloverOpen, accFundingOpen, tpPrice, slPrice, liqPrice string

		if err := rows.Scan(&trader, &pairIdx, &tradeIdx, &isLong, &leverage,
			&collateral, &openPrice, &accRolloverOpen, &accFundingOpen,
			&tpPrice, &slPrice, &liqPrice, &liqThresholdP); err != nil {
			s.logger.Warn().Err(err).Msg("state: scan trade row")
			continue
		}

		key := TradeKey{Trader: trader, PairIndex: pairIdx, TradeIndex: tradeIdx}
		entry := &TradeEntry{
			Key:           key,
			IsLong:        isLong,
			Leverage:      leverage,
			LiqThresholdP: liqThresholdP,
		}
		entry.Collateral, _ = new(big.Int).SetString(collateral, 10)
		entry.OpenPrice, _ = new(big.Int).SetString(openPrice, 10)
		entry.AccRolloverOpen, _ = new(big.Int).SetString(accRolloverOpen, 10)
		entry.AccFundingOpen, _ = new(big.Int).SetString(accFundingOpen, 10)
		entry.LiqPrice, _ = new(big.Int).SetString(liqPrice, 10)
		entry.TpPrice, _ = new(big.Int).SetString(tpPrice, 10)
		entry.SlPrice, _ = new(big.Int).SetString(slPrice, 10)

		s.mu.Lock()
		s.trades[key] = entry
		s.mu.Unlock()
		n++
	}
	s.logger.Info().Int("count", n).Msg("state: loaded trades from DB")

	if err := s.loadLimitsFromDB(ctx, pool); err != nil {
		return err
	}
	if err := s.LoadFeeAccumulatorsFromDB(ctx, pool); err != nil {
		return err
	}
	return rows.Err()
}

func (s *TradeState) LoadFeeAccumulatorsFromDB(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT pair_index, acc_rollover, acc_funding_long, acc_funding_short, projected_at_ledger
		FROM pair_fee_accumulators`)
	if err != nil {
		return err
	}
	defer rows.Close()

	next := make(map[int]*PairFeeState)
	for rows.Next() {
		var pairIdx int
		var ledger uint64
		var accRollover, accFundingLong, accFundingShort string
		if err := rows.Scan(&pairIdx, &accRollover, &accFundingLong, &accFundingShort, &ledger); err != nil {
			s.logger.Warn().Err(err).Msg("state: scan pair fee accumulator row")
			continue
		}
		fee := &PairFeeState{PairIndex: pairIdx, ProjectedAtLedger: ledger}
		fee.AccRollover, _ = new(big.Int).SetString(accRollover, 10)
		fee.AccFundingLong, _ = new(big.Int).SetString(accFundingLong, 10)
		fee.AccFundingShort, _ = new(big.Int).SetString(accFundingShort, 10)
		next[pairIdx] = fee
	}
	if err := rows.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	s.fees = next
	s.mu.Unlock()
	s.logger.Debug().Int("count", len(next)).Msg("state: refreshed pair fee accumulators")
	return nil
}

func (s *TradeState) loadLimitsFromDB(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT trader, pair_index, limit_index, is_long, limit_price
		FROM limit_orders`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var n int
	for rows.Next() {
		var trader string
		var pairIdx, limitIdx int
		var isLong bool
		var limitPrice string
		if err := rows.Scan(&trader, &pairIdx, &limitIdx, &isLong, &limitPrice); err != nil {
			s.logger.Warn().Err(err).Msg("state: scan limit row")
			continue
		}
		key := LimitKey{Trader: trader, PairIndex: pairIdx, LimitIndex: limitIdx}
		entry := &LimitEntry{Key: key, IsLong: isLong}
		entry.LimitPrice, _ = new(big.Int).SetString(limitPrice, 10)

		s.mu.Lock()
		s.limits[key] = entry
		s.mu.Unlock()
		n++
	}
	s.logger.Info().Int("count", n).Msg("state: loaded limit orders from DB")
	return rows.Err()
}

func (s *TradeState) Apply(e events.Event) {
	switch e.Topic {
	case "opened":
		if e.Trade == nil || e.TradeIndex == nil {
			return
		}
		key := TradeKey{
			Trader:     e.Trader,
			PairIndex:  int(e.Trade.PairIndex),
			TradeIndex: int(*e.TradeIndex),
		}
		liqPrice, _ := LiquidationPrice(
			e.Trade.OpenPrice, e.Trade.IsLong,
			e.Trade.Collateral, e.Trade.Leverage,
			nil, nil, 90,
		)
		entry := &TradeEntry{
			Key:             key,
			IsLong:          e.Trade.IsLong,
			Leverage:        e.Trade.Leverage,
			Collateral:      e.Trade.Collateral,
			OpenPrice:       e.Trade.OpenPrice,
			AccRolloverOpen: e.Trade.AccRolloverOpen,
			AccFundingOpen:  e.Trade.AccFundingOpen,
			LiqThresholdP:   90,
			LiqPrice:        liqPrice,
			TpPrice:         e.Trade.TpPrice,
			SlPrice:         e.Trade.SlPrice,
		}
		s.mu.Lock()
		s.trades[key] = entry
		s.mu.Unlock()
		s.logger.Debug().Str("trader", e.Trader).Int("pair", int(e.Trade.PairIndex)).Msg("state: added trade")

	case "closed", "liq", "tp_sl_executed":
		if e.TradeIndex == nil {
			return
		}
		s.Remove(TradeKey{
			Trader:     e.Trader,
			PairIndex:  int(deref32(e.PairIndex)),
			TradeIndex: int(*e.TradeIndex),
		})

	case "updated_tp_sl":
		if e.TradeIndex == nil {
			return
		}
		s.mu.Lock()
		key := TradeKey{
			Trader:     e.Trader,
			PairIndex:  int(deref32(e.PairIndex)),
			TradeIndex: int(*e.TradeIndex),
		}
		if t, ok := s.trades[key]; ok {
			if e.TpPrice != nil {
				t.TpPrice = e.TpPrice
			}
			if e.SlPrice != nil {
				t.SlPrice = e.SlPrice
			}
			s.logger.Debug().Str("trader", e.Trader).Int("pair", key.PairIndex).
				Int("trade_idx", key.TradeIndex).Msg("state: updated tp/sl")
		}
		s.mu.Unlock()

	case "placed":
		if e.Limit == nil || e.LimitIndex == nil {
			return
		}
		key := LimitKey{
			Trader:     e.Trader,
			PairIndex:  int(e.Limit.PairIndex),
			LimitIndex: int(*e.LimitIndex),
		}
		entry := &LimitEntry{Key: key, IsLong: e.Limit.IsLong, LimitPrice: e.Limit.LimitPrice}
		s.mu.Lock()
		s.limits[key] = entry
		s.mu.Unlock()
		s.logger.Info().Str("trader", e.Trader).Int("pair", key.PairIndex).
			Int("limit_idx", key.LimitIndex).Str("limit_price", bigStr(e.Limit.LimitPrice)).
			Bool("is_long", e.Limit.IsLong).Msg("state: added limit order to watch")

	case "executed", "canceled":
		// executed also emits `opened`, handled above; here drop the limit order.
		if e.LimitIndex == nil {
			return
		}
		s.RemoveLimit(LimitKey{
			Trader:     e.Trader,
			PairIndex:  int(deref32(e.PairIndex)),
			LimitIndex: int(*e.LimitIndex),
		})

	case "updated_limit":
		if e.LimitIndex == nil {
			return
		}
		s.mu.Lock()
		key := LimitKey{
			Trader:     e.Trader,
			PairIndex:  int(deref32(e.PairIndex)),
			LimitIndex: int(*e.LimitIndex),
		}
		if l, ok := s.limits[key]; ok && e.LimitPrice != nil {
			l.LimitPrice = e.LimitPrice
			s.logger.Debug().Str("trader", e.Trader).Int("pair", key.PairIndex).
				Int("limit_idx", key.LimitIndex).Msg("state: updated limit price")
		}
		s.mu.Unlock()
	}
}

func (s *TradeState) FeeState(pairIndex int) *PairFeeState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fee := s.fees[pairIndex]
	if fee == nil {
		return nil
	}
	return &PairFeeState{
		PairIndex:         fee.PairIndex,
		AccRollover:       cloneBig(fee.AccRollover),
		AccFundingLong:    cloneBig(fee.AccFundingLong),
		AccFundingShort:   cloneBig(fee.AccFundingShort),
		ProjectedAtLedger: fee.ProjectedAtLedger,
	}
}

func deref32(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

func (s *TradeState) Remove(key TradeKey) {
	s.mu.Lock()
	delete(s.trades, key)
	s.mu.Unlock()
}

func (s *TradeState) GetTradesForPair(pairIndex int) []TradeEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []TradeEntry
	for _, t := range s.trades {
		if t.Key.PairIndex == pairIndex {
			out = append(out, *t)
		}
	}
	return out
}

func (s *TradeState) GetLimitsForPair(pairIndex int) []LimitEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []LimitEntry
	for _, l := range s.limits {
		if l.Key.PairIndex == pairIndex {
			out = append(out, *l)
		}
	}
	return out
}

func (s *TradeState) RemoveLimit(key LimitKey) {
	s.mu.Lock()
	delete(s.limits, key)
	s.mu.Unlock()
}

func bigStr(b *big.Int) string {
	if b == nil {
		return "0"
	}
	return b.String()
}

func cloneBig(b *big.Int) *big.Int {
	if b == nil {
		return nil
	}
	return new(big.Int).Set(b)
}

func (s *TradeState) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.trades)
}

// SubscribeRedis listens to the indexer's Redis events and updates state.
// Subscribes only to events:global — the indexer fans every event out there,
// plus to per-trader/per-pair channels. Using global avoids duplicate delivery.
func (s *TradeState) SubscribeRedis(ctx context.Context, rdb *redis.Client, logger zerolog.Logger) {
	sub := rdb.Subscribe(ctx, "events:global")
	ch := sub.Channel()
	logger.Info().Msg("state: subscribed to Redis events:global")

	go func() {
		<-ctx.Done()
		sub.Close()
	}()

	for msg := range ch {
		var e events.Event
		if err := json.Unmarshal([]byte(msg.Payload), &e); err != nil {
			logger.Warn().Err(err).Str("channel", msg.Channel).Msg("state: unmarshal event")
			continue
		}
		s.Apply(e)
	}
}
