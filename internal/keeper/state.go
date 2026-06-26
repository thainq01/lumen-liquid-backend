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
	Key      TradeKey
	IsLong   bool
	LiqPrice *big.Int
	TpPrice  *big.Int
	SlPrice  *big.Int
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
	logger zerolog.Logger
}

func NewTradeState(logger zerolog.Logger) *TradeState {
	return &TradeState{
		trades: make(map[TradeKey]*TradeEntry),
		limits: make(map[LimitKey]*LimitEntry),
		logger: logger,
	}
}

func (s *TradeState) LoadFromDB(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pool.Query(ctx, `
		SELECT trader, pair_index, trade_index, is_long, leverage,
		       collateral, open_price, tp_price, sl_price, liq_price
		FROM trades`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var n int
	for rows.Next() {
		var trader string
		var pairIdx, tradeIdx, leverage int
		var isLong bool
		var collateral, openPrice, tpPrice, slPrice, liqPrice string

		if err := rows.Scan(&trader, &pairIdx, &tradeIdx, &isLong, &leverage,
			&collateral, &openPrice, &tpPrice, &slPrice, &liqPrice); err != nil {
			s.logger.Warn().Err(err).Msg("state: scan trade row")
			continue
		}

		key := TradeKey{Trader: trader, PairIndex: pairIdx, TradeIndex: tradeIdx}
		entry := &TradeEntry{
			Key:    key,
			IsLong: isLong,
		}
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
	return rows.Err()
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
			Key:      key,
			IsLong:   e.Trade.IsLong,
			LiqPrice: liqPrice,
			TpPrice:  e.Trade.TpPrice,
			SlPrice:  e.Trade.SlPrice,
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
