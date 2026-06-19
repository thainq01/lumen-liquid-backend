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

type TradeState struct {
	mu     sync.RWMutex
	trades map[TradeKey]*TradeEntry
	logger zerolog.Logger
}

func NewTradeState(logger zerolog.Logger) *TradeState {
	return &TradeState{
		trades: make(map[TradeKey]*TradeEntry),
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
