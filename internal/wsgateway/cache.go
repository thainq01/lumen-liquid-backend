package wsgateway

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/lumenliquid/backend/internal/events"
	"github.com/lumenliquid/backend/internal/keeper"
)

// CachedTrade is a trader's open position, sent over WebSocket.
type CachedTrade struct {
	Trader     string `json:"trader"`
	PairIndex  int    `json:"pair_index"`
	TradeIndex int    `json:"trade_index"`
	IsLong     bool   `json:"is_long"`
	Leverage   int    `json:"leverage"`
	Collateral string `json:"collateral"`
	OpenPrice  string `json:"open_price"`
	TpPrice    string `json:"tp_price"`
	SlPrice    string `json:"sl_price"`
	LiqPrice   string `json:"liq_price"`
	OpenedAt   string `json:"opened_at"`
}

// CachedPair is pair config, sent alongside trades so the client has context.
type CachedPair struct {
	PairIndex     int    `json:"pair_index"`
	Symbol        string `json:"symbol"`
	MinLeverage   int    `json:"min_leverage"`
	MaxLeverage   int    `json:"max_leverage"`
	LiqThresholdP int    `json:"liq_threshold_p"`
	Disabled      bool   `json:"disabled"`
}

// CachedLimit is a trader's open limit order, sent over WebSocket.
type CachedLimit struct {
	Trader     string `json:"trader"`
	PairIndex  int    `json:"pair_index"`
	LimitIndex int    `json:"limit_index"`
	IsLong     bool   `json:"is_long"`
	Leverage   int    `json:"leverage"`
	Collateral string `json:"collateral"`
	LimitPrice string `json:"limit_price"`
	TpPrice    string `json:"tp_price"`
	SlPrice    string `json:"sl_price"`
	PlacedAt   string `json:"placed_at"`
}

// TradeCache holds open trades and pair configs in memory, updated in
// real-time from indexer events via a persistent Redis subscription to
// `events:global`. Updates are idempotent: a re-delivered event (the indexer
// re-publishes on ledger re-scan) does not duplicate state and does not
// re-notify subscribers.
type TradeCache struct {
	mu       sync.RWMutex
	trades   map[string][]CachedTrade // trader -> open trades
	limits   map[string][]CachedLimit // trader -> open limit orders
	pairs    map[int]CachedPair       // pair_index -> config
	pool     *pgxpool.Pool
	rdb      *redis.Client
	onChange func(trader string)
}

func NewTradeCache(pool *pgxpool.Pool, rdb *redis.Client) *TradeCache {
	return &TradeCache{
		trades: make(map[string][]CachedTrade),
		limits: make(map[string][]CachedLimit),
		pairs:  make(map[int]CachedPair),
		pool:   pool,
		rdb:    rdb,
	}
}

// SetOnChange registers a callback fired when a trader's open trades change.
func (c *TradeCache) SetOnChange(fn func(trader string)) { c.onChange = fn }

// Start launches a persistent subscription to `events:global` that keeps the
// cache state in sync. Runs until ctx is canceled. Call in a goroutine.
func (c *TradeCache) Start(ctx context.Context) {
	pubsub := c.rdb.Subscribe(ctx, "events:global")
	defer pubsub.Close()
	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			c.applyEvent([]byte(msg.Payload))
		}
	}
}

// LoadFromDB seeds the cache with current open trades and pair configs.
func (c *TradeCache) LoadFromDB(ctx context.Context) error {
	rows, err := c.pool.Query(ctx, `
		SELECT trader, pair_index, trade_index, is_long, leverage,
		       collateral, open_price, tp_price, sl_price, liq_price, opened_at
		FROM trades`)
	if err != nil {
		return fmt.Errorf("load trades: %w", err)
	}
	defer rows.Close()

	c.mu.Lock()
	c.trades = make(map[string][]CachedTrade)
	for rows.Next() {
		var t CachedTrade
		var openedAt time.Time
		if err := rows.Scan(&t.Trader, &t.PairIndex, &t.TradeIndex, &t.IsLong,
			&t.Leverage, &t.Collateral, &t.OpenPrice, &t.TpPrice, &t.SlPrice,
			&t.LiqPrice, &openedAt); err != nil {
			continue
		}
		t.OpenedAt = openedAt.Format(time.RFC3339)
		c.trades[t.Trader] = append(c.trades[t.Trader], t)
	}
	c.mu.Unlock()

	limitRows, err := c.pool.Query(ctx, `
		SELECT trader, pair_index, limit_index, is_long, leverage,
		       collateral, limit_price, tp_price, sl_price, placed_at
		FROM limit_orders`)
	if err != nil {
		return fmt.Errorf("load limit_orders: %w", err)
	}
	defer limitRows.Close()

	c.mu.Lock()
	c.limits = make(map[string][]CachedLimit)
	for limitRows.Next() {
		var l CachedLimit
		var placedAt time.Time
		if err := limitRows.Scan(&l.Trader, &l.PairIndex, &l.LimitIndex, &l.IsLong,
			&l.Leverage, &l.Collateral, &l.LimitPrice, &l.TpPrice, &l.SlPrice,
			&placedAt); err != nil {
			continue
		}
		l.PlacedAt = placedAt.Format(time.RFC3339)
		c.limits[l.Trader] = append(c.limits[l.Trader], l)
	}
	c.mu.Unlock()

	pairRows, err := c.pool.Query(ctx, `
		SELECT pair_index, symbol, min_leverage, max_leverage, liq_threshold_p, disabled
		FROM pairs`)
	if err != nil {
		return fmt.Errorf("load pairs: %w", err)
	}
	defer pairRows.Close()

	c.mu.Lock()
	c.pairs = make(map[int]CachedPair)
	for pairRows.Next() {
		var p CachedPair
		if err := pairRows.Scan(&p.PairIndex, &p.Symbol, &p.MinLeverage,
			&p.MaxLeverage, &p.LiqThresholdP, &p.Disabled); err != nil {
			continue
		}
		c.pairs[p.PairIndex] = p
	}
	c.mu.Unlock()
	return nil
}

// applyEvent mutates the cache from a Redis event. Idempotent: a re-delivered
// event is a no-op and does not notify. Only PM trade events affect state.
func (c *TradeCache) applyEvent(eventJSON []byte) {
	var e events.Event
	if err := json.Unmarshal(eventJSON, &e); err != nil {
		return
	}
	if e.Source != events.SrcPM {
		return
	}

	c.mu.Lock()
	var changed bool
	switch e.Topic {
	case "opened":
		if e.Trade == nil || e.TradeIndex == nil {
			c.mu.Unlock()
			return
		}
		// Idempotent: skip if this trade is already tracked.
		if !hasTrade(c.trades[e.Trader], int(e.Trade.PairIndex), int(*e.TradeIndex)) {
			c.trades[e.Trader] = append(c.trades[e.Trader], buildTrade(&e))
			changed = true
		}
	case "closed", "liq", "tp_sl_executed":
		if e.TradeIndex == nil {
			c.mu.Unlock()
			return
		}
		pairIdx := 0
		if e.PairIndex != nil {
			pairIdx = int(*e.PairIndex)
		}
		// Idempotent: only notify if the trade was actually present.
		if removed := removeTrade(c.trades, e.Trader, pairIdx, int(*e.TradeIndex)); removed {
			changed = true
		}
	case "updated_tp_sl":
		if e.TradeIndex == nil || e.TpPrice == nil || e.SlPrice == nil {
			c.mu.Unlock()
			return
		}
		pairIdx := 0
		if e.PairIndex != nil {
			pairIdx = int(*e.PairIndex)
		}
		if updated := updateTradeTpSl(c.trades, e.Trader, pairIdx, int(*e.TradeIndex),
			bigStr(e.TpPrice), bigStr(e.SlPrice)); updated {
			changed = true
		}

	case "placed":
		if e.Limit == nil || e.LimitIndex == nil {
			c.mu.Unlock()
			return
		}
		if !hasLimit(c.limits[e.Trader], int(e.Limit.PairIndex), int(*e.LimitIndex)) {
			c.limits[e.Trader] = append(c.limits[e.Trader], buildLimit(&e))
			changed = true
		}
	case "executed", "canceled":
		// `executed` also emits `opened`, which adds the new trade above. Here we
		// drop the consumed/canceled limit order from the trader's open limits.
		if e.LimitIndex == nil {
			c.mu.Unlock()
			return
		}
		pairIdx := 0
		if e.PairIndex != nil {
			pairIdx = int(*e.PairIndex)
		}
		if removed := removeLimit(c.limits, e.Trader, pairIdx, int(*e.LimitIndex)); removed {
			changed = true
		}
	case "updated_limit":
		if e.LimitIndex == nil {
			c.mu.Unlock()
			return
		}
		pairIdx := 0
		if e.PairIndex != nil {
			pairIdx = int(*e.PairIndex)
		}
		if updated := updateLimit(c.limits, e.Trader, pairIdx, int(*e.LimitIndex),
			bigStr(e.LimitPrice), bigStr(e.TpPrice), bigStr(e.SlPrice)); updated {
			changed = true
		}
	}
	c.mu.Unlock()

	if changed && c.onChange != nil {
		c.onChange(e.Trader)
	}
}

// GetSnapshot returns the current open trades + relevant pairs for a trader.
func (c *TradeCache) GetSnapshot(trader string) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	trades := c.trades[trader]
	if trades == nil {
		trades = []CachedTrade{}
	}
	limits := c.limits[trader]
	if limits == nil {
		limits = []CachedLimit{}
	}
	pairSet := make(map[int]bool)
	for _, t := range trades {
		pairSet[t.PairIndex] = true
	}
	for _, l := range limits {
		pairSet[l.PairIndex] = true
	}
	pairs := []CachedPair{}
	for idx, p := range c.pairs {
		if pairSet[idx] {
			pairs = append(pairs, p)
		}
	}
	return json.Marshal(map[string]any{
		"type":   "snapshot",
		"trades": trades,
		"limits": limits,
		"pairs":  pairs,
	})
}

// GetAllSnapshot returns every open trade across all traders + all pairs.
// Used by the global /v1/ws/trades endpoint.
func (c *TradeCache) GetAllSnapshot() ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	all := []CachedTrade{}
	for _, list := range c.trades {
		all = append(all, list...)
	}
	allLimits := []CachedLimit{}
	for _, list := range c.limits {
		allLimits = append(allLimits, list...)
	}
	pairs := []CachedPair{}
	for _, p := range c.pairs {
		pairs = append(pairs, p)
	}
	return json.Marshal(map[string]any{
		"type":   "snapshot",
		"trades": all,
		"limits": allLimits,
		"pairs":  pairs,
	})
}

func buildTrade(e *events.Event) CachedTrade {
	liqPrice, _ := keeper.LiquidationPrice(
		e.Trade.OpenPrice, e.Trade.IsLong,
		e.Trade.Collateral, e.Trade.Leverage,
		nil, nil, 90,
	)
	return CachedTrade{
		Trader:     e.Trader,
		PairIndex:  int(e.Trade.PairIndex),
		TradeIndex: int(*e.TradeIndex),
		IsLong:     e.Trade.IsLong,
		Leverage:   int(e.Trade.Leverage),
		Collateral: bigStr(e.Trade.Collateral),
		OpenPrice:  bigStr(e.Trade.OpenPrice),
		TpPrice:    bigStr(e.Trade.TpPrice),
		SlPrice:    bigStr(e.Trade.SlPrice),
		LiqPrice:   bigStr(liqPrice),
		OpenedAt:   e.OccurredAt,
	}
}

func hasTrade(trades []CachedTrade, pairIdx, tradeIdx int) bool {
	for _, t := range trades {
		if t.PairIndex == pairIdx && t.TradeIndex == tradeIdx {
			return true
		}
	}
	return false
}

// removeTrade removes a trade from a trader's slice. Returns true if removed.
func removeTrade(trades map[string][]CachedTrade, trader string, pairIdx, tradeIdx int) bool {
	list := trades[trader]
	for i, t := range list {
		if t.PairIndex == pairIdx && t.TradeIndex == tradeIdx {
			trades[trader] = append(list[:i], list[i+1:]...)
			return true
		}
	}
	return false
}

// updateTradeTpSl updates tp_price and sl_price on a cached trade (matched by
// trader + pair_index + trade_index). Returns true if the trade was found and
// updated.
func updateTradeTpSl(trades map[string][]CachedTrade, trader string, pairIdx, tradeIdx int, tp, sl string) bool {
	list := trades[trader]
	for i, t := range list {
		if t.PairIndex == pairIdx && t.TradeIndex == tradeIdx {
			list[i].TpPrice = tp
			list[i].SlPrice = sl
			return true
		}
	}
	return false
}

func bigStr(b *big.Int) string {
	if b == nil {
		return "0"
	}
	return b.String()
}

func buildLimit(e *events.Event) CachedLimit {
	return CachedLimit{
		Trader:     e.Trader,
		PairIndex:  int(e.Limit.PairIndex),
		LimitIndex: int(*e.LimitIndex),
		IsLong:     e.Limit.IsLong,
		Leverage:   int(e.Limit.Leverage),
		Collateral: bigStr(e.Limit.Collateral),
		LimitPrice: bigStr(e.Limit.LimitPrice),
		TpPrice:    bigStr(e.Limit.TpPrice),
		SlPrice:    bigStr(e.Limit.SlPrice),
		PlacedAt:   e.OccurredAt,
	}
}

func hasLimit(limits []CachedLimit, pairIdx, limitIdx int) bool {
	for _, l := range limits {
		if l.PairIndex == pairIdx && l.LimitIndex == limitIdx {
			return true
		}
	}
	return false
}

func removeLimit(limits map[string][]CachedLimit, trader string, pairIdx, limitIdx int) bool {
	list := limits[trader]
	for i, l := range list {
		if l.PairIndex == pairIdx && l.LimitIndex == limitIdx {
			limits[trader] = append(list[:i], list[i+1:]...)
			return true
		}
	}
	return false
}

func updateLimit(limits map[string][]CachedLimit, trader string, pairIdx, limitIdx int, limitPrice, tp, sl string) bool {
	list := limits[trader]
	for i, l := range list {
		if l.PairIndex == pairIdx && l.LimitIndex == limitIdx {
			list[i].LimitPrice = limitPrice
			list[i].TpPrice = tp
			list[i].SlPrice = sl
			return true
		}
	}
	return false
}
