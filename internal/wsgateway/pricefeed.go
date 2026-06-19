package wsgateway

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"
)

// PriceFeed connects to Binance Futures WS and broadcasts aggTrade prices
// directly to the Hub's "prices" channel with minimal latency.
type PriceFeed struct {
	hub          *Hub
	url          string
	symbols      []string
	symbolToPair map[string]int
	logger       zerolog.Logger
}

// NewPriceFeed creates a price feed that pushes Binance aggTrades to the hub.
func NewPriceFeed(hub *Hub, wsURL, pairSymbolMap string, logger zerolog.Logger) *PriceFeed {
	symbolToPair := make(map[string]int)
	var symbols []string

	for _, entry := range strings.Split(pairSymbolMap, ",") {
		parts := strings.SplitN(strings.TrimSpace(entry), ":", 2)
		if len(parts) != 2 {
			continue
		}
		symbol := strings.ToUpper(strings.TrimSpace(parts[0]))
		idx, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			continue
		}
		symbolToPair[symbol] = idx
		symbols = append(symbols, strings.ToLower(symbol)+"@aggTrade")
	}

	return &PriceFeed{
		hub:          hub,
		url:          wsURL,
		symbols:      symbols,
		symbolToPair: symbolToPair,
		logger:       logger,
	}
}

// Run connects to Binance and streams prices until ctx is canceled.
// Auto-reconnects on disconnect.
func (pf *PriceFeed) Run(ctx context.Context) {
	var backoff time.Duration
	for {
		if err := pf.connect(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			backoff = pf.nextBackoff(backoff)
			pf.logger.Warn().Err(err).Dur("backoff", backoff).Msg("pricefeed: reconnecting")
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
		} else {
			backoff = 0
		}
	}
}

func (pf *PriceFeed) connect(ctx context.Context) error {
	conn, _, err := websocket.Dial(ctx, pf.url, nil)
	if err != nil {
		return err
	}
	defer conn.CloseNow()

	conn.SetReadLimit(32 * 1024)

	subMsg, _ := json.Marshal(map[string]any{
		"method": "SUBSCRIBE",
		"params": pf.symbols,
		"id":     1,
	})
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = conn.Write(writeCtx, websocket.MessageText, subMsg)
	cancel()
	if err != nil {
		return err
	}
	pf.logger.Info().Strs("streams", pf.symbols).Msg("pricefeed: subscribed")

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		pf.handleMessage(data)
	}
}

type priceFeedAggTrade struct {
	Event     string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Price     string `json:"p"`
}

type priceFeedStreamWrapper struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

func (pf *PriceFeed) handleMessage(data []byte) {
	var raw json.RawMessage = data

	// Handle /stream combined endpoint wrapper
	var wrapper priceFeedStreamWrapper
	if err := json.Unmarshal(data, &wrapper); err == nil && wrapper.Stream != "" {
		raw = wrapper.Data
	}

	var msg priceFeedAggTrade
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if msg.Event != "aggTrade" {
		return
	}

	pairIdx, ok := pf.symbolToPair[msg.Symbol]
	if !ok {
		return
	}

	// Format: "0|62709.90"
	payload := fmt.Sprintf("%d|%s", pairIdx, msg.Price)

	select {
	case pf.hub.broadcast <- &Message{Channel: "prices", Payload: []byte(payload)}:
	default:
	}
}

func (pf *PriceFeed) nextBackoff(current time.Duration) time.Duration {
	if current == 0 {
		return 1 * time.Second
	}
	next := current * 2
	if next > 30*time.Second {
		return 30 * time.Second
	}
	return next
}
