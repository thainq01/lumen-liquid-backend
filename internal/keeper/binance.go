package keeper

import (
	"context"
	"encoding/json"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"
)

type PriceTick struct {
	PairIndex int
	Price     *big.Int
	Symbol    string
}

type BinanceClient struct {
	url          string
	symbols      []string
	symbolToPair map[string]int
	out          chan PriceTick
	logger       zerolog.Logger
}

func NewBinanceClient(wsURL string, pairSymbolMap string, logger zerolog.Logger) *BinanceClient {
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

	return &BinanceClient{
		url:          wsURL,
		symbols:      symbols,
		symbolToPair: symbolToPair,
		out:          make(chan PriceTick, 1024),
		logger:       logger,
	}
}

func (b *BinanceClient) Prices() <-chan PriceTick {
	return b.out
}

func (b *BinanceClient) Run(ctx context.Context) {
	var backoff time.Duration
	for {
		if err := b.connect(ctx); err != nil {
			if ctx.Err() != nil {
				close(b.out)
				return
			}
			backoff = nextBackoff(backoff)
			b.logger.Warn().Err(err).Dur("backoff", backoff).Msg("binance: reconnecting")
			select {
			case <-ctx.Done():
				close(b.out)
				return
			case <-time.After(backoff):
			}
		} else {
			backoff = 0
		}
	}
}

func (b *BinanceClient) connect(ctx context.Context) error {
	conn, _, err := websocket.Dial(ctx, b.url, nil)
	if err != nil {
		return err
	}
	defer conn.CloseNow()

	conn.SetReadLimit(32 * 1024)

	subMsg, _ := json.Marshal(map[string]any{
		"method": "SUBSCRIBE",
		"params": b.symbols,
		"id":     1,
	})
	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	err = conn.Write(writeCtx, websocket.MessageText, subMsg)
	cancel()
	if err != nil {
		return err
	}
	b.logger.Info().Strs("streams", b.symbols).Msg("binance: subscribed")

	// Per-read deadline: Binance futures streams trade constantly, so a gap
	// longer than readTimeout means the connection is dead — reconnect.
	const readTimeout = 15 * time.Second
	for {
		readCtx, cancel := context.WithTimeout(ctx, readTimeout)
		_, data, err := conn.Read(readCtx)
		cancel()
		if err != nil {
			return err
		}
		b.handleMessage(data)
	}
}

// aggTradeMsg decodes Binance aggTrade. The EventTime field (json:"E") MUST be
// present: Go's json matching is case-insensitive, so without an exact "E"
// field the numeric "E" (event time) collides with Event (json:"e") and fails
// the whole unmarshal. Declaring "E" gives each key an exact match.
type aggTradeMsg struct {
	Event     string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Price     string `json:"p"`
}

// streamWrapper handles the /stream combined endpoint envelope format:
// {"stream":"btcusdt@aggTrade","data":{...actual aggTrade msg...}}
type streamWrapper struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

func (b *BinanceClient) handleMessage(data []byte) {
	var raw json.RawMessage = data

	// If using /stream endpoint, messages arrive wrapped in {"stream":...,"data":...}
	var wrapper streamWrapper
	if err := json.Unmarshal(data, &wrapper); err == nil && wrapper.Stream != "" {
		raw = wrapper.Data
	}

	var msg aggTradeMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		return
	}
	if msg.Event != "aggTrade" {
		return
	}

	pairIdx, ok := b.symbolToPair[msg.Symbol]
	if !ok {
		return
	}

	price := parsePriceToScale(msg.Price)
	if price == nil {
		return
	}

	select {
	case b.out <- PriceTick{PairIndex: pairIdx, Price: price, Symbol: msg.Symbol}:
	default:
		b.logger.Warn().Msg("binance: price channel full, dropping tick")
	}
}

// parsePriceToScale converts a decimal string price (e.g. "62738.10") to the
// Reflector oracle scale (1e14, decimals=14) as *big.Int. This must match the
// scale of the on-chain tp/sl/liq prices, which the contract compares directly
// against the Reflector price inside execute_tp_sl / liquidate_trade.
//
// Parsing is done on the decimal string directly (not via float64) to keep
// full precision at 1e14 magnitude, where float64 mantissa would round the
// low-order digits.
func parsePriceToScale(s string) *big.Int {
	const decimals = 14
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}

	intPart, fracPart, _ := strings.Cut(s, ".")
	if len(fracPart) > decimals {
		fracPart = fracPart[:decimals] // truncate excess precision
	}
	digits := intPart + fracPart + strings.Repeat("0", decimals-len(fracPart))

	v, ok := new(big.Int).SetString(digits, 10)
	if !ok || v.Sign() <= 0 {
		return nil
	}
	return v
}

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
