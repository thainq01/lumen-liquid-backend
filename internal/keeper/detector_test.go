package keeper

import (
	"math/big"
	"testing"

	"github.com/rs/zerolog"
)

func bi(s string) *big.Int {
	v, _ := new(big.Int).SetString(s, 10)
	return v
}

func newTestState(entries ...*TradeEntry) *TradeState {
	s := NewTradeState(zerolog.Nop())
	for _, e := range entries {
		s.trades[e.Key] = e
	}
	return s
}

func TestDetect_LongLiquidation(t *testing.T) {
	// Long liquidates when price <= liq_price.
	entry := &TradeEntry{
		Key:      TradeKey{Trader: "G1", PairIndex: 0, TradeIndex: 1},
		IsLong:   true,
		LiqPrice: bi("600000000000000"), // 60,000 @ 1e10
		TpPrice:  big.NewInt(0),
		SlPrice:  big.NewInt(0),
	}
	state := newTestState(entry)

	// Above liq → no action.
	if got := Detect(0, bi("610000000000000"), state); len(got) != 0 {
		t.Fatalf("expected no action above liq, got %d", len(got))
	}
	// At/below liq → liquidate.
	got := Detect(0, bi("600000000000000"), state)
	if len(got) != 1 || got[0].Type != ActionLiquidate {
		t.Fatalf("expected liquidate at liq price, got %+v", got)
	}
}

func TestDetect_ShortLiquidation(t *testing.T) {
	// Short liquidates when price >= liq_price.
	entry := &TradeEntry{
		Key:      TradeKey{Trader: "G2", PairIndex: 1, TradeIndex: 1},
		IsLong:   false,
		LiqPrice: bi("700000000000000"),
		TpPrice:  big.NewInt(0),
		SlPrice:  big.NewInt(0),
	}
	state := newTestState(entry)

	if got := Detect(1, bi("690000000000000"), state); len(got) != 0 {
		t.Fatalf("expected no action below liq for short, got %d", len(got))
	}
	got := Detect(1, bi("700000000000001"), state)
	if len(got) != 1 || got[0].Type != ActionLiquidate {
		t.Fatalf("expected liquidate for short, got %+v", got)
	}
}

func TestDetect_LongTakeProfit(t *testing.T) {
	entry := &TradeEntry{
		Key:      TradeKey{Trader: "G3", PairIndex: 0, TradeIndex: 2},
		IsLong:   true,
		LiqPrice: bi("100000000000"),
		TpPrice:  bi("650000000000000"), // TP at 65,000
		SlPrice:  big.NewInt(0),
	}
	state := newTestState(entry)

	if got := Detect(0, bi("640000000000000"), state); len(got) != 0 {
		t.Fatalf("expected no action below TP, got %d", len(got))
	}
	got := Detect(0, bi("650000000000000"), state)
	if len(got) != 1 || got[0].Type != ActionTpSl {
		t.Fatalf("expected tp_sl at TP, got %+v", got)
	}
}

func TestDetect_LongStopLoss(t *testing.T) {
	entry := &TradeEntry{
		Key:      TradeKey{Trader: "G4", PairIndex: 0, TradeIndex: 3},
		IsLong:   true,
		LiqPrice: bi("100000000000"), // far away so SL fires first
		TpPrice:  big.NewInt(0),
		SlPrice:  bi("580000000000000"), // SL at 58,000
	}
	state := newTestState(entry)

	if got := Detect(0, bi("590000000000000"), state); len(got) != 0 {
		t.Fatalf("expected no action above SL, got %d", len(got))
	}
	got := Detect(0, bi("580000000000000"), state)
	if len(got) != 1 || got[0].Type != ActionTpSl {
		t.Fatalf("expected tp_sl at SL, got %+v", got)
	}
}

func TestDetect_LiquidationTakesPriority(t *testing.T) {
	// When both liq and SL would trigger, liquidation wins (checked first).
	entry := &TradeEntry{
		Key:      TradeKey{Trader: "G5", PairIndex: 0, TradeIndex: 4},
		IsLong:   true,
		LiqPrice: bi("600000000000000"),
		TpPrice:  big.NewInt(0),
		SlPrice:  bi("610000000000000"),
	}
	state := newTestState(entry)

	got := Detect(0, bi("590000000000000"), state)
	if len(got) != 1 || got[0].Type != ActionLiquidate {
		t.Fatalf("expected liquidate priority, got %+v", got)
	}
}

func TestDetect_ZeroTpSlIgnored(t *testing.T) {
	// tp/sl of 0 means unset — must never trigger.
	entry := &TradeEntry{
		Key:      TradeKey{Trader: "G6", PairIndex: 0, TradeIndex: 5},
		IsLong:   true,
		LiqPrice: bi("100000000000"),
		TpPrice:  big.NewInt(0),
		SlPrice:  big.NewInt(0),
	}
	state := newTestState(entry)

	if got := Detect(0, bi("650000000000000"), state); len(got) != 0 {
		t.Fatalf("expected no action with zero tp/sl, got %d", len(got))
	}
}

func TestParsePriceToScale(t *testing.T) {
	got := parsePriceToScale("62738.10")
	want := bi("6273810000000000000") // 62,738.10 × 1e14
	if got == nil || got.Cmp(want) != 0 {
		t.Fatalf("parsePriceToScale: got %v want %v", got, want)
	}
	if parsePriceToScale("not-a-number") != nil {
		t.Fatal("expected nil for invalid input")
	}
	if parsePriceToScale("0") != nil {
		t.Fatal("expected nil for zero price")
	}
}
