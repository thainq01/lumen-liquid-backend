package events

import (
	"math/big"
	"testing"

	"github.com/stellar/go/xdr"
)

// dummyScVal is the raw ScVal passed to decodePM; the limit-order code paths
// read from the native `data` arg, not raw, so a zero value is fine.
func dummyScVal() xdr.ScVal { return xdr.ScVal{} }

// limitMap mirrors the ScMap-native form the SDK produces for a LimitOrder
// struct: map[string]any keyed by the contract field Symbol names.
func limitMap() map[string]any {
	return map[string]any{
		"pair_index":  int32(0),
		"is_long":     true,
		"collateral":  big.NewInt(1000000000),
		"leverage":    int32(10),
		"limit_price": big.NewInt(600000000000000),
		"tp_price":    big.NewInt(700000000000000),
		"sl_price":    big.NewInt(580000000000000),
	}
}

func TestDecodePM_Placed(t *testing.T) {
	e := &Event{Source: SrcPM}
	topics := []any{"placed", "GTRADER", int32(0)}
	data := []any{int32(2), limitMap()}

	decodePM(e, "placed", topics, data, dummyScVal())

	if e.Trader != "GTRADER" {
		t.Fatalf("trader: got %q", e.Trader)
	}
	if e.PairIndex == nil || *e.PairIndex != 0 {
		t.Fatalf("pair_index: got %v", e.PairIndex)
	}
	if e.LimitIndex == nil || *e.LimitIndex != 2 {
		t.Fatalf("limit_index: got %v", e.LimitIndex)
	}
	if e.Limit == nil {
		t.Fatal("limit struct not decoded")
	}
	if !e.Limit.IsLong || e.Limit.Leverage != 10 {
		t.Fatalf("limit fields: %+v", e.Limit)
	}
	if e.Limit.LimitPrice.Cmp(big.NewInt(600000000000000)) != 0 {
		t.Fatalf("limit_price: got %v", e.Limit.LimitPrice)
	}
	if e.Limit.Collateral.Cmp(big.NewInt(1000000000)) != 0 {
		t.Fatalf("collateral: got %v", e.Limit.Collateral)
	}
}

func TestDecodePM_PlacedBackwardCompat(t *testing.T) {
	// Old events carried only limit_index (bare u32).
	e := &Event{Source: SrcPM}
	decodePM(e, "placed", []any{"placed", "G", int32(0)}, int32(5), dummyScVal())
	if e.LimitIndex == nil || *e.LimitIndex != 5 {
		t.Fatalf("backward-compat limit_index: got %v", e.LimitIndex)
	}
	if e.Limit != nil {
		t.Fatal("expected nil limit struct for legacy event")
	}
}

func TestDecodePM_Executed(t *testing.T) {
	// executed now carries limit_index (the consumed order), not trade_index.
	e := &Event{Source: SrcPM}
	decodePM(e, "executed", []any{"executed", "G", int32(0)}, int32(2), dummyScVal())
	if e.LimitIndex == nil || *e.LimitIndex != 2 {
		t.Fatalf("executed limit_index: got %v", e.LimitIndex)
	}
}

func TestDecodePM_UpdatedLimit(t *testing.T) {
	e := &Event{Source: SrcPM}
	data := []any{int32(1), big.NewInt(610000000000000), big.NewInt(720000000000000), big.NewInt(590000000000000)}
	decodePM(e, "updated_limit", []any{"updated_limit", "G", int32(0)}, data, dummyScVal())

	if e.LimitIndex == nil || *e.LimitIndex != 1 {
		t.Fatalf("limit_index: got %v", e.LimitIndex)
	}
	if e.LimitPrice == nil || e.LimitPrice.Cmp(big.NewInt(610000000000000)) != 0 {
		t.Fatalf("limit_price: got %v", e.LimitPrice)
	}
	if e.TpPrice == nil || e.TpPrice.Cmp(big.NewInt(720000000000000)) != 0 {
		t.Fatalf("tp_price: got %v", e.TpPrice)
	}
	if e.SlPrice == nil || e.SlPrice.Cmp(big.NewInt(590000000000000)) != 0 {
		t.Fatalf("sl_price: got %v", e.SlPrice)
	}
}
