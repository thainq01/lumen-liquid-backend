package keeper

import (
	"math/big"
	"testing"
)

func TestLiquidationPrice_LongMatchesKeeperBotExample(t *testing.T) {
	// Mirrors the example in keeper_bot.js:
	//   open=614717920722247 (~61,471 at 1e10)
	//   collateral=20000000 (2 USDC at 1e7), leverage=5, liq_p=90, long
	open := big.NewInt(614717920722247)
	collat := big.NewInt(20000000)
	got, err := LiquidationPrice(open, true, collat, 5, nil, nil, 90)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	// Reproduce the Rust algorithm step by step (separate floors per step):
	//   collThresh = floor(collat * 90 / 100) = 18_000_000
	//   numer      = 18_000_000
	//   stage      = floor(open * numer / collat) = floor(open * 9 / 10)
	//   distance   = floor(stage / leverage)
	//   liq_long   = open - distance
	collThresh := new(big.Int).Quo(new(big.Int).Mul(collat, big.NewInt(90)), big.NewInt(100))
	stage := new(big.Int).Quo(new(big.Int).Mul(open, collThresh), collat)
	distance := new(big.Int).Quo(stage, big.NewInt(5))
	want := new(big.Int).Sub(open, distance)
	if got.Cmp(want) != 0 {
		t.Fatalf("long liq mismatch: got=%s want=%s", got, want)
	}
}

func TestLiquidationPrice_ShortMirror(t *testing.T) {
	open := big.NewInt(614717920722247)
	collat := big.NewInt(20000000)
	gotLong, _ := LiquidationPrice(open, true, collat, 5, nil, nil, 90)
	gotShort, _ := LiquidationPrice(open, false, collat, 5, nil, nil, 90)
	// short = open + distance, long = open - distance, so short - open == open - long
	leftDist := new(big.Int).Sub(gotShort, open)
	rightDist := new(big.Int).Sub(open, gotLong)
	if leftDist.Cmp(rightDist) != 0 {
		t.Fatalf("short/long not symmetric: short-open=%s open-long=%s", leftDist, rightDist)
	}
}

func TestLiquidationPrice_ZeroLeverageErrors(t *testing.T) {
	if _, err := LiquidationPrice(big.NewInt(1), true, big.NewInt(1), 0, nil, nil, 90); err != ErrDivByZero {
		t.Fatalf("want ErrDivByZero, got %v", err)
	}
}

func TestLiquidationPrice_FloorAtZero(t *testing.T) {
	// Pick parameters where distance ≥ open: open small, leverage 1, threshold 100
	// → collThresh = collat, numer = collat, stage = open, distance = open → long liq = 0
	open := big.NewInt(100)
	got, err := LiquidationPrice(open, true, big.NewInt(50), 1, nil, nil, 100)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got.Sign() != 0 {
		t.Fatalf("want 0 floor, got %s", got)
	}
}

func TestIsLiquidatable_Long(t *testing.T) {
	if !IsLiquidatable(big.NewInt(99), big.NewInt(100), true) {
		t.Fatal("want liquidatable when observed below liq for long")
	}
	if IsLiquidatable(big.NewInt(101), big.NewInt(100), true) {
		t.Fatal("want NOT liquidatable when observed above liq for long")
	}
}

func TestIsLiquidatable_Short(t *testing.T) {
	if !IsLiquidatable(big.NewInt(101), big.NewInt(100), false) {
		t.Fatal("want liquidatable when observed above liq for short")
	}
	if IsLiquidatable(big.NewInt(99), big.NewInt(100), false) {
		t.Fatal("want NOT liquidatable when observed below liq for short")
	}
}

func TestIsLiquidatable_LiqZeroAlwaysFalse(t *testing.T) {
	if IsLiquidatable(big.NewInt(50), big.NewInt(0), true) {
		t.Fatal("liq==0 must mean not liquidatable")
	}
}
