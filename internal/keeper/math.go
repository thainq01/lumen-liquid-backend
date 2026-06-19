// Package keeper holds the off-chain math that mirrors crates/math in the
// contracts repo. The indexer uses LiquidationPrice at the `opened` event to
// store a precomputed liq_price on each trade row; the keeper (Phase 2) uses
// the same code to decide whether to submit liquidate_trade.
//
// Numbers here are math/big.Int because i128 won't fit in int64. Scales:
//   - USDC_SCALE = 1e7 (collateral)
//   - PRICE_SCALE = 1e10 (open_price, returned liq_price)
//   - liq_threshold_p is integer percent (e.g. 90), NOT scaled
package keeper

import (
	"errors"
	"math/big"
)

var (
	ErrDivByZero = errors.New("math: division by zero")
	ErrOverflow  = errors.New("math: overflow")
	ErrInvalid   = errors.New("math: invalid input")
)

var (
	hundred = big.NewInt(100)
	zero    = big.NewInt(0)
)

// LiquidationPrice mirrors crates/math/src/liq.rs::liquidation_price.
//
//	collateral_threshold = collateral * liq_threshold_p / 100
//	numer_inner          = collateral_threshold - rollover_fee - funding_fee
//	distance             = open_price * numer_inner / collateral / leverage
//	liq_long             = open_price - distance
//	liq_short            = open_price + distance
//	(floored at 0)
//
// All arithmetic uses big.Int integer division (floor toward zero matches the
// Rust contract's i128 / divisor on positive operands; for our inputs the
// signs always make this safe).
func LiquidationPrice(
	openPrice *big.Int,
	isLong bool,
	collateral *big.Int,
	leverage uint32,
	rolloverFee *big.Int,
	fundingFee *big.Int,
	liqThresholdP uint32,
) (*big.Int, error) {
	if collateral == nil || collateral.Sign() == 0 {
		return nil, ErrDivByZero
	}
	if leverage == 0 {
		return nil, ErrDivByZero
	}
	if openPrice == nil {
		return nil, ErrInvalid
	}
	if rolloverFee == nil {
		rolloverFee = zero
	}
	if fundingFee == nil {
		fundingFee = zero
	}

	// collateral_threshold = collateral * liq_threshold_p / 100
	collThresh := new(big.Int).Mul(collateral, big.NewInt(int64(liqThresholdP)))
	collThresh.Quo(collThresh, hundred)

	// numer_inner = collateral_threshold - rollover - funding   (signed)
	numer := new(big.Int).Sub(collThresh, rolloverFee)
	numer.Sub(numer, fundingFee)

	// stage = open * numer / collateral
	stage := new(big.Int).Mul(openPrice, numer)
	stage.Quo(stage, collateral)

	// distance = stage / leverage
	distance := new(big.Int).Quo(stage, big.NewInt(int64(leverage)))

	liq := new(big.Int)
	if isLong {
		liq.Sub(openPrice, distance)
	} else {
		liq.Add(openPrice, distance)
	}
	if liq.Sign() < 0 {
		liq.SetInt64(0)
	}
	return liq, nil
}

// IsLiquidatable mirrors liq.rs::is_liquidatable.
func IsLiquidatable(observed, liq *big.Int, isLong bool) bool {
	if liq == nil || liq.Sign() == 0 || observed == nil {
		return false
	}
	if isLong {
		return observed.Cmp(liq) <= 0
	}
	return observed.Cmp(liq) >= 0
}
