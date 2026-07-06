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
	hundred   = big.NewInt(100)
	zero      = big.NewInt(0)
	pScale    = big.NewInt(10_000_000_000)
	usdcScale = big.NewInt(10_000_000)
)

// PendingAccRollover mirrors crates/math/src/fees.rs::pending_acc_rollover.
func PendingAccRollover(accPerCollateral *big.Int, deltaLedgers uint64, rolloverFeePerLedgerP *big.Int) (*big.Int, error) {
	if accPerCollateral == nil {
		accPerCollateral = zero
	}
	if deltaLedgers == 0 || rolloverFeePerLedgerP == nil || rolloverFeePerLedgerP.Sign() == 0 {
		return new(big.Int).Set(accPerCollateral), nil
	}
	denom := new(big.Int).Mul(pScale, hundred)
	num := new(big.Int).Mul(rolloverFeePerLedgerP, usdcScale)
	inc := new(big.Int).Mul(big.NewInt(int64(deltaLedgers)), num)
	inc.Quo(inc, denom)
	return new(big.Int).Add(accPerCollateral, inc), nil
}

// PendingAccFunding mirrors crates/math/src/fees.rs::pending_acc_funding.
func PendingAccFunding(
	accLong, accShort, oiLong, oiShort *big.Int,
	deltaLedgers uint64,
	fundingFeePerLedgerP *big.Int,
) (*big.Int, *big.Int, error) {
	if accLong == nil {
		accLong = zero
	}
	if accShort == nil {
		accShort = zero
	}
	if oiLong == nil {
		oiLong = zero
	}
	if oiShort == nil {
		oiShort = zero
	}
	if deltaLedgers == 0 || fundingFeePerLedgerP == nil || fundingFeePerLedgerP.Sign() == 0 {
		return new(big.Int).Set(accLong), new(big.Int).Set(accShort), nil
	}

	oiDiff := new(big.Int).Sub(oiLong, oiShort)
	stage := new(big.Int).Mul(oiDiff, big.NewInt(int64(deltaLedgers)))
	denom := new(big.Int).Mul(pScale, hundred)
	paidByLongs := new(big.Int).Mul(stage, fundingFeePerLedgerP)
	paidByLongs.Quo(paidByLongs, denom)

	newLong := new(big.Int).Set(accLong)
	if oiLong.Sign() > 0 {
		inc := new(big.Int).Mul(paidByLongs, usdcScale)
		inc.Quo(inc, oiLong)
		newLong.Add(newLong, inc)
	}

	newShort := new(big.Int).Set(accShort)
	if oiShort.Sign() > 0 {
		inc := new(big.Int).Mul(paidByLongs, usdcScale)
		inc.Quo(inc, oiShort)
		newShort.Sub(newShort, inc)
	}
	return newLong, newShort, nil
}

// RolloverFeeForTrade mirrors crates/math/src/fees.rs::rollover_fee_for_trade.
func RolloverFeeForTrade(accOpen, accNow, collateral *big.Int) (*big.Int, error) {
	if accOpen == nil {
		accOpen = zero
	}
	if accNow == nil {
		accNow = zero
	}
	if collateral == nil {
		return nil, ErrInvalid
	}
	delta := new(big.Int).Sub(accNow, accOpen)
	if delta.Sign() == 0 {
		return big.NewInt(0), nil
	}
	fee := new(big.Int).Mul(delta, collateral)
	fee.Quo(fee, usdcScale)
	return fee, nil
}

// FundingFeeForTrade mirrors crates/math/src/fees.rs::funding_fee_for_trade.
func FundingFeeForTrade(accOpen, accNow, collateral *big.Int, leverage uint32) (*big.Int, error) {
	if accOpen == nil {
		accOpen = zero
	}
	if accNow == nil {
		accNow = zero
	}
	if collateral == nil {
		return nil, ErrInvalid
	}
	delta := new(big.Int).Sub(accNow, accOpen)
	if delta.Sign() == 0 {
		return big.NewInt(0), nil
	}
	staged := new(big.Int).Mul(delta, collateral)
	staged.Quo(staged, usdcScale)
	staged.Mul(staged, big.NewInt(int64(leverage)))
	return staged, nil
}

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
