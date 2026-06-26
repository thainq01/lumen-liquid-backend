package keeper

import "math/big"

type ActionType string

const (
	ActionLiquidate ActionType = "liquidate"
	ActionTpSl      ActionType = "tp_sl"
	ActionLimit     ActionType = "limit"
)

type PendingAction struct {
	Type   ActionType
	Key    TradeKey
	IsLong bool
	// LimitIndex is set only for ActionLimit; Key.TradeIndex is unused there.
	LimitIndex int
}

// Detect scans all trades for a pair against the current price and returns
// actions that should be queued for execution.
func Detect(pairIndex int, price *big.Int, state *TradeState) []PendingAction {
	trades := state.GetTradesForPair(pairIndex)
	var actions []PendingAction

	for _, t := range trades {
		if IsLiquidatable(price, t.LiqPrice, t.IsLong) {
			actions = append(actions, PendingAction{
				Type:   ActionLiquidate,
				Key:    t.Key,
				IsLong: t.IsLong,
			})
			continue
		}

		if checkTpSl(price, t.TpPrice, t.SlPrice, t.IsLong) {
			actions = append(actions, PendingAction{
				Type:   ActionTpSl,
				Key:    t.Key,
				IsLong: t.IsLong,
			})
		}
	}

	for _, l := range state.GetLimitsForPair(pairIndex) {
		if checkLimit(price, l.LimitPrice, l.IsLong) {
			actions = append(actions, PendingAction{
				Type:       ActionLimit,
				Key:        TradeKey{Trader: l.Key.Trader, PairIndex: l.Key.PairIndex},
				IsLong:     l.IsLong,
				LimitIndex: l.Key.LimitIndex,
			})
		}
	}
	return actions
}

// checkLimit mirrors the contract gate in execute_limit_order: a long fills
// when price drops to/below the limit, a short fills when price rises to/above.
func checkLimit(price, limit *big.Int, isLong bool) bool {
	if limit == nil || limit.Cmp(zero_) <= 0 || price == nil {
		return false
	}
	if isLong {
		return price.Cmp(limit) <= 0
	}
	return price.Cmp(limit) >= 0
}

var zero_ = big.NewInt(0)

func checkTpSl(price, tp, sl *big.Int, isLong bool) bool {
	if isLong {
		if tp != nil && tp.Cmp(zero_) > 0 && price.Cmp(tp) >= 0 {
			return true
		}
		if sl != nil && sl.Cmp(zero_) > 0 && price.Cmp(sl) <= 0 {
			return true
		}
	} else {
		if tp != nil && tp.Cmp(zero_) > 0 && price.Cmp(tp) <= 0 {
			return true
		}
		if sl != nil && sl.Cmp(zero_) > 0 && price.Cmp(sl) >= 0 {
			return true
		}
	}
	return false
}
