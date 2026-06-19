package keeper

import "math/big"

type ActionType string

const (
	ActionLiquidate ActionType = "liquidate"
	ActionTpSl      ActionType = "tp_sl"
)

type PendingAction struct {
	Type       ActionType
	Key        TradeKey
	IsLong     bool
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
	return actions
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
