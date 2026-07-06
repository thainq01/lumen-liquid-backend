// Package events decodes Soroban event payloads emitted by the LumenLiquid
// contracts (PositionManager, Vault, PairRegistry).
//
// Layout convention used by the contracts (see ../../../lumenliquid-contracts):
//   - topics[0] is always a Symbol naming the event (`opened`, `closed`, ...)
//   - topics[1..] carry routing keys (trader Address, pair_index, group_index)
//   - the value carries the payload, often a 2-tuple or struct
//
// The decoder yields a typed Event union the indexer can switch on.
package events

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/lumenliquid/backend/internal/soroban"
	"github.com/stellar/go/xdr"
)

// Source identifies which contract emitted the event.
type Source string

const (
	SrcPM       Source = "pm"
	SrcVault    Source = "vault"
	SrcRegistry Source = "registry"
	SrcUnknown  Source = "unknown"
)

// Trade mirrors PM types.rs::Trade — 9 fields in declared order.
type Trade struct {
	PairIndex       uint32   `json:"pair_index"`
	IsLong          bool     `json:"is_long"`
	Leverage        uint32   `json:"leverage"`
	OpenPrice       *big.Int `json:"open_price"`
	Collateral      *big.Int `json:"collateral"`
	AccRolloverOpen *big.Int `json:"acc_rollover_open"`
	AccFundingOpen  *big.Int `json:"acc_funding_open"`
	TpPrice         *big.Int `json:"tp_price"`
	SlPrice         *big.Int `json:"sl_price"`
}

// LimitOrder mirrors PM types.rs::LimitOrder — 7 fields in declared order.
type LimitOrder struct {
	PairIndex  uint32   `json:"pair_index"`
	IsLong     bool     `json:"is_long"`
	Collateral *big.Int `json:"collateral"`
	Leverage   uint32   `json:"leverage"`
	LimitPrice *big.Int `json:"limit_price"`
	TpPrice    *big.Int `json:"tp_price"`
	SlPrice    *big.Int `json:"sl_price"`
}

// Event is the decoded form of a Soroban contract event. RawData carries the
// full ScVal-converted payload as JSON-friendly types so unknown topics still
// land in trade_events for forensic review.
type Event struct {
	Source     Source `json:"source"`
	Topic      string `json:"topic"`
	ContractID string `json:"contract_id"`
	TxHash     string `json:"tx_hash"`
	EventIndex uint32 `json:"event_index"`
	Ledger     uint64 `json:"ledger"`
	OccurredAt string `json:"occurred_at"`

	// Routing keys decoded from topic[1..], populated when present.
	Trader     string `json:"trader,omitempty"`
	PairIndex  *int32 `json:"pair_index,omitempty"`
	TradeIndex *int32 `json:"trade_index,omitempty"`
	LimitIndex *int32 `json:"limit_index,omitempty"`
	GroupIndex *int32 `json:"group_index,omitempty"`
	OIIsLong   *bool  `json:"oi_is_long,omitempty"`

	// Decoded payload (typed for known topics, raw for unknown).
	Trade       *Trade      `json:"trade,omitempty"`
	Limit       *LimitOrder `json:"limit,omitempty"`
	Assets      *big.Int    `json:"assets,omitempty"`
	Shares      *big.Int    `json:"shares,omitempty"`
	NetPnl      *big.Int    `json:"net_pnl,omitempty"`
	GrossPayout *big.Int    `json:"gross_payout,omitempty"`
	Amount      *big.Int    `json:"amount,omitempty"`
	RatePer     *big.Int    `json:"rate_p,omitempty"`
	FeePer      *big.Int    `json:"fee_p,omitempty"`
	Symbol      string      `json:"symbol,omitempty"`

	// PM close/open financial fields (enriched events).
	OpenFee    *big.Int `json:"open_fee,omitempty"`
	CloseFee   *big.Int `json:"close_fee,omitempty"`
	ClosePrice *big.Int `json:"close_price,omitempty"`

	// New tp/sl values carried by the updated_tp_sl event.
	TpPrice *big.Int `json:"tp_price,omitempty"`
	SlPrice *big.Int `json:"sl_price,omitempty"`

	// Limit price carried by updated_limit.
	LimitPrice *big.Int `json:"limit_price,omitempty"`

	RawTopics []any `json:"raw_topics"`
	RawData   any   `json:"raw_data"`
}

// pmTopics / vaultTopics / registryTopics let us classify by the Symbol topic[0].
var pmTopics = map[string]bool{
	"opened":         true,
	"closed":         true,
	"placed":         true,
	"executed":       true,
	"canceled":       true,
	"updated_limit":  true,
	"updated_tp_sl":  true,
	"tp_sl_executed": true,
	"liq":            true,
}

var vaultTopics = map[string]bool{
	"deposit":     true,
	"withdraw":    true,
	"settle":      true,
	"take_collat": true,
	"bad_debt":    true,
	"transfer":    true,
	"burn":        true,
	"approve":     true,
	"set_lock":    true,
	"set_pm":      true,
	"paused":      true,
	"unpaused":    true,
	"upgraded":    true,
	"init":        true,
}

var registryTopics = map[string]bool{
	"pair_added":    true,
	"pair_updated":  true,
	"pair_disabled": true,
	"funding_rate":  true,
	"rollover_rate": true,
	"depth":         true,
	"group_added":   true,
	"group_updated": true,
	"open_fee":      true,
	"close_fee":     true,
	"max_pos":       true,
	"oi_add":        true,
	"oi_sub":        true,
}

// Decode converts an RPC EventResult into a typed Event. `pmID`, `vaultID`,
// `registryID` let the caller classify by contract; pass empty strings to
// skip a route and rely solely on topic-name fallback.
func Decode(r soroban.EventResult, pmID, vaultID, registryID string) (Event, error) {
	out := Event{
		ContractID: r.ContractID,
		TxHash:     r.TxHash,
		Ledger:     uint64(r.Ledger),
		OccurredAt: r.LedgerClosedAt,
	}

	topics := make([]any, 0, len(r.Topic))
	for _, t := range r.Topic {
		v, err := soroban.DecodeScValB64(t)
		if err != nil {
			return out, fmt.Errorf("decode topic: %w", err)
		}
		nv, err := soroban.Native(v)
		if err != nil {
			return out, fmt.Errorf("native topic: %w", err)
		}
		topics = append(topics, nv)
	}
	out.RawTopics = topics

	if len(topics) == 0 {
		return out, fmt.Errorf("event has no topics")
	}
	topic, ok := topics[0].(string)
	if !ok {
		return out, fmt.Errorf("topic[0] is not a symbol: %T", topics[0])
	}
	out.Topic = topic

	switch {
	case r.ContractID == pmID || (pmID == "" && pmTopics[topic]):
		out.Source = SrcPM
	case r.ContractID == vaultID || (vaultID == "" && vaultTopics[topic]):
		out.Source = SrcVault
	case r.ContractID == registryID || (registryID == "" && registryTopics[topic]):
		out.Source = SrcRegistry
	default:
		out.Source = SrcUnknown
	}

	val, err := soroban.DecodeScValB64(r.Value)
	if err != nil {
		return out, fmt.Errorf("decode value: %w", err)
	}
	data, err := soroban.Native(val)
	if err != nil {
		return out, fmt.Errorf("native value: %w", err)
	}
	out.RawData = data

	switch out.Source {
	case SrcPM:
		decodePM(&out, topic, topics, data, val)
	case SrcVault:
		decodeVault(&out, topic, topics, data)
	case SrcRegistry:
		decodeRegistry(&out, topic, topics, data)
	}

	return out, nil
}

// ── PM ─────────────────────────────────────────────────────────────────────

func decodePM(e *Event, topic string, topics []any, data any, raw xdr.ScVal) {
	// PM events: topics = (Symbol, trader Address [, pair_index])
	if len(topics) > 1 {
		if s, ok := topics[1].(string); ok {
			e.Trader = s
		}
	}
	// Pair_index is now topics[2] on close/liq/tp_sl_executed/updated_tp_sl
	if len(topics) > 2 {
		if i, ok := toInt32(topics[2]); ok {
			e.PairIndex = ptrI32(i)
		}
	}
	switch topic {
	case "opened":
		// data = (trade_index:u32, Trade-struct, open_fee:i128)
		if v, ok := data.([]any); ok && len(v) >= 2 {
			if i, ok := toInt32(v[0]); ok {
				e.TradeIndex = ptrI32(i)
			}
			e.Trade = decodeTradeStruct(v[1])
			if len(v) >= 3 {
				e.OpenFee = toBig(v[2])
			}
		}
	case "closed", "tp_sl_executed", "liq":
		// data = (trade_index:u32, close_price:i128, close_fee:i128,
		//         net_pnl:i128, gross_payout:i128)
		if v, ok := data.([]any); ok && len(v) >= 1 {
			if i, ok := toInt32(v[0]); ok {
				e.TradeIndex = ptrI32(i)
			}
			if len(v) >= 5 {
				e.ClosePrice = toBig(v[1])
				e.CloseFee = toBig(v[2])
				e.NetPnl = toBig(v[3])
				e.GrossPayout = toBig(v[4])
			}
		} else if i, ok := toInt32(data); ok {
			// Backwards-compat: old events carried only trade_index.
			e.TradeIndex = ptrI32(i)
		}
	case "updated_tp_sl":
		// data = (trade_index:u32, tp_price:i128, sl_price:i128)
		if v, ok := data.([]any); ok && len(v) >= 1 {
			if i, ok := toInt32(v[0]); ok {
				e.TradeIndex = ptrI32(i)
			}
			if len(v) >= 3 {
				e.TpPrice = toBig(v[1])
				e.SlPrice = toBig(v[2])
			}
		} else if i, ok := toInt32(data); ok {
			// Backwards-compat: old events carried only trade_index.
			e.TradeIndex = ptrI32(i)
		}
	case "executed":
		// data = limit_index:u32 (the consumed limit order). The opened position
		// arrives separately as an `opened` event emitted in the same call.
		if i, ok := toInt32(data); ok {
			e.LimitIndex = ptrI32(i)
		}
	case "placed":
		// data = (limit_index:u32, LimitOrder-struct)
		if v, ok := data.([]any); ok && len(v) >= 1 {
			if i, ok := toInt32(v[0]); ok {
				e.LimitIndex = ptrI32(i)
			}
			if len(v) >= 2 {
				e.Limit = decodeLimitStruct(v[1])
			}
		} else if i, ok := toInt32(data); ok {
			// Backwards-compat: old events carried only limit_index.
			e.LimitIndex = ptrI32(i)
		}
	case "updated_limit":
		// data = (limit_index:u32, limit_price:i128, tp_price:i128, sl_price:i128)
		if v, ok := data.([]any); ok && len(v) >= 1 {
			if i, ok := toInt32(v[0]); ok {
				e.LimitIndex = ptrI32(i)
			}
			if len(v) >= 4 {
				e.LimitPrice = toBig(v[1])
				e.TpPrice = toBig(v[2])
				e.SlPrice = toBig(v[3])
			}
		} else if i, ok := toInt32(data); ok {
			// Backwards-compat: old events carried only limit_index.
			e.LimitIndex = ptrI32(i)
		}
	case "canceled":
		if i, ok := toInt32(data); ok {
			e.LimitIndex = ptrI32(i)
		}
	}
}

// ── Vault ──────────────────────────────────────────────────────────────────

func decodeVault(e *Event, topic string, topics []any, data any) {
	switch topic {
	case "deposit", "withdraw":
		// topics: (Symbol, owner_or_from, receiver). data: (assets, shares)
		if len(topics) > 1 {
			if s, ok := topics[1].(string); ok {
				e.Trader = s
			}
		}
		if v, ok := data.([]any); ok && len(v) >= 2 {
			e.Assets = toBig(v[0])
			e.Shares = toBig(v[1])
		}
	case "settle":
		// topics: (Symbol, trader). data: (eff_collat, net_pnl, gross_payout)
		if len(topics) > 1 {
			if s, ok := topics[1].(string); ok {
				e.Trader = s
			}
		}
		if v, ok := data.([]any); ok && len(v) >= 3 {
			e.Assets = toBig(v[0])
			e.NetPnl = toBig(v[1])
			e.GrossPayout = toBig(v[2])
		}
	case "bad_debt":
		// topics: (Symbol, pair_index). data: amount
		if len(topics) > 1 {
			if i, ok := toInt32(topics[1]); ok {
				e.PairIndex = ptrI32(i)
			}
		}
		e.Amount = toBig(data)
	case "take_collat":
		// data: (pm:Address, amount)
		if v, ok := data.([]any); ok && len(v) >= 2 {
			e.Amount = toBig(v[1])
		}
	case "transfer", "burn", "approve":
		// SEP-41 surface — captured as raw, no typed projection.
	}
}

// ── Registry ───────────────────────────────────────────────────────────────

func decodeRegistry(e *Event, topic string, topics []any, data any) {
	// topics: (Symbol, index)  for most pair/group events
	if len(topics) > 1 {
		if i, ok := toInt32(topics[1]); ok {
			switch topic {
			case "pair_added", "pair_updated", "pair_disabled",
				"funding_rate", "rollover_rate", "depth":
				e.PairIndex = ptrI32(i)
			case "group_added", "group_updated", "open_fee", "close_fee":
				e.GroupIndex = ptrI32(i)
			case "oi_add", "oi_sub":
				e.PairIndex = ptrI32(i)
			}
		}
	}
	if (topic == "oi_add" || topic == "oi_sub") && len(topics) > 2 {
		if b, ok := topics[2].(bool); ok {
			e.OIIsLong = &b
		}
	}
	switch topic {
	case "pair_added":
		if s, ok := data.(string); ok {
			e.Symbol = s
		}
	case "group_added":
		if s, ok := data.(string); ok {
			e.Symbol = s
		}
	case "funding_rate", "rollover_rate":
		e.RatePer = toBig(data)
	case "open_fee", "close_fee":
		e.FeePer = toBig(data)
	case "depth", "max_pos":
		e.Amount = toBig(data)
	case "oi_add", "oi_sub":
		e.Amount = toBig(data)
	}
}

// ── helpers ────────────────────────────────────────────────────────────────

// decodeTradeStruct turns the ScVal-native form of `Trade` into a typed Trade.
// The contract emits structs as ScMap with field names as Symbol keys (via
// #[contracttype]); soroban.Native turns those into map[string]any.
func decodeTradeStruct(v any) *Trade {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	t := &Trade{}
	if x, ok := toInt32(m["pair_index"]); ok {
		t.PairIndex = uint32(x)
	}
	if b, ok := m["is_long"].(bool); ok {
		t.IsLong = b
	}
	if x, ok := toInt32(m["leverage"]); ok {
		t.Leverage = uint32(x)
	}
	t.OpenPrice = toBig(m["open_price"])
	t.Collateral = toBig(m["collateral"])
	t.AccRolloverOpen = toBig(m["acc_rollover_open"])
	t.AccFundingOpen = toBig(m["acc_funding_open"])
	t.TpPrice = toBig(m["tp_price"])
	t.SlPrice = toBig(m["sl_price"])
	return t
}

// decodeLimitStruct turns the ScVal-native form of `LimitOrder` into a typed
// LimitOrder. Same ScMap-with-Symbol-keys shape as decodeTradeStruct.
func decodeLimitStruct(v any) *LimitOrder {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	o := &LimitOrder{}
	if x, ok := toInt32(m["pair_index"]); ok {
		o.PairIndex = uint32(x)
	}
	if b, ok := m["is_long"].(bool); ok {
		o.IsLong = b
	}
	if x, ok := toInt32(m["leverage"]); ok {
		o.Leverage = uint32(x)
	}
	o.Collateral = toBig(m["collateral"])
	o.LimitPrice = toBig(m["limit_price"])
	o.TpPrice = toBig(m["tp_price"])
	o.SlPrice = toBig(m["sl_price"])
	return o
}

func toBig(v any) *big.Int {
	switch x := v.(type) {
	case *big.Int:
		return new(big.Int).Set(x)
	case int32:
		return big.NewInt(int64(x))
	case int64:
		return big.NewInt(x)
	case uint32:
		return big.NewInt(int64(x))
	case uint64:
		return new(big.Int).SetUint64(x)
	case json.Number:
		i := new(big.Int)
		_, _ = i.SetString(x.String(), 10)
		return i
	}
	return nil
}

func toInt32(v any) (int32, bool) {
	switch x := v.(type) {
	case uint32:
		return int32(x), true
	case int32:
		return x, true
	case int64:
		return int32(x), true
	case uint64:
		return int32(x), true
	case *big.Int:
		return int32(x.Int64()), true
	}
	return 0, false
}

func ptrI32(v int32) *int32 { return &v }
