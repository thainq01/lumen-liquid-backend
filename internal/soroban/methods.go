package soroban

import "context"

// ── getLatestLedger ────────────────────────────────────────

type GetLatestLedgerResp struct {
	ID              string `json:"id"`
	ProtocolVersion uint32 `json:"protocolVersion"`
	Sequence        uint32 `json:"sequence"`
}

func (c *Client) GetLatestLedger(ctx context.Context) (*GetLatestLedgerResp, error) {
	var out GetLatestLedgerResp
	if err := c.Call(ctx, "getLatestLedger", struct{}{}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ── getHealth ──────────────────────────────────────────────

type GetHealthResp struct {
	Status                string `json:"status"`
	LatestLedger          uint32 `json:"latestLedger"`
	OldestLedger          uint32 `json:"oldestLedger"`
	LedgerRetentionWindow uint32 `json:"ledgerRetentionWindow"`
}

func (c *Client) GetHealth(ctx context.Context) (*GetHealthResp, error) {
	var out GetHealthResp
	if err := c.Call(ctx, "getHealth", struct{}{}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ── getEvents ──────────────────────────────────────────────

type EventFilterType string

const (
	EventFilterContract EventFilterType = "contract"
	EventFilterSystem   EventFilterType = "system"
	EventFilterDiag     EventFilterType = "diagnostic"
)

type EventFilter struct {
	Type        EventFilterType `json:"type"`
	ContractIDs []string        `json:"contractIds,omitempty"`
	Topics      [][]string      `json:"topics,omitempty"`
}

type Pagination struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type GetEventsParams struct {
	StartLedger uint32        `json:"startLedger,omitempty"`
	Filters     []EventFilter `json:"filters"`
	Pagination  *Pagination   `json:"pagination,omitempty"`
}

// EventResult mirrors Soroban RPC's getEvents response entry.
// Topic and Value are XDR-encoded base64 strings (ScVal); decode via internal/soroban/scval.go.
type EventResult struct {
	Type                     string   `json:"type"`
	Ledger                   uint32   `json:"ledger"`
	LedgerClosedAt           string   `json:"ledgerClosedAt"`
	ContractID               string   `json:"contractId"`
	ID                       string   `json:"id"` // PagingToken-style
	PagingToken              string   `json:"pagingToken"`
	InSuccessfulContractCall bool     `json:"inSuccessfulContractCall"`
	Topic                    []string `json:"topic"`
	Value                    string   `json:"value"`
	TxHash                   string   `json:"txHash"`
}

type GetEventsResp struct {
	Events       []EventResult `json:"events"`
	LatestLedger uint32        `json:"latestLedger"`
	Cursor       string        `json:"cursor"`
}

func (c *Client) GetEvents(ctx context.Context, p GetEventsParams) (*GetEventsResp, error) {
	var out GetEventsResp
	if err := c.Call(ctx, "getEvents", p, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
