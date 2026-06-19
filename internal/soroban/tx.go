package soroban

import "context"

// ── simulateTransaction ───────────────────────────────────────

type SimulateParams struct {
	Transaction string `json:"transaction"`
}

type SimRestorePreamble struct {
	TransactionData string `json:"transactionData"`
	MinResourceFee  int64  `json:"minResourceFee"`
}

type SimResult struct {
	TransactionData string              `json:"transactionData"`
	MinResourceFee  string              `json:"minResourceFee"`
	Events          []string            `json:"events"`
	Results         []SimResultEntry    `json:"results"`
	Cost            *SimCost            `json:"cost"`
	RestorePreamble *SimRestorePreamble `json:"restorePreamble"`
	LatestLedger    uint32              `json:"latestLedger"`
	Error           string              `json:"error"`
}

type SimResultEntry struct {
	XDR  string   `json:"xdr"`
	Auth []string `json:"auth"`
}

type SimCost struct {
	CPUInsns string `json:"cpuInsns"`
	MemBytes string `json:"memBytes"`
}

func (s *SimResult) IsSuccess() bool {
	return s.Error == "" && s.TransactionData != ""
}

func (c *Client) SimulateTransaction(ctx context.Context, txXDR string) (*SimResult, error) {
	var out SimResult
	if err := c.Call(ctx, "simulateTransaction", SimulateParams{Transaction: txXDR}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ── sendTransaction ───────────────────────────────────────────

type SendResult struct {
	Status                string `json:"status"`
	Hash                  string `json:"hash"`
	LatestLedger          uint32 `json:"latestLedger"`
	LatestLedgerCloseTime string `json:"latestLedgerCloseTime"`
	ErrorResultXDR        string `json:"errorResultXdr"`
}

func (c *Client) SendTransaction(ctx context.Context, txXDR string) (*SendResult, error) {
	var out SendResult
	if err := c.Call(ctx, "sendTransaction", SimulateParams{Transaction: txXDR}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ── getTransaction ────────────────────────────────────────────

type GetTransactionParams struct {
	Hash string `json:"hash"`
}

type GetTransactionResult struct {
	Status          string `json:"status"`
	LatestLedger    uint32 `json:"latestLedger"`
	OldestLedger    uint32 `json:"oldestLedger"`
	ApplicationOrder int32 `json:"applicationOrder"`
	Ledger          uint32 `json:"ledger"`
	EnvelopeXDR     string `json:"envelopeXdr"`
	ResultXDR       string `json:"resultXdr"`
	ResultMetaXDR   string `json:"resultMetaXdr"`
}

func (r *GetTransactionResult) IsSuccess() bool {
	return r.Status == "SUCCESS"
}

func (r *GetTransactionResult) IsNotFound() bool {
	return r.Status == "NOT_FOUND"
}

func (c *Client) GetTransaction(ctx context.Context, hash string) (*GetTransactionResult, error) {
	var out GetTransactionResult
	if err := c.Call(ctx, "getTransaction", GetTransactionParams{Hash: hash}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ── getAccount (for sequence number) ──────────────────────────

type AccountParams struct {
	Address string `json:"address"`
}

type AccountEntry struct {
	ID       string `json:"id"`
	Sequence int64  `json:"sequence"`
}

type GetLedgerEntriesParams struct {
	Keys []string `json:"keys"`
}

type LedgerEntry struct {
	Key            string `json:"key"`
	XDR            string `json:"xdr"`
	LastModified   uint32 `json:"lastModifiedLedgerSeq"`
	LiveUntilLedger uint32 `json:"liveUntilLedgerSeq"`
}

type GetLedgerEntriesResult struct {
	Entries      []LedgerEntry `json:"entries"`
	LatestLedger uint32        `json:"latestLedger"`
}

func (c *Client) GetLedgerEntries(ctx context.Context, keys []string) (*GetLedgerEntriesResult, error) {
	var out GetLedgerEntriesResult
	if err := c.Call(ctx, "getLedgerEntries", GetLedgerEntriesParams{Keys: keys}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
