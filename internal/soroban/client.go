// Package soroban is a thin JSON-RPC client over net/http for the methods
// the indexer needs: getEvents, getLatestLedger, simulateTransaction.
//
// It avoids depending on the (still-evolving) soroban surface in stellar/go;
// JSON-RPC is stable enough to call directly. XDR is decoded by stellar/go/xdr.
package soroban

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

type Client struct {
	url   string
	http  *http.Client
	idSeq atomic.Uint64
}

func New(url string) *Client {
	return &Client{
		url:  url,
		http: &http.Client{Timeout: 30 * time.Second},
	}
}

type rpcReq struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type rpcResp struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      uint64          `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RPCError       `json:"error,omitempty"`
}

type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string { return fmt.Sprintf("rpc %d: %s", e.Code, e.Message) }

// HTTPError wraps RPC errors with HTTP status for rate-limit detection.
type HTTPError struct {
	StatusCode int
	Body       string
	Err        error
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("http %d: %s", e.StatusCode, e.Err.Error())
}

func (e *HTTPError) Unwrap() error { return e.Err }

// IsRateLimit returns true if this error is a rate-limit (429) response.
func (e *HTTPError) IsRateLimit() bool { return e.StatusCode == 429 }

func (c *Client) Call(ctx context.Context, method string, params any, out any) error {
	body, err := json.Marshal(rpcReq{
		JSONRPC: "2.0",
		ID:      c.idSeq.Add(1),
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Check HTTP status for rate limits or server errors
	if resp.StatusCode != http.StatusOK {
		baseErr := fmt.Errorf("unexpected status: %s", resp.Status)
		if len(raw) > 0 {
			baseErr = fmt.Errorf("%s: %s", baseErr, string(raw))
		}
		return &HTTPError{StatusCode: resp.StatusCode, Body: string(raw), Err: baseErr}
	}

	var r rpcResp
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("decode rpc resp: %w (body=%s)", err, string(raw))
	}
	if r.Error != nil {
		return r.Error
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(r.Result, out)
}
