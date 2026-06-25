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
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

type Client struct {
	urls  []string
	http  *http.Client
	idSeq atomic.Uint64
}

// New builds a client. First url is primary; any extras are failover backups
// tried in order when the primary returns a network error or 5xx/429 response.
// Empty url strings are ignored.
func New(url string, backups ...string) *Client {
	urls := make([]string, 0, 1+len(backups))
	if url != "" {
		urls = append(urls, url)
	}
	for _, b := range backups {
		if b != "" {
			urls = append(urls, b)
		}
	}
	if len(urls) == 0 {
		urls = []string{""} // preserve prior behavior: fail loudly on use
	}
	return &Client{
		urls: urls,
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

	var lastErr error
	for i, url := range c.urls {
		err := c.callOne(ctx, url, body, out)
		if err == nil {
			return nil
		}
		lastErr = err
		// Only fail over on transport errors or retryable HTTP statuses (5xx/429).
		// RPC-level errors (*RPCError) are valid responses — return immediately.
		if !shouldFailover(err) || i == len(c.urls)-1 {
			return err
		}
		// else: try next backup endpoint
	}
	return lastErr
}

// shouldFailover reports whether err warrants trying the next endpoint.
func shouldFailover(err error) bool {
	var he *HTTPError
	if errors.As(err, &he) {
		return he.StatusCode == http.StatusTooManyRequests || he.StatusCode >= 500
	}
	var re *RPCError
	if errors.As(err, &re) {
		return false // valid RPC error response — backups won't differ
	}
	return true // transport/timeout/decode error — try next
}

func (c *Client) callOne(ctx context.Context, url string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
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
