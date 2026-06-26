# LumenLiquid API & WebSocket Documentation

**Base URL:** `https://services.lumenliquid.xyz`

---

## Table of Contents

1. [REST API](#rest-api)
2. [WebSocket API](#websocket-api)
3. [Trading Pairs](#trading-pairs)
4. [Error Handling](#error-handling)

---

## REST API

### Health Check

Verify the service is running.

```
GET /healthz
```

**Response:** `200 OK`
```
ok
```

---

### Get Open Trades

Retrieve all currently open positions for a trader.

```
GET /v1/trades/{trader}
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `trader` | string | Stellar public key (G...) |

**Response:** `200 OK`
```json
{
  "trader": "GA...",
  "trades": [
    {
      "trader": "GA...",
      "pair_index": 0,
      "trade_index": 1,
      "is_long": true,
      "leverage": 10,
      "collateral": "1000000000",
      "open_price": "65000.1234567",
      "tp_price": "70000.00",
      "sl_price": "64000.00",
      "liq_price": "62000.50",
      "opened_at": "2026-06-19T10:00:00Z"
    }
  ]
}
```

| Field | Type | Description |
|-------|------|-------------|
| `trader` | string | Stellar public key |
| `pair_index` | int | On-chain pair index (see [Trading Pairs](#trading-pairs)) |
| `trade_index` | int | Unique trade index within the pair |
| `is_long` | bool | `true` for long, `false` for short |
| `leverage` | int | Leverage applied |
| `collateral` | string | Collateral amount (raw integer string) |
| `open_price` | string | Entry price |
| `tp_price` | string | Take-profit price (or empty/"0") |
| `sl_price` | string | Stop-loss price (or empty/"0") |
| `liq_price` | string | Liquidation price |
| `opened_at` | string (RFC3339) | Position open timestamp |

---

### Get Trading History

Retrieve closed trade history for a trader (paginated).

```
GET /api/v1/trading-history/{trader}
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `trader` | string | Stellar public key (G...) |

**Query Parameters:**

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `limit` | integer | 20 | Page size (max 100) |
| `cursor` | string (RFC3339) | — | Cursor for pagination (`closed_at` timestamp) |

**Response:** `200 OK`
```json
{
  "trader": "GA...",
  "history": [
    {
      "pair_index": 0,
      "trade_index": 1,
      "is_long": true,
      "leverage": 10,
      "collateral": "1000000000",
      "open_price": "65000.1234567",
      "close_price": "67000.00",
      "tp_price": "70000.00",
      "sl_price": "64000.00",
      "realized_pnl": "150.25",
      "open_fee": "5000000",
      "close_fee": "5000000",
      "close_reason": "tp",
      "opened_at": "2026-06-18T10:00:00Z",
      "opened_tx": "abc123...",
      "closed_at": "2026-06-19T12:00:00Z",
      "closed_tx": "def456..."
    }
  ],
  "next_cursor": "2026-06-19T12:00:00Z",
  "has_more": false
}
```

| Field | Type | Description |
|-------|------|-------------|
| `pair_index` | int | On-chain pair index |
| `trade_index` | int | Trade index within the pair |
| `is_long` | bool | `true` for long, `false` for short |
| `leverage` | int | Leverage applied |
| `collateral` | string | Collateral amount (raw integer string) |
| `open_price` | string | Entry price |
| `close_price` | string | Exit price |
| `tp_price` | string | Take-profit price (or empty/"0") |
| `sl_price` | string | Stop-loss price (or empty/"0") |
| `realized_pnl` | string | Realized PnL (raw integer string) |
| `open_fee` | string | Fee paid to open (raw integer string) |
| `close_fee` | string | Fee paid to close (raw integer string) |
| `close_reason` | string | Reason for close: `manual`, `tp`, `sl`, `liquidation` |
| `opened_at` | string (RFC3339) | Position open timestamp |
| `opened_tx` | string | Stellar transaction hash of open |
| `closed_at` | string (RFC3339) | Position close timestamp |
| `closed_tx` | string | Stellar transaction hash of close |

**Pagination:** When `has_more` is `true`, use `next_cursor` as the `cursor` query parameter in the next request to fetch the next page.

---

## WebSocket API

WebSocket connections are available at the paths below. All connections are upgraded via standard HTTP WebSocket upgrade. The server sends pings every 54 seconds; connections are closed if no pong is received within 60 seconds.

**Base WebSocket URL:** `wss://services.lumenliquid.xyz`

### Connection

```javascript
const ws = new WebSocket("wss://services.lumenliquid.xyz/ws/v1/trades");
```

---

### 1. Trade Updates for a Specific Trader

```
/ws/v1/trades/{trader}
```

Real-time updates for a single trader's positions and open limit orders. On connect, a full snapshot is sent immediately. Thereafter, any state change (opened, closed, liquidated, TP/SL executed, TP/SL updated, limit placed/executed/canceled/updated) sends a fresh full snapshot.

**Server → Client (full snapshot):**
```json
{
  "type": "snapshot",
  "trades": [
    {
      "trader": "GA...",
      "pair_index": 0,
      "trade_index": 1,
      "is_long": true,
      "leverage": 10,
      "collateral": "1000000000",
      "open_price": "65000.1234567",
      "tp_price": "70000.00",
      "sl_price": "64000.00",
      "liq_price": "62000.50",
      "opened_at": "2026-06-19T10:00:00Z"
    }
  ],
  "limits": [
    {
      "trader": "GA...",
      "pair_index": 0,
      "limit_index": 0,
      "is_long": true,
      "leverage": 10,
      "collateral": "1000000000",
      "limit_price": "60000.00",
      "tp_price": "70000.00",
      "sl_price": "58000.00",
      "placed_at": "2026-06-19T09:30:00Z"
    }
  ],
  "pairs": [
    {
      "pair_index": 0,
      "symbol": "BTC/USD",
      "min_leverage": 1,
      "max_leverage": 10,
      "liq_threshold_p": 90,
      "disabled": false
    }
  ]
}
```

`limits` lists the trader's open (unfilled) limit orders. When a limit fills, it
leaves `limits` and a new position appears in `trades` (the keeper's
`execute_limit_order` emits both an `opened` and an `executed` event). `collateral`
is the raw pre-fee amount; the post-fee `collateral` shows on the resulting trade.

**Limit order fields:**

| Field | Type | Description |
|-------|------|-------------|
| `pair_index` | int | On-chain pair index |
| `limit_index` | int | Limit order index within the pair |
| `is_long` | bool | `true` for long, `false` for short |
| `leverage` | int | Leverage to apply on fill |
| `collateral` | string | Raw collateral, pre-fee (raw integer string) |
| `limit_price` | string | Trigger price; long fills at/below, short fills at/above |
| `tp_price` | string | Take-profit for the resulting trade (or "0") |
| `sl_price` | string | Stop-loss for the resulting trade (or "0") |
| `placed_at` | string (RFC3339) | When the order was placed |

---

### 2. Global Trade Feed

```
/ws/v1/trades
```

Subscribe to all trade activity across every trader. Same snapshot format as per-trader feed, containing all open trades and open limit orders across all traders.

**Server → Client:**

```json
{
  "type": "snapshot",
  "trades": [ /* every open trade across all traders */ ],
  "limits": [ /* every open limit order across all traders */ ],
  "pairs": [ /* all configured pairs */ ]
}
```

---

### 3. Price Feed

```
/ws/v1/prices
```

Real-time price feed sourced from Binance Futures aggregated trade streams. Each message is a plain text string (not JSON).

**Server → Client:**
```
0|67123.45
```

Format: `{pairIndex}|{price}`

| Pair Index | Symbol |
|------------|--------|
| 0 | BTC/USD |
| 1 | ETH/USD |
| 2 | SOL/USD |
| 3 | BNB/USD |

---

### Client-to-Server Messages

Subscribe/unsubscribe to channels after connection.

```json
{ "type": "subscribe", "channel": "trader:GA..." }
```

```json
{ "type": "unsubscribe", "channel": "trader:GA..." }
```

**Supported channels:**

| Channel | Description |
|---------|-------------|
| `trader:{address}` | Real-time updates for a specific trader |
| `trades:all` | Global trade feed |
| `prices` | Real-time price feed |

---

## Trading Pairs

On-chain pair indices and their corresponding symbols:

| Index | Symbol | Binance Stream |
|-------|--------|----------------|
| 0 | BTC/USD | `btcusdt@aggTrade` |
| 1 | ETH/USD | `ethusdt@aggTrade` |
| 2 | SOL/USD | `solusdt@aggTrade` |
| 3 | BNB/USD | `bnbusdt@aggTrade` |

Pair configuration (leverage range, fee, thresholds) is available in REST and WebSocket snapshot responses.

---

## Error Handling

**REST API:** Returns standard HTTP status codes. `400 Bad Request` for invalid parameters, `500 Internal Server Error` for server-side failures.

**WebSocket:** Connection is closed with `websocket.StatusNormalClosure` on disconnect. Invalid client messages (malformed JSON, missing channel) are silently ignored and logged server-side.
