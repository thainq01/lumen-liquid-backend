-- Initial schema for the LumenLiquid indexer (Phase 1).
--
-- Live tables (`trades`, `limit_orders`, `vault_positions`, `pair_oi`)
-- mirror the on-chain state for fast keeper / API reads. Rows are deleted
-- when the on-chain state ends (close, cancel, withdraw).
--
-- Append-only tables (`trade_history`, `limit_order_history`, `trade_events`)
-- are never modified after insert — they power history APIs and projection
-- replay.

BEGIN;

-- ── audit log: raw decoded events ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_events (
  tx_hash       TEXT      NOT NULL,
  event_index   INTEGER   NOT NULL,
  ledger        BIGINT    NOT NULL,
  contract_id   TEXT      NOT NULL,
  topic         TEXT      NOT NULL,
  trader        TEXT,
  pair_index    INTEGER,
  trade_index   INTEGER,
  data          JSONB     NOT NULL,
  occurred_at   TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tx_hash, event_index)
);
CREATE INDEX IF NOT EXISTS trade_events_trader_time_idx
  ON trade_events (trader, occurred_at DESC) WHERE trader IS NOT NULL;
CREATE INDEX IF NOT EXISTS trade_events_pair_time_idx
  ON trade_events (pair_index, occurred_at DESC) WHERE pair_index IS NOT NULL;
CREATE INDEX IF NOT EXISTS trade_events_topic_idx
  ON trade_events (topic, ledger);

-- ── live: open trades ──────────────────────────────────────────────────────
-- Keeper hits this table on every price tick. Indexes are filtered partials
-- so they stay tight even with many pairs.
CREATE TABLE IF NOT EXISTS trades (
  trader        TEXT      NOT NULL,
  pair_index    INTEGER   NOT NULL,
  trade_index   INTEGER   NOT NULL,
  is_long       BOOLEAN   NOT NULL,
  leverage      INTEGER   NOT NULL,
  collateral    NUMERIC   NOT NULL,
  open_price    NUMERIC   NOT NULL,
  acc_rollover_open NUMERIC NOT NULL DEFAULT 0,
  acc_funding_open  NUMERIC NOT NULL DEFAULT 0,
  tp_price      NUMERIC   NOT NULL DEFAULT 0,
  sl_price      NUMERIC   NOT NULL DEFAULT 0,
  liq_price     NUMERIC,                              -- precomputed at open
  liq_threshold_p INTEGER NOT NULL DEFAULT 90,
  opened_at     TIMESTAMPTZ NOT NULL,
  opened_tx     TEXT      NOT NULL,
  PRIMARY KEY (trader, pair_index, trade_index)
);
CREATE INDEX IF NOT EXISTS trades_long_liq_idx
  ON trades (pair_index, liq_price)  WHERE is_long;
CREATE INDEX IF NOT EXISTS trades_short_liq_idx
  ON trades (pair_index, liq_price)  WHERE NOT is_long;
CREATE INDEX IF NOT EXISTS trades_long_tp_idx
  ON trades (pair_index, tp_price)   WHERE is_long AND tp_price > 0;
CREATE INDEX IF NOT EXISTS trades_short_tp_idx
  ON trades (pair_index, tp_price)   WHERE NOT is_long AND tp_price > 0;
CREATE INDEX IF NOT EXISTS trades_long_sl_idx
  ON trades (pair_index, sl_price)   WHERE is_long AND sl_price > 0;
CREATE INDEX IF NOT EXISTS trades_short_sl_idx
  ON trades (pair_index, sl_price)   WHERE NOT is_long AND sl_price > 0;

-- ── append-only: closed trades ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS trade_history (
  id            BIGSERIAL PRIMARY KEY,
  trader        TEXT      NOT NULL,
  pair_index    INTEGER   NOT NULL,
  trade_index   INTEGER   NOT NULL,
  is_long       BOOLEAN   NOT NULL,
  leverage      INTEGER   NOT NULL,
  collateral    NUMERIC   NOT NULL,
  open_price    NUMERIC   NOT NULL,
  close_price   NUMERIC,
  acc_rollover_open NUMERIC NOT NULL DEFAULT 0,
  acc_funding_open  NUMERIC NOT NULL DEFAULT 0,
  tp_price      NUMERIC   NOT NULL DEFAULT 0,
  sl_price      NUMERIC   NOT NULL DEFAULT 0,
  realized_pnl  NUMERIC,
  open_fee      NUMERIC,
  close_fee     NUMERIC,
  close_reason  TEXT      NOT NULL,            -- 'manual'|'tp'|'sl'|'liquidation'
  opened_at     TIMESTAMPTZ NOT NULL,
  opened_tx     TEXT      NOT NULL,
  closed_at     TIMESTAMPTZ NOT NULL,
  closed_tx     TEXT      NOT NULL,
  CONSTRAINT trade_history_close_reason_chk
    CHECK (close_reason IN ('manual','tp','sl','liquidation'))
);
CREATE INDEX IF NOT EXISTS trade_history_trader_idx
  ON trade_history (trader, closed_at DESC);
CREATE INDEX IF NOT EXISTS trade_history_pair_idx
  ON trade_history (pair_index, closed_at DESC);
CREATE UNIQUE INDEX IF NOT EXISTS trade_history_unique_close_idx
  ON trade_history (trader, pair_index, trade_index, closed_tx);

-- ── live: open limit orders ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS limit_orders (
  trader        TEXT      NOT NULL,
  pair_index    INTEGER   NOT NULL,
  limit_index   INTEGER   NOT NULL,
  is_long       BOOLEAN   NOT NULL,
  leverage      INTEGER   NOT NULL,
  collateral    NUMERIC   NOT NULL,
  limit_price   NUMERIC   NOT NULL,
  tp_price      NUMERIC   NOT NULL DEFAULT 0,
  sl_price      NUMERIC   NOT NULL DEFAULT 0,
  placed_at     TIMESTAMPTZ NOT NULL,
  placed_tx     TEXT      NOT NULL,
  PRIMARY KEY (trader, pair_index, limit_index)
);
CREATE INDEX IF NOT EXISTS limit_orders_long_idx
  ON limit_orders (pair_index, limit_price) WHERE is_long;
CREATE INDEX IF NOT EXISTS limit_orders_short_idx
  ON limit_orders (pair_index, limit_price) WHERE NOT is_long;

-- ── append-only: closed limit orders ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS limit_order_history (
  id            BIGSERIAL PRIMARY KEY,
  trader        TEXT      NOT NULL,
  pair_index    INTEGER   NOT NULL,
  limit_index   INTEGER   NOT NULL,
  is_long       BOOLEAN   NOT NULL,
  leverage      INTEGER   NOT NULL,
  collateral    NUMERIC   NOT NULL,
  limit_price   NUMERIC   NOT NULL,
  tp_price      NUMERIC   NOT NULL DEFAULT 0,
  sl_price      NUMERIC   NOT NULL DEFAULT 0,
  resolution    TEXT      NOT NULL,            -- 'executed'|'canceled'
  placed_at     TIMESTAMPTZ NOT NULL,
  placed_tx     TEXT      NOT NULL,
  resolved_at   TIMESTAMPTZ NOT NULL,
  resolved_tx   TEXT      NOT NULL,
  CONSTRAINT limit_order_history_resolution_chk
    CHECK (resolution IN ('executed','canceled'))
);
CREATE INDEX IF NOT EXISTS limit_order_history_trader_idx
  ON limit_order_history (trader, resolved_at DESC);

-- ── vault positions ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS vault_positions (
  depositor       TEXT      PRIMARY KEY,
  shares          NUMERIC   NOT NULL DEFAULT 0,
  total_deposited NUMERIC   NOT NULL DEFAULT 0,
  total_withdrawn NUMERIC   NOT NULL DEFAULT 0,
  last_action_at  TIMESTAMPTZ NOT NULL
);

-- ── pair open interest ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pair_oi (
  pair_index    INTEGER   PRIMARY KEY,
  long_oi       NUMERIC   NOT NULL DEFAULT 0,
  short_oi      NUMERIC   NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── indexer cursor (singleton) ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS indexer_cursor (
  id            INTEGER   PRIMARY KEY DEFAULT 1,
  last_ledger   BIGINT    NOT NULL,
  last_cursor   TEXT      NOT NULL DEFAULT '',
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT indexer_cursor_singleton CHECK (id = 1)
);

COMMIT;
