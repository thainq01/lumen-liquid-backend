-- Pair-registry config tables (Phase 1).
--
-- Populated by `cmd/pair-indexer` (run as a cron), which reads the deployed
-- PairRegistry contract via `simulateTransaction` and upserts a full snapshot
-- of every pair + group config. These tables back the `/pairs` REST endpoints
-- and give the keeper its `liq_threshold_p` / fee parameters without a chain
-- round-trip per trade.

BEGIN;

-- ── groups: fee tiers shared across pairs ──────────────────────────────────
CREATE TABLE IF NOT EXISTS pair_groups (
  group_index         INTEGER   PRIMARY KEY,
  name                TEXT      NOT NULL,
  max_collateral_usdc NUMERIC   NOT NULL,   -- USDC_SCALE (1e7)
  open_fee_p          NUMERIC   NOT NULL,   -- P_SCALE (1e10)
  close_fee_p         NUMERIC   NOT NULL,   -- P_SCALE (1e10)
  synced_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── pairs: per-market config snapshot ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS pairs (
  pair_index          INTEGER   PRIMARY KEY,
  symbol              TEXT      NOT NULL,
  reflector_asset_type TEXT     NOT NULL,   -- 'stellar' | 'other'
  reflector_asset     TEXT      NOT NULL,   -- C…/G… address, or the Symbol
  group_index         INTEGER   NOT NULL,
  spread_p            NUMERIC   NOT NULL,   -- P_SCALE (1e10)
  min_leverage        INTEGER   NOT NULL,
  max_leverage        INTEGER   NOT NULL,
  min_lev_pos_usdc    NUMERIC   NOT NULL,   -- USDC_SCALE (1e7)
  max_oi_usdc         NUMERIC   NOT NULL,   -- USDC_SCALE (1e7)
  max_neg_pnl_p       NUMERIC   NOT NULL,   -- P_SCALE (1e10)
  liq_threshold_p     INTEGER   NOT NULL,   -- integer percent
  max_gain_p          INTEGER   NOT NULL,   -- integer percent
  disabled            BOOLEAN   NOT NULL DEFAULT false,
  one_percent_depth   NUMERIC   NOT NULL DEFAULT 0,  -- USDC_SCALE (1e7)
  synced_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS pairs_group_idx ON pairs (group_index);

COMMIT;
