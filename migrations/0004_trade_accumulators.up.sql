-- Persist the rollover/funding accumulator snapshots emitted on opened trades.
-- They are needed to calculate elapsed rollover/funding fees later.

BEGIN;

ALTER TABLE trades
  ADD COLUMN IF NOT EXISTS acc_rollover_open NUMERIC NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS acc_funding_open  NUMERIC NOT NULL DEFAULT 0;

ALTER TABLE trade_history
  ADD COLUMN IF NOT EXISTS acc_rollover_open NUMERIC NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS acc_funding_open  NUMERIC NOT NULL DEFAULT 0;

COMMIT;
