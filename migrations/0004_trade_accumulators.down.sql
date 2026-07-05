BEGIN;

ALTER TABLE trade_history
  DROP COLUMN IF EXISTS acc_rollover_open,
  DROP COLUMN IF EXISTS acc_funding_open;

ALTER TABLE trades
  DROP COLUMN IF EXISTS acc_rollover_open,
  DROP COLUMN IF EXISTS acc_funding_open;

COMMIT;
