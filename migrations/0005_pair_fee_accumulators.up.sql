-- Pair funding/rollover accumulator snapshots.
--
-- These are off-chain mirrors for API/keeper calculations. The contracts still
-- compute and charge the canonical fees during close/liquidation.

BEGIN;

CREATE TABLE IF NOT EXISTS pair_fee_accumulators (
  pair_index                 INTEGER PRIMARY KEY,
  acc_rollover               NUMERIC NOT NULL DEFAULT 0,
  acc_funding_long           NUMERIC NOT NULL DEFAULT 0,
  acc_funding_short          NUMERIC NOT NULL DEFAULT 0,
  rollover_fee_per_ledger_p  NUMERIC NOT NULL DEFAULT 0,
  funding_fee_per_ledger_p   NUMERIC NOT NULL DEFAULT 0,
  projected_at_ledger        BIGINT  NOT NULL DEFAULT 0,
  synced_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
