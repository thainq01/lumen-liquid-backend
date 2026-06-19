-- Carry open_fee on the live trade so it can be copied into trade_history at
-- close. The contract emits open_fee on the `opened` event; storing it here
-- (rather than recomputing) keeps the indexer the single source of truth.

BEGIN;
ALTER TABLE trades ADD COLUMN IF NOT EXISTS open_fee NUMERIC NOT NULL DEFAULT 0;
COMMIT;
