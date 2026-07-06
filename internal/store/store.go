// Package store contains repositories that translate decoded events into
// Postgres writes. Each event type lands in `trade_events` (audit) and may
// also mutate a projection table (`trades`, `trade_history`, `limit_orders`,
// `limit_order_history`, `vault_positions`, `pair_oi`).
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lumenliquid/backend/internal/events"
	"github.com/lumenliquid/backend/internal/keeper"
)

type Repo struct{ Pool *pgxpool.Pool }

func New(p *pgxpool.Pool) *Repo { return &Repo{Pool: p} }

// ── cursor ────────────────────────────────────────────────────────────────

func (r *Repo) ReadCursor(ctx context.Context) (lastLedger uint64, lastCursor string, err error) {
	row := r.Pool.QueryRow(ctx, `SELECT last_ledger, last_cursor FROM indexer_cursor WHERE id = 1`)
	if err := row.Scan(&lastLedger, &lastCursor); err != nil {
		if err == pgx.ErrNoRows {
			return 0, "", nil
		}
		return 0, "", err
	}
	return
}

func (r *Repo) WriteCursor(ctx context.Context, lastLedger uint64, cursor string) error {
	_, err := r.Pool.Exec(ctx, `
		INSERT INTO indexer_cursor (id, last_ledger, last_cursor, updated_at)
		VALUES (1, $1, $2, now())
		ON CONFLICT (id) DO UPDATE
		  SET last_ledger = EXCLUDED.last_ledger,
		      last_cursor = EXCLUDED.last_cursor,
		      updated_at  = now()`,
		lastLedger, cursor)
	return err
}

// ── apply event ───────────────────────────────────────────────────────────

// Apply persists a decoded event idempotently inside a single transaction.
// The audit-log INSERT uses `ON CONFLICT DO NOTHING` so re-running a ledger
// (after restart, or via replay) is safe.
func (r *Repo) Apply(ctx context.Context, e events.Event, eventIndex uint32) error {
	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	occurred, err := parseLedgerTime(e.OccurredAt)
	if err != nil {
		return fmt.Errorf("parse ledger time %q: %w", e.OccurredAt, err)
	}

	rawData, _ := json.Marshal(e)
	_, err = tx.Exec(ctx, `
		INSERT INTO trade_events (
		  tx_hash, event_index, ledger, contract_id, topic,
		  trader, pair_index, trade_index, data, occurred_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (tx_hash, event_index) DO NOTHING`,
		e.TxHash, eventIndex, e.Ledger, e.ContractID, e.Topic,
		nilIfEmpty(e.Trader), e.PairIndex, e.TradeIndex,
		rawData, occurred,
	)
	if err != nil {
		return fmt.Errorf("insert trade_events: %w", err)
	}

	switch e.Source {
	case events.SrcPM:
		if err := applyPM(ctx, tx, e, occurred); err != nil {
			return err
		}
	case events.SrcVault:
		if err := applyVault(ctx, tx, e, occurred); err != nil {
			return err
		}
	case events.SrcRegistry:
		if err := applyRegistry(ctx, tx, e, occurred); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

// ── PM projections ────────────────────────────────────────────────────────

func applyPM(ctx context.Context, tx pgx.Tx, e events.Event, at time.Time) error {
	switch e.Topic {
	case "opened":
		if e.Trade == nil || e.TradeIndex == nil {
			return nil
		}
		liqPrice, _ := keeper.LiquidationPrice(
			e.Trade.OpenPrice, e.Trade.IsLong,
			e.Trade.Collateral, e.Trade.Leverage,
			nil, nil, 90, // MVP: rollover/funding 0; liq_threshold from pair config (default 90)
		)
		_, err := tx.Exec(ctx, `
			INSERT INTO trades (
			  trader, pair_index, trade_index, is_long, leverage,
			  collateral, open_price, acc_rollover_open, acc_funding_open,
			  tp_price, sl_price, liq_price,
			  liq_threshold_p, open_fee, opened_at, opened_tx
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
			ON CONFLICT (trader, pair_index, trade_index) DO UPDATE
			  SET is_long      = EXCLUDED.is_long,
			      leverage     = EXCLUDED.leverage,
			      collateral   = EXCLUDED.collateral,
			      open_price   = EXCLUDED.open_price,
			      acc_rollover_open = EXCLUDED.acc_rollover_open,
			      acc_funding_open  = EXCLUDED.acc_funding_open,
			      tp_price     = EXCLUDED.tp_price,
			      sl_price     = EXCLUDED.sl_price,
			      liq_price    = EXCLUDED.liq_price,
			      open_fee     = EXCLUDED.open_fee`,
			e.Trader, e.Trade.PairIndex, *e.TradeIndex,
			e.Trade.IsLong, e.Trade.Leverage,
			bigStr(e.Trade.Collateral), bigStr(e.Trade.OpenPrice),
			bigStr(e.Trade.AccRolloverOpen), bigStr(e.Trade.AccFundingOpen),
			bigStr(e.Trade.TpPrice), bigStr(e.Trade.SlPrice),
			bigStr(liqPrice), 90, bigStr(e.OpenFee), at, e.TxHash,
		)
		return err

	case "updated_tp_sl":
		if e.TradeIndex == nil || e.TpPrice == nil || e.SlPrice == nil {
			return nil
		}
		_, err := tx.Exec(ctx, `
				UPDATE trades
				   SET tp_price = $1, sl_price = $2
				 WHERE trader = $3 AND pair_index = $4 AND trade_index = $5`,
			bigStr(e.TpPrice), bigStr(e.SlPrice),
			e.Trader, deref(e.PairIndex), *e.TradeIndex,
		)
		return err

	case "closed":
		return closeTrade(ctx, tx, e, "manual", at)
	case "tp_sl_executed":
		return closeTradeUnknownReason(ctx, tx, e, at) // figured out via tp/sl side
	case "liq":
		return closeTrade(ctx, tx, e, "liquidation", at)

	case "placed":
		if e.Limit == nil || e.LimitIndex == nil {
			return nil
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO limit_orders (
			  trader, pair_index, limit_index, is_long, leverage,
			  collateral, limit_price, tp_price, sl_price, placed_at, placed_tx
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (trader, pair_index, limit_index) DO UPDATE
			  SET is_long     = EXCLUDED.is_long,
			      leverage    = EXCLUDED.leverage,
			      collateral  = EXCLUDED.collateral,
			      limit_price = EXCLUDED.limit_price,
			      tp_price    = EXCLUDED.tp_price,
			      sl_price    = EXCLUDED.sl_price`,
			e.Trader, e.Limit.PairIndex, *e.LimitIndex,
			e.Limit.IsLong, e.Limit.Leverage,
			bigStr(e.Limit.Collateral), bigStr(e.Limit.LimitPrice),
			bigStr(e.Limit.TpPrice), bigStr(e.Limit.SlPrice), at, e.TxHash,
		)
		return err
	case "updated_limit":
		if e.LimitIndex == nil || e.LimitPrice == nil {
			return nil
		}
		_, err := tx.Exec(ctx, `
			UPDATE limit_orders
			   SET limit_price = $1, tp_price = $2, sl_price = $3
			 WHERE trader = $4 AND pair_index = $5 AND limit_index = $6`,
			bigStr(e.LimitPrice), bigStr(e.TpPrice), bigStr(e.SlPrice),
			e.Trader, deref(e.PairIndex), *e.LimitIndex,
		)
		return err
	case "executed":
		return resolveLimit(ctx, tx, e, "executed", at)
	case "canceled":
		return resolveLimit(ctx, tx, e, "canceled", at)
	}
	return nil
}

// closeTrade moves a row from trades → trade_history with the given reason.
// Financial fields (close_price, close_fee, realized_pnl) come from the
// enriched close event. net_pnl is already the trader's PnL (negative = loss),
// matching the on-chain return_collateral_with_pnl(eff_collat, net_pnl) call,
// so it is stored verbatim.
func closeTrade(ctx context.Context, tx pgx.Tx, e events.Event, reason string, at time.Time) error {
	if e.TradeIndex == nil {
		return nil
	}
	var realizedPnl any
	if e.NetPnl != nil {
		realizedPnl = e.NetPnl.String()
	}
	_, err := tx.Exec(ctx, `
		WITH del AS (
		  DELETE FROM trades
		   WHERE trader = $1 AND pair_index = $2 AND trade_index = $3
		   RETURNING trader, pair_index, trade_index, is_long, leverage,
		             collateral, open_price, acc_rollover_open, acc_funding_open,
		             tp_price, sl_price, open_fee,
		             opened_at, opened_tx
		)
		INSERT INTO trade_history (
		  trader, pair_index, trade_index, is_long, leverage,
		  collateral, open_price, close_price, acc_rollover_open, acc_funding_open,
		  tp_price, sl_price,
		  realized_pnl, open_fee, close_fee,
		  close_reason, opened_at, opened_tx, closed_at, closed_tx
		)
		SELECT trader, pair_index, trade_index, is_long, leverage,
		       collateral, open_price, $7, acc_rollover_open, acc_funding_open,
		       tp_price, sl_price,
		       $8, open_fee, $9,
		       $4, opened_at, opened_tx, $5, $6
		  FROM del
		ON CONFLICT (trader, pair_index, trade_index, closed_tx) DO NOTHING`,
		e.Trader, deref(e.PairIndex), *e.TradeIndex, reason, at, e.TxHash,
		bigStrOrNil(e.ClosePrice), realizedPnl, bigStrOrNil(e.CloseFee),
	)
	return err
}

// closeTradeUnknownReason is fired by tp_sl_executed; we pick "tp" vs "sl" by
// comparing the close_price against the row's tp_price and sl_price. When the
// event carries a close_price we use it directly; falling back to the old
// "tp_price nonzero → tp" heuristic for legacy events without close_price.
func closeTradeUnknownReason(ctx context.Context, tx pgx.Tx, e events.Event, at time.Time) error {
	if e.TradeIndex == nil {
		return nil
	}
	row := tx.QueryRow(ctx, `
		SELECT is_long, tp_price, sl_price FROM trades
		 WHERE trader = $1 AND pair_index = $2 AND trade_index = $3`,
		e.Trader, deref(e.PairIndex), *e.TradeIndex)
	var isLong, hasRow bool
	var tp, sl float64
	if err := row.Scan(&isLong, &tp, &sl); err == nil {
		hasRow = true
	}

	reason := "tp" // default
	if hasRow && e.ClosePrice != nil {
		cp, _ := new(big.Float).SetString(e.ClosePrice.String())
		if isLong {
			if sl > 0 && cp.Cmp(new(big.Float).SetFloat64(sl)) <= 0 {
				reason = "sl"
			} else if tp > 0 && cp.Cmp(new(big.Float).SetFloat64(tp)) >= 0 {
				reason = "tp"
			}
		} else {
			if sl > 0 && cp.Cmp(new(big.Float).SetFloat64(sl)) >= 0 {
				reason = "sl"
			} else if tp > 0 && cp.Cmp(new(big.Float).SetFloat64(tp)) <= 0 {
				reason = "tp"
			}
		}
	} else if tp == 0 {
		reason = "sl"
	}
	return closeTrade(ctx, tx, e, reason, at)
}

func resolveLimit(ctx context.Context, tx pgx.Tx, e events.Event, resolution string, at time.Time) error {
	if e.LimitIndex == nil {
		return nil
	}
	_, err := tx.Exec(ctx, `
		WITH del AS (
		  DELETE FROM limit_orders
		   WHERE trader = $1 AND pair_index = $2 AND limit_index = $3
		   RETURNING *
		)
		INSERT INTO limit_order_history (
		  trader, pair_index, limit_index, is_long, leverage,
		  collateral, limit_price, tp_price, sl_price,
		  resolution, placed_at, placed_tx, resolved_at, resolved_tx
		)
		SELECT trader, pair_index, limit_index, is_long, leverage,
		       collateral, limit_price, tp_price, sl_price,
		       $4, placed_at, placed_tx, $5, $6
		  FROM del`,
		e.Trader, deref(e.PairIndex), *e.LimitIndex, resolution, at, e.TxHash,
	)
	return err
}

// ── Vault projections ─────────────────────────────────────────────────────

func applyVault(ctx context.Context, tx pgx.Tx, e events.Event, at time.Time) error {
	switch e.Topic {
	case "deposit":
		_, err := tx.Exec(ctx, `
			INSERT INTO vault_positions (depositor, shares, total_deposited, last_action_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (depositor) DO UPDATE
			  SET shares          = vault_positions.shares + EXCLUDED.shares,
			      total_deposited = vault_positions.total_deposited + EXCLUDED.total_deposited,
			      last_action_at  = EXCLUDED.last_action_at`,
			e.Trader, bigStr(e.Shares), bigStr(e.Assets), at)
		return err
	case "withdraw":
		_, err := tx.Exec(ctx, `
			INSERT INTO vault_positions (depositor, shares, total_withdrawn, last_action_at)
			VALUES ($1, -($2::numeric), $3, $4)
			ON CONFLICT (depositor) DO UPDATE
			  SET shares          = vault_positions.shares + EXCLUDED.shares,
			      total_withdrawn = vault_positions.total_withdrawn + EXCLUDED.total_withdrawn,
			      last_action_at  = EXCLUDED.last_action_at`,
			e.Trader, bigStr(e.Shares), bigStr(e.Assets), at)
		return err
	}
	return nil
}

// ── Registry projections ──────────────────────────────────────────────────

func applyRegistry(ctx context.Context, tx pgx.Tx, e events.Event, at time.Time) error {
	switch e.Topic {
	case "rollover_rate":
		if e.PairIndex == nil || e.RatePer == nil {
			return nil
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO pair_fee_accumulators (
			  pair_index, rollover_fee_per_ledger_p, synced_at
			) VALUES ($1, $2, $3)
			ON CONFLICT (pair_index) DO UPDATE
			  SET rollover_fee_per_ledger_p = EXCLUDED.rollover_fee_per_ledger_p,
			      synced_at = EXCLUDED.synced_at`,
			*e.PairIndex, bigStr(e.RatePer), at)
		return err
	case "funding_rate":
		if e.PairIndex == nil || e.RatePer == nil {
			return nil
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO pair_fee_accumulators (
			  pair_index, funding_fee_per_ledger_p, synced_at
			) VALUES ($1, $2, $3)
			ON CONFLICT (pair_index) DO UPDATE
			  SET funding_fee_per_ledger_p = EXCLUDED.funding_fee_per_ledger_p,
			      synced_at = EXCLUDED.synced_at`,
			*e.PairIndex, bigStr(e.RatePer), at)
		return err
	case "oi_add", "oi_sub":
		if e.PairIndex == nil || e.OIIsLong == nil || e.Amount == nil {
			return nil
		}
		longDelta, shortDelta := "0", "0"
		if *e.OIIsLong {
			longDelta = bigStr(e.Amount)
		} else {
			shortDelta = bigStr(e.Amount)
		}
		sign := "+"
		insertLong, insertShort := longDelta, shortDelta
		if e.Topic == "oi_sub" {
			sign = "-"
			insertLong, insertShort = "0", "0"
		}
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO pair_oi (pair_index, long_oi, short_oi, updated_at)
			VALUES ($1, GREATEST(0, $2::NUMERIC), GREATEST(0, $3::NUMERIC), $4)
			ON CONFLICT (pair_index) DO UPDATE
			  SET long_oi = GREATEST(0, pair_oi.long_oi %s $5::NUMERIC),
			      short_oi = GREATEST(0, pair_oi.short_oi %s $6::NUMERIC),
			      updated_at = EXCLUDED.updated_at`, sign, sign),
			*e.PairIndex, insertLong, insertShort, at, longDelta, shortDelta)
		return err
	}
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func deref(p *int32) int32 {
	if p == nil {
		return 0
	}
	return *p
}

func bigStr(b *big.Int) string {
	if b == nil {
		return "0"
	}
	return b.String()
}

// bigStrOrNil returns the decimal string, or nil so the column stays NULL when
// the event didn't carry the value (e.g. replaying an old pre-enrichment event).
func bigStrOrNil(b *big.Int) any {
	if b == nil {
		return nil
	}
	return b.String()
}

func parseLedgerTime(s string) (time.Time, error) {
	if s == "" {
		return time.Now().UTC(), nil
	}
	return time.Parse(time.RFC3339, s)
}
