package pgstore

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/ledger"
)

// Billing works like a card payment:
//
//	acquire  hold     wallet -H, hold:<lease> +H        H = price × duration
//	release  capture  hold:<lease> -H, revenue +U, wallet +(H-U)
//	expiry   expire   hold:<lease> -H, revenue +H
//
// where U is the price of the minutes actually used. Every step runs in the
// same transaction as the lease change that causes it, and each lease's
// settlement has the idempotency key settle:<lease>, so it happens exactly
// once whichever path (release, expiry, device reuse) gets there first.

// SyncPrices upserts the configured per-minute prices.
func SyncPrices(ctx context.Context, pool *pgxpool.Pool, prices map[string]int64) error {
	batch := &pgx.Batch{}
	for model, price := range prices {
		batch.Queue(`INSERT INTO prices (model, credits_per_minute) VALUES ($1, $2)
			ON CONFLICT (model) DO UPDATE SET credits_per_minute = excluded.credits_per_minute`, model, price)
	}

	return pool.SendBatch(ctx, batch).Close()
}

// priceFor returns the per-minute price for a model, or zero if neither the
// model nor 'default' has a price, in which case the lease is free.
func priceFor(ctx context.Context, tx pgx.Tx, model string) (int64, error) {
	var price int64
	err := tx.QueryRow(ctx, `
		SELECT credits_per_minute FROM prices WHERE model IN ($1, 'default')
		ORDER BY model = 'default' LIMIT 1`, model).Scan(&price)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}

	return price, err
}

// minutes rounds a duration up to whole billable minutes. Usage is rounded
// the same way, in SQL, when a lease settles.
func minutes(d time.Duration) int64 {
	return int64((d + time.Minute - 1) / time.Minute)
}

func hold(ctx context.Context, tx pgx.Tx, l lease.Lease) error {
	if l.Held == 0 {
		return nil
	}

	_, _, err := ledger.Post(ctx, tx, ledger.Entry{
		Kind: ledger.KindHold, Holder: l.Holder, LeaseID: l.ID, IdempotencyKey: "hold:" + l.ID,
		Postings: []ledger.Posting{
			{Account: ledger.WalletAccount(l.Holder), Amount: -l.Held},
			{Account: ledger.HoldAccount(l.ID), Amount: l.Held},
		},
	})
	if errors.Is(err, ledger.ErrInsufficientFunds) {
		return lease.ErrInsufficientCredits
	}

	return err
}

// settle releases a lease's hold, charging for the minutes between
// acquired_at and released_at. The caller must have set released_at. If the
// lease has already been settled, settle returns that original settlement.
func settle(ctx context.Context, tx pgx.Tx, leaseID string) (lease.Settlement, error) {
	var held int64
	err := tx.QueryRow(ctx, `SELECT balance FROM account_balances WHERE account = $1 FOR UPDATE`,
		ledger.HoldAccount(leaseID)).Scan(&held)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && held == 0) {
		return previousSettlement(ctx, tx, leaseID)
	}
	if err != nil {
		return lease.Settlement{}, err
	}

	var (
		holder      string
		price, used int64
		expired     bool
	)
	if err := tx.QueryRow(ctx, `
		SELECT holder, credits_per_minute,
			ceil(extract(epoch FROM released_at - acquired_at) / 60)::bigint,
			released_at >= expires_at
		FROM leases WHERE id = $1`, leaseID).Scan(&holder, &price, &used, &expired); err != nil {
		return lease.Settlement{}, err
	}

	s := lease.Settlement{Charged: min(price*used, held)}
	s.Returned = held - s.Charged

	kind := ledger.KindCapture
	if expired {
		kind = ledger.KindExpire
	}
	postings := []ledger.Posting{{Account: ledger.HoldAccount(leaseID), Amount: -held}}
	if s.Charged > 0 {
		postings = append(postings, ledger.Posting{Account: ledger.Revenue, Amount: s.Charged})
	}
	if s.Returned > 0 {
		postings = append(postings, ledger.Posting{Account: ledger.WalletAccount(holder), Amount: s.Returned})
	}

	_, _, err = ledger.Post(ctx, tx, ledger.Entry{
		Kind: kind, Holder: holder, LeaseID: leaseID, IdempotencyKey: "settle:" + leaseID, Postings: postings,
	})

	return s, err
}

func previousSettlement(ctx context.Context, tx pgx.Tx, leaseID string) (lease.Settlement, error) {
	var s lease.Settlement
	err := tx.QueryRow(ctx, `
		SELECT
			COALESCE(sum(p.amount) FILTER (WHERE p.account = $2), 0),
			COALESCE(sum(p.amount) FILTER (WHERE p.account LIKE 'wallet:%'), 0)
		FROM journal_entries e JOIN postings p ON p.entry_id = e.id
		WHERE e.idempotency_key = $1`, "settle:"+leaseID, ledger.Revenue).Scan(&s.Charged, &s.Returned)

	return s, err
}
