package ledger

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrInvalidArgument     = errors.New("invalid argument")
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
)

type Wallet struct {
	Holder    string
	Available int64 // spendable now
	Held      int64 // reserved by leases that haven't settled
}

type StatementLine struct {
	EntryID int64
	Kind    Kind
	LeaseID string
	Amount  int64 // change to the wallet; zero for an expiry, which only moves held credits
	Charged int64 // credits that went to revenue in this entry
	Balance int64 // wallet balance after this entry
	At      time.Time
}

// Book is the read side of the ledger plus top-ups, which are the one
// movement that doesn't involve a lease.
type Book struct {
	pool *pgxpool.Pool
}

func NewBook(pool *pgxpool.Pool) *Book {
	return &Book{pool: pool}
}

func (b *Book) TopUp(ctx context.Context, holder string, amount int64, key string) (Wallet, error) {
	switch {
	case holder == "":
		return Wallet{}, fmt.Errorf("%w: holder is required", ErrInvalidArgument)
	case key == "":
		return Wallet{}, fmt.Errorf("%w: idempotency key is required", ErrInvalidArgument)
	case amount <= 0:
		return Wallet{}, fmt.Errorf("%w: amount must be positive", ErrInvalidArgument)
	}

	err := pgx.BeginFunc(ctx, b.pool, func(tx pgx.Tx) error {
		idemKey := fmt.Sprintf("topup:%s:%s", holder, key)
		_, posted, err := Post(ctx, tx, Entry{
			Kind: KindTopUp, Holder: holder, IdempotencyKey: idemKey,
			Postings: []Posting{{External, -amount}, {WalletAccount(holder), amount}},
		})
		if err != nil || posted {
			return err
		}

		// A retry: fine if it's the same top-up, an error if the key is
		// being reused for a different amount.
		var original int64
		if err := tx.QueryRow(ctx, `
			SELECT p.amount FROM journal_entries e JOIN postings p ON p.entry_id = e.id
			WHERE e.idempotency_key = $1 AND p.account = $2`, idemKey, WalletAccount(holder)).Scan(&original); err != nil {
			return err
		}
		if original != amount {
			return ErrIdempotencyMismatch
		}
		return nil
	})
	if err != nil {
		return Wallet{}, err
	}

	return b.Wallet(ctx, holder)
}

func (b *Book) Wallet(ctx context.Context, holder string) (Wallet, error) {
	w := Wallet{Holder: holder}
	err := b.pool.QueryRow(ctx, `
		SELECT
			COALESCE((SELECT balance FROM account_balances WHERE account = $2), 0),
			COALESCE((SELECT sum(b.balance) FROM account_balances b
				JOIN journal_entries e ON e.kind = 'hold' AND b.account = 'hold:' || e.lease_id
				WHERE e.holder = $1), 0)`,
		holder, WalletAccount(holder)).Scan(&w.Available, &w.Held)

	return w, err
}

// Statement returns the holder's most recent entries, oldest first.
func (b *Book) Statement(ctx context.Context, holder string, limit int) ([]StatementLine, error) {
	rows, err := b.pool.Query(ctx, `
		SELECT * FROM (
			SELECT e.id, e.kind, COALESCE(e.lease_id::text, ''),
				COALESCE(w.amount, 0), COALESCE(r.amount, 0),
				sum(COALESCE(w.amount, 0)) OVER (ORDER BY e.id),
				e.created_at
			FROM journal_entries e
			LEFT JOIN postings w ON w.entry_id = e.id AND w.account = $2
			LEFT JOIN postings r ON r.entry_id = e.id AND r.account = $3
			WHERE e.holder = $1
		) s
		ORDER BY id DESC LIMIT $4`, holder, WalletAccount(holder), Revenue, limit)
	if err != nil {
		return nil, err
	}

	lines, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (StatementLine, error) {
		var l StatementLine
		err := row.Scan(&l.EntryID, &l.Kind, &l.LeaseID, &l.Amount, &l.Charged, &l.Balance, &l.At)
		return l, err
	})
	slices.Reverse(lines)

	return lines, err
}
