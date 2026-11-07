// Package ledger posts double-entry journal entries inside a caller's
// transaction, so a money movement commits or rolls back together with the
// lease change that caused it.
//
// The rules are enforced twice: here, so callers get a clear error, and in
// the schema (migrations/0002_ledger.sql), so no code path can break them.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	External = "external"
	Revenue  = "revenue"
)

func WalletAccount(holder string) string { return "wallet:" + holder }
func HoldAccount(leaseID string) string  { return "hold:" + leaseID }

type Kind string

const (
	KindTopUp   Kind = "topup"
	KindHold    Kind = "hold"
	KindCapture Kind = "capture"
	KindExpire  Kind = "expire"
)

var (
	ErrUnbalanced        = errors.New("journal entry does not balance")
	ErrInsufficientFunds = errors.New("insufficient credits")
)

type Posting struct {
	Account string
	Amount  int64
}

type Entry struct {
	Kind           Kind
	Holder         string
	LeaseID        string // empty for top-ups
	IdempotencyKey string
	Postings       []Posting
}

func (e Entry) Validate() error {
	if len(e.Postings) < 2 {
		return fmt.Errorf("%w: needs at least two postings", ErrUnbalanced)
	}

	var sum int64
	seen := map[string]bool{}
	for _, p := range e.Postings {
		if p.Amount == 0 {
			return fmt.Errorf("%w: zero posting to %s", ErrUnbalanced, p.Account)
		}
		if seen[p.Account] {
			return fmt.Errorf("%w: %s posted twice", ErrUnbalanced, p.Account)
		}
		seen[p.Account] = true
		sum += p.Amount
	}
	if sum != 0 {
		return fmt.Errorf("%w: postings sum to %d", ErrUnbalanced, sum)
	}

	return nil
}

// materialized reports whether an account keeps a running balance row, and
// so can't go negative. See account_balances in the schema.
func materialized(account string) bool {
	return strings.HasPrefix(account, "wallet:") || strings.HasPrefix(account, "hold:")
}

// Post records the entry and updates balances. If an entry with the same
// idempotency key already exists, Post does nothing and returns posted=false.
func Post(ctx context.Context, tx pgx.Tx, e Entry) (id int64, posted bool, err error) {
	if err := e.Validate(); err != nil {
		return 0, false, err
	}

	var leaseID *string
	if e.LeaseID != "" {
		leaseID = &e.LeaseID
	}
	err = tx.QueryRow(ctx, `
		INSERT INTO journal_entries (kind, holder, lease_id, idempotency_key)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id`, e.Kind, e.Holder, leaseID, e.IdempotencyKey).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}

	// Lock balance rows in a fixed order so two entries touching the same
	// accounts can't deadlock.
	postings := append([]Posting(nil), e.Postings...)
	sort.Slice(postings, func(i, j int) bool { return postings[i].Account < postings[j].Account })

	for _, p := range postings {
		if _, err := tx.Exec(ctx, `INSERT INTO postings (entry_id, account, amount) VALUES ($1, $2, $3)`,
			id, p.Account, p.Amount); err != nil {
			return 0, false, err
		}
		if materialized(p.Account) {
			if err := applyBalance(ctx, tx, p); err != nil {
				return 0, false, err
			}
		}
	}

	return id, true, nil
}

func applyBalance(ctx context.Context, tx pgx.Tx, p Posting) error {
	if p.Amount > 0 {
		_, err := tx.Exec(ctx, `
			INSERT INTO account_balances (account, balance) VALUES ($1, $2)
			ON CONFLICT (account) DO UPDATE SET balance = account_balances.balance + excluded.balance`,
			p.Account, p.Amount)
		return err
	}

	// Debits can't use the upsert above: Postgres checks the CHECK constraint
	// against the row the INSERT proposes (a negative balance) before it
	// notices the conflict, so every debit would be refused.
	tag, err := tx.Exec(ctx, `UPDATE account_balances SET balance = balance + $2 WHERE account = $1`,
		p.Account, p.Amount)
	switch {
	case isConstraint(err, "account_balances_non_negative"):
		return ErrInsufficientFunds
	case err != nil:
		return err
	case tag.RowsAffected() == 0:
		return ErrInsufficientFunds // no row yet means a balance of zero
	}

	return nil
}

type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Balance returns an account's balance, which is zero for an account that has
// never been posted to.
func Balance(ctx context.Context, q Querier, account string) (int64, error) {
	var b int64
	err := q.QueryRow(ctx, `SELECT COALESCE(sum(amount), 0) FROM postings WHERE account = $1`, account).Scan(&b)
	return b, err
}

func isConstraint(err error, name string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.ConstraintName == name
}
