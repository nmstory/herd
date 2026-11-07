package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nmstory/herd/internal/db/dbtest"
	"github.com/nmstory/herd/internal/ledger"
)

func TestValidate(t *testing.T) {
	tests := map[string][]ledger.Posting{
		"one posting":  {{"a", 1}},
		"unbalanced":   {{"a", 5}, {"b", -4}},
		"zero amount":  {{"a", 0}, {"b", 0}},
		"same account": {{"a", 5}, {"a", -5}},
	}
	for name, postings := range tests {
		if err := (ledger.Entry{Postings: postings}).Validate(); !errors.Is(err, ledger.ErrUnbalanced) {
			t.Errorf("%s: got %v, want ErrUnbalanced", name, err)
		}
	}

	ok := ledger.Entry{Postings: []ledger.Posting{{"a", 5}, {"b", -3}, {"c", -2}}}
	if err := ok.Validate(); err != nil {
		t.Errorf("balanced entry: %v", err)
	}
}

func topUp(holder string, amount int64, key string) ledger.Entry {
	return ledger.Entry{
		Kind: ledger.KindTopUp, Holder: holder, IdempotencyKey: key,
		Postings: []ledger.Posting{{ledger.External, -amount}, {ledger.WalletAccount(holder), amount}},
	}
}

func spend(holder string, amount int64, key string) ledger.Entry {
	return ledger.Entry{
		Kind: ledger.KindCapture, Holder: holder, IdempotencyKey: key,
		Postings: []ledger.Posting{{ledger.WalletAccount(holder), -amount}, {ledger.Revenue, amount}},
	}
}

func post(ctx context.Context, pool *pgxpool.Pool, e ledger.Entry) (posted bool, err error) {
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		_, posted, err = ledger.Post(ctx, tx, e)
		return err
	})
	return posted, err
}

func balance(t *testing.T, pool *pgxpool.Pool, account string) int64 {
	t.Helper()
	b, err := ledger.Balance(context.Background(), pool, account)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestPostIsIdempotent(t *testing.T) {
	ctx := context.Background()
	_, pool := dbtest.Open(t)

	for i, want := range []bool{true, false} {
		posted, err := post(ctx, pool, topUp("alice", 100, "topup:alice:1"))
		if err != nil {
			t.Fatal(err)
		}
		if posted != want {
			t.Errorf("attempt %d: posted = %v, want %v", i+1, posted, want)
		}
	}

	if b := balance(t, pool, ledger.WalletAccount("alice")); b != 100 {
		t.Errorf("wallet = %d, want 100", b)
	}
	if b := balance(t, pool, ledger.External); b != -100 {
		t.Errorf("external = %d, want -100", b)
	}
}

func TestOverdraftIsRejected(t *testing.T) {
	ctx := context.Background()
	_, pool := dbtest.Open(t)

	if _, err := post(ctx, pool, topUp("alice", 10, "t1")); err != nil {
		t.Fatal(err)
	}
	if _, err := post(ctx, pool, spend("alice", 11, "s1")); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("got %v, want ErrInsufficientFunds", err)
	}
	if b := balance(t, pool, ledger.WalletAccount("alice")); b != 10 {
		t.Errorf("wallet = %d after rejected spend, want 10", b)
	}
}

// Concurrent spends from one wallet serialise on its balance row, and the
// CHECK constraint refuses the first one that would go negative.
func TestConcurrentSpendsNeverOverdraw(t *testing.T) {
	ctx := context.Background()
	_, pool := dbtest.Open(t)

	if _, err := post(ctx, pool, topUp("alice", 10, "t1")); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var ok, refused atomic.Int64
	for i := range 50 {
		wg.Go(func() {
			_, err := post(ctx, pool, spend("alice", 1, fmt.Sprint("s", i)))
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ledger.ErrInsufficientFunds):
				refused.Add(1)
			default:
				t.Error(err)
			}
		})
	}
	wg.Wait()

	if ok.Load() != 10 || refused.Load() != 40 {
		t.Errorf("%d spends succeeded and %d refused, want 10 and 40", ok.Load(), refused.Load())
	}
	if b := balance(t, pool, ledger.WalletAccount("alice")); b != 0 {
		t.Errorf("wallet = %d, want 0", b)
	}
}

// The schema rejects an unbalanced entry even when Post's checks are
// bypassed entirely.
func TestDatabaseRejectsUnbalancedEntry(t *testing.T) {
	ctx := context.Background()
	_, pool := dbtest.Open(t)

	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var id int64
		if err := tx.QueryRow(ctx, `INSERT INTO journal_entries (kind, holder, idempotency_key)
			VALUES ('topup', 'mallory', 'free-money') RETURNING id`).Scan(&id); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO postings (entry_id, account, amount) VALUES ($1, 'wallet:mallory', 1000000)`, id)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "unbalanced") {
		t.Fatalf("got %v, want the commit to fail as unbalanced", err)
	}
	if b := balance(t, pool, ledger.WalletAccount("mallory")); b != 0 {
		t.Errorf("mallory's wallet = %d, want 0", b)
	}
}

func TestLedgerIsAppendOnly(t *testing.T) {
	ctx := context.Background()
	_, pool := dbtest.Open(t)

	if _, err := post(ctx, pool, topUp("alice", 10, "t1")); err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`UPDATE postings SET amount = amount * 100`,
		`DELETE FROM postings`,
		`DELETE FROM journal_entries`,
	} {
		if _, err := pool.Exec(ctx, sql); err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Errorf("%s: got %v, want append-only error", sql, err)
		}
	}
}

// Random concurrent top-ups, spends and transfers must leave the books
// consistent: everything sums to zero, no wallet is negative, and every
// running balance equals the sum of its postings.
func TestRandomTrafficKeepsBooksBalanced(t *testing.T) {
	ctx := context.Background()
	_, pool := dbtest.Open(t)
	holders := []string{"alice", "bob", "carol"}

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			r := rand.New(rand.NewPCG(uint64(w), 1))
			for i := range 50 {
				a, b := holders[r.IntN(3)], holders[r.IntN(3)]
				amount := int64(r.IntN(20) + 1)
				key := fmt.Sprintf("w%d-%d", w, i)

				var e ledger.Entry
				switch r.IntN(3) {
				case 0:
					e = topUp(a, amount, key)
				case 1:
					e = spend(a, amount, key)
				default:
					if a == b {
						continue
					}
					e = ledger.Entry{Kind: ledger.KindCapture, Holder: a, IdempotencyKey: key,
						Postings: []ledger.Posting{{ledger.WalletAccount(a), -amount}, {ledger.WalletAccount(b), amount}}}
				}

				if _, err := post(ctx, pool, e); err != nil && !errors.Is(err, ledger.ErrInsufficientFunds) {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()

	var total int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(sum(amount), 0) FROM postings`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 0 {
		t.Errorf("all postings sum to %d, want 0", total)
	}

	var drifted int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM account_balances b
		WHERE b.balance <> (SELECT sum(amount) FROM postings p WHERE p.account = b.account)`).Scan(&drifted); err != nil {
		t.Fatal(err)
	}
	if drifted != 0 {
		t.Errorf("%d running balances disagree with their postings", drifted)
	}
}
