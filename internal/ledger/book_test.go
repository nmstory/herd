package ledger_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nmstory/herd/internal/db/dbtest"
	"github.com/nmstory/herd/internal/ledger"
	"github.com/nmstory/herd/internal/outbox"
)

func TestTopUp(t *testing.T) {
	ctx := context.Background()
	_, pool := dbtest.Open(t)
	book := ledger.NewBook(pool, outbox.Table{})

	for range 2 { // the retry must not credit twice
		w, err := book.TopUp(ctx, "alice", 500, "k1")
		if err != nil {
			t.Fatal(err)
		}
		if w.Available != 500 {
			t.Errorf("available = %d, want 500", w.Available)
		}
	}

	if _, err := book.TopUp(ctx, "alice", 600, "k1"); !errors.Is(err, ledger.ErrIdempotencyMismatch) {
		t.Errorf("reused key, new amount: got %v, want ErrIdempotencyMismatch", err)
	}
	if _, err := book.TopUp(ctx, "alice", -5, "k2"); !errors.Is(err, ledger.ErrInvalidArgument) {
		t.Errorf("negative amount: got %v, want ErrInvalidArgument", err)
	}

	if _, err := book.TopUp(ctx, "alice", 250, "k2"); err != nil {
		t.Fatal(err)
	}
	lines, err := book.Statement(ctx, "alice", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 2 || lines[0].Balance != 500 || lines[1].Balance != 750 || lines[1].Amount != 250 {
		t.Errorf("statement = %+v, want two top-ups ending at 750", lines)
	}

	if w, err := book.Wallet(ctx, "nobody"); err != nil || w.Available != 0 || w.Held != 0 {
		t.Errorf("unknown holder: got %+v, %v, want an empty wallet", w, err)
	}
}
