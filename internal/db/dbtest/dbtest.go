// Package dbtest gives each test its own freshly migrated Postgres schema.
//
// Tests need a Postgres to talk to, e.g.
//
//	docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=herd postgres:17
//	HERD_TEST_DATABASE_URL=postgres://postgres:herd@localhost:5432/postgres go test ./...
//
// and are skipped when HERD_TEST_DATABASE_URL is unset.
package dbtest

import (
	"context"
	"crypto/rand"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nmstory/herd/internal/db"
)

func databaseURL(t testing.TB) string {
	url := os.Getenv("HERD_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("HERD_TEST_DATABASE_URL not set")
	}
	return url
}

// NewSchema creates an empty schema for one test and drops it afterwards, so
// tests never see each other's rows.
func NewSchema(t testing.TB) string {
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, databaseURL(t))
	if err != nil {
		t.Fatal(err)
	}

	schema := "herd_test_" + rand.Text()[:12]
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})

	return schema
}

// OpenPool connects to the given schema, as a separate herd replica would.
func OpenPool(t testing.TB, schema string) *pgxpool.Pool {
	cfg, err := pgxpool.ParseConfig(databaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 32

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	return pool
}

// Open returns a pool on a new, fully migrated schema.
func Open(t testing.TB) (string, *pgxpool.Pool) {
	schema := NewSchema(t)
	pool := OpenPool(t, schema)
	if err := db.Migrate(context.Background(), pool); err != nil {
		t.Fatal(err)
	}

	return schema, pool
}
