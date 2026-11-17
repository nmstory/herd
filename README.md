# Herd

Shepherd a herd of devices. Herd is a Go control plane that hands out devices
from a shared pool (Raspberry Pis, Jetsons, VMs, Mac minis) on time-limited
leases over gRPC. Ask for "any `rpi5` for 30 minutes" and you get one that
nobody else can have until you release it or the lease expires.

Two rules hold no matter how many clients, retries or herd replicas there are:

1. **No double lease.** A device has at most one active lease at a time.
2. **Retries are safe.** Acquire takes an idempotency key, and the same holder
   and key always get back the original lease instead of a second device.
   Reusing a key with a different request is rejected.

The story of moving Herd from a mutex to Postgres and Kafka, with the
experiments below, is written up in
[From Mutex to Postgres: What Survives a Restart](https://nmstory.net/posts/postgres/).

## How it works

There are two lease stores behind one `lease.Store` interface, and both pass
the same conformance suite (`internal/lease/storetest`).

- **Memory** (`-store memory`, the default) is one mutex around a few maps.
  It's fast, but every lease is lost on restart and it can't be shared
  between replicas.
- **Postgres** (`-store postgres`) keeps leases in the database, so they
  survive restarts and several herd replicas can share one pool.

In Postgres the rules live in the schema rather than in Go:

- A partial unique index, `leases (device_id) WHERE released_at IS NULL`,
  makes a double lease impossible. Acquire picks a device with
  `FOR UPDATE SKIP LOCKED` so concurrent claimers spread out instead of
  queueing, but that lock only reduces contention. Under READ COMMITTED the
  "is it free?" check can read a stale snapshot, and the index is what catches
  it. Acquire then retries with a fresh snapshot.
- `UNIQUE (holder, idempotency_key)` makes retries safe, and a stored hash of
  the request catches a key reused for something else.
- Expired leases are closed lazily, inside the acquire that reuses their
  device, and all times come from the database clock (`now()`), not from
  each replica's clock.

### Credits

With the Postgres store, device time costs credits, priced per minute by
model under `prices:` in `devices.yaml`. It works like a card
pre-authorisation:

| Event   | What happens                                                        |
|---------|---------------------------------------------------------------------|
| acquire | price × duration is held from the holder's wallet                   |
| release | the minutes used are charged and the rest goes back to the wallet   |
| expiry  | the hold is charged in full, by a background settler or the next acquire |

Every movement is an entry in a double-entry ledger (`internal/ledger`) and is
written in the same transaction as the lease change that causes it. The
schema makes the ledger append-only, rejects entries whose postings don't sum
to zero, and has a `CHECK (balance >= 0)` that makes an overdraft impossible.

### Events

Every lease change and journal entry is published to Kafka (`herd.lease.v1`
and `herd.ledger.v1`, protobuf-encoded) through a transactional outbox. Events
are inserted into an `outbox` table in the same transaction as the change, so
an event exists if and only if the change committed. A relay then publishes
them in order. Delivery is at-least-once, and each record carries its outbox
id in the `herd-event-id` header so consumers can drop repeats. Only one relay
publishes at a time, elected with a Postgres advisory lock.

`herd-reconciler` checks the books from the outside. It rebuilds every balance
from the Kafka events alone, compares them with Postgres, and exits non-zero if
they disagree.

## Running

You need Go and Docker. `docker compose` starts Postgres 17 and Redpanda, which
speaks the Kafka protocol, on `localhost:5432` and `localhost:19092`.

```sh
# In-memory leases, no dependencies (leases are lost on restart)
go run ./cmd/herd

# Postgres leases, credits and Kafka events
docker compose up -d --wait
go run ./cmd/herd -store postgres -kafka localhost:19092 \
    -database-url postgres://postgres:herd@localhost:5432/postgres
```

Then, with `herdctl`:

```sh
go run ./cmd/herdctl list -selector zone=us-east
go run ./cmd/herdctl wallet topup -holder alice 500
go run ./cmd/herdctl acquire -selector model=jetson-orin -duration 30m -holder alice -key k1
go run ./cmd/herdctl release -holder alice <lease-id>
go run ./cmd/herdctl wallet statement -holder alice
go run ./cmd/herdctl events tail          # live from Kafka

go run ./cmd/herd-reconciler -database-url postgres://postgres:herd@localhost:5432/postgres
```

Run `acquire` again with the same `-key` and you get the same lease back, even
after restarting herd on the Postgres store. Without `-key`, herdctl picks a
random one. The wallet commands need the Postgres store.

The devices themselves are listed in `devices.yaml`. Herd reads them at
startup (`-config`) and, with Postgres, adds them to the `devices` table.
Migrations run on startup under an advisory lock, so replicas can start
together.

## Tests

```sh
docker compose up -d --wait
HERD_TEST_DATABASE_URL=postgres://postgres:herd@localhost:5432/postgres \
HERD_TEST_KAFKA_BROKERS=localhost:19092 go test -race ./...
```

Postgres tests are skipped when `HERD_TEST_DATABASE_URL` is unset, and Kafka
tests when `HERD_TEST_KAFKA_BROKERS` is unset. CI runs everything against
the same compose stack. Each test gets its own throwaway schema.

Besides the shared conformance suite, the Postgres tests check that leases
survive a restart, that two replicas with separate connection pools never
double lease, that concurrent spends from one wallet never overdraw it, that
the relay publishes each event once and in order and catches up after
downtime, and that the reconciler catches phantom and conflicting entries.

## Experiments

`herdbench` runs the experiments from [the write-up](https://nmstory.net/posts/postgres/) directly against a store,
without gRPC, in a throwaway schema it drops afterwards.

```sh
DB=postgres://postgres:herd@localhost:5432/postgres
go run ./cmd/herdbench -store memory   -mode latency
go run ./cmd/herdbench -store postgres -mode latency    -database-url $DB
go run ./cmd/herdbench -store postgres -mode contention -database-url $DB -lock skip   # or wait, none
go run ./cmd/herdbench -store postgres -mode contention -database-url $DB -lock none -drop-unique-index
go run ./cmd/herdbench -mode wallet -database-url $DB [-no-wallet-lock]
go run ./cmd/herdbench -mode events -database-url $DB [-dual-write]
go run ./cmd/herdbench -mode lag    -database-url $DB
```

Some results from one machine (Postgres in Docker Desktop on Windows, so the
absolute times are mostly the VM and vary a lot between runs):

| Experiment | Result |
|---|---|
| 100 claimers, 20 devices, `-lock skip` | 128 ms, 0 retries, 0 double leases |
| same, `-lock wait` | 1.7 s, 886 retries, 0 double leases |
| same, `-lock none` | 2.3 s, 1260 retries, 0 double leases |
| `-lock none -drop-unique-index` | **97 double leases** |
| `-lock skip -drop-unique-index` | **8 to 10 double leases** |
| uncontended acquire, memory vs Postgres | ~4 µs vs ~6 ms |
| 1500 operations with 5% crashes, outbox | 0 phantom events, books reconcile |
| same, publishing inside the transaction | 85 phantom events, 55 accounts out of balance |
| relay lag at ~850 events/s | p50 19 ms, p99 264 ms |

The locking strategy changes how fast acquire is. Only the unique index stops
double leases.

## Layout

| Path | What's there |
|---|---|
| `cmd/herd` | the gRPC server |
| `cmd/herdctl` | CLI client, plus `events tail` straight from Kafka |
| `cmd/herdbench` | lease, credits and Kafka experiments |
| `cmd/herd-reconciler` | rebuilds balances from Kafka and checks them against Postgres |
| `api/proto`, `gen/go` | gRPC and event protobufs, and the generated Go |
| `internal/lease` | the `Store` interface, both stores, and the conformance suite |
| `internal/ledger` | double-entry ledger and wallets |
| `internal/outbox` | outbox writer and the Kafka relay |
| `internal/reconcile` | Kafka-vs-Postgres reconciliation |
| `internal/db` | embedded migrations and the migration runner |

## Limits

- **No fencing.** Herd knows when a lease has expired but the device doesn't,
  so a client holding an expired lease can keep using the device.
- **No renewal.** A lease's duration is fixed when it's acquired.
- **`SKIP LOCKED` can return "no device available"** when every free device is
  briefly locked by a transaction that's about to fail.
- **Idempotency keys are never cleaned up.**
- **No TLS or authentication.** The holder is whatever the client says it is.
