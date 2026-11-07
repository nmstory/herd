# Herd

Shepherd a herd of devices. Go control plane for leasing devices from a shared pool over HTTP and gRPC.

## Running

```sh
go run ./cmd/herd                        # in-memory leases (lost on restart)
go run ./cmd/herd -store postgres -database-url postgres://...

go run ./cmd/herdctl list
go run ./cmd/herdctl acquire -selector model=rpi5 -holder alice -key k1
go run ./cmd/herdctl release -holder alice <lease-id>
```

Reusing `-key` makes `acquire` safe to retry: the same holder and key always
return the original lease.

## Credits and events

With the Postgres store, device time costs credits. Acquiring a lease holds
price × duration from the holder's wallet; releasing charges the minutes used
and returns the rest; an expired lease is charged in full. Every movement is a
double-entry journal entry, and every lease change and entry is published to
Kafka through a transactional outbox.

```sh
docker compose up -d --wait               # Postgres + Redpanda (Kafka on localhost:19092)
go run ./cmd/herd -store postgres -kafka localhost:19092 \
    -database-url postgres://postgres:herd@localhost:5432/postgres

go run ./cmd/herdctl wallet topup -holder alice 500
go run ./cmd/herdctl acquire -selector model=jetson-orin -duration 30m -holder alice
go run ./cmd/herdctl release -holder alice <lease-id>
go run ./cmd/herdctl wallet statement -holder alice
go run ./cmd/herdctl events tail          # live from Kafka

go run ./cmd/herd-reconciler -database-url postgres://postgres:herd@localhost:5432/postgres
```

Prices per minute are set by model under `prices:` in `devices.yaml`.
`herd-reconciler` rebuilds every balance from the Kafka events alone and checks
them against Postgres; it exits non-zero if the books don't reconcile.

## Tests and experiments

```sh
docker compose up -d --wait
HERD_TEST_DATABASE_URL=postgres://postgres:herd@localhost:5432/postgres \
HERD_TEST_KAFKA_BROKERS=localhost:19092 go test -race ./...

go run ./cmd/herdbench -store postgres -mode contention -lock skip -database-url postgres://...
go run ./cmd/herdbench -mode wallet [-no-wallet-lock] -database-url postgres://...
go run ./cmd/herdbench -mode events [-dual-write] -database-url postgres://...
go run ./cmd/herdbench -mode lag -database-url postgres://...
```

Postgres tests are skipped when `HERD_TEST_DATABASE_URL` is unset, and Kafka
tests when `HERD_TEST_KAFKA_BROKERS` is unset. Both lease stores run the same
conformance suite in `internal/lease/storetest`.
