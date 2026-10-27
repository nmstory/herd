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

## Tests and experiments

```sh
docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=herd postgres:17
HERD_TEST_DATABASE_URL=postgres://postgres:herd@localhost:5432/postgres go test -race ./...

go run ./cmd/herdbench -store postgres -mode contention -lock skip -database-url postgres://...
```

Postgres tests are skipped when `HERD_TEST_DATABASE_URL` is unset. Both
stores run the same conformance suite in `internal/lease/storetest`.
