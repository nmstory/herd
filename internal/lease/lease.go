// Package lease defines the storage contract for device leases. Every backend
// (in-memory, Postgres) must satisfy the same guarantees, checked by the
// conformance suite in package storetest:
//
//   - A device has at most one active lease at a time.
//   - Acquire is idempotent per (holder, idempotency key): a retry returns the
//     original lease rather than claiming a second device.
//   - Reusing a key with a different request is rejected.
//   - Expired leases free their device for the next claimer.
package lease

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"time"
)

var (
	ErrInvalidArgument     = errors.New("invalid argument")
	ErrNoDeviceAvailable   = errors.New("no matching device available")
	ErrLeaseNotFound       = errors.New("lease not found")
	ErrNotHolder           = errors.New("lease is held by another client")
	ErrIdempotencyMismatch = errors.New("idempotency key reused with a different request")
)

type Device struct {
	ID     string
	Labels map[string]string
	Leased bool
}

type Lease struct {
	ID        string
	DeviceID  string
	Holder    string
	ExpiresAt time.Time
}

type AcquireParams struct {
	Selector       map[string]string
	Duration       time.Duration
	Holder         string
	IdempotencyKey string
}

func (p AcquireParams) Validate() error {
	switch {
	case p.Holder == "":
		return fmt.Errorf("%w: holder is required", ErrInvalidArgument)
	case p.IdempotencyKey == "":
		return fmt.Errorf("%w: idempotency key is required", ErrInvalidArgument)
	case p.Duration <= 0:
		return fmt.Errorf("%w: duration must be positive", ErrInvalidArgument)
	}

	return nil
}

// Hash fingerprints the parts of the request that must match on a retry.
// Holder and key aren't included: they're how the original request is found.
func (p AcquireParams) Hash() []byte {
	keys := make([]string, 0, len(p.Selector))
	for k := range p.Selector {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		fmt.Fprintf(h, "%q=%q\n", k, p.Selector[k])
	}
	fmt.Fprintf(h, "duration=%d\n", p.Duration)

	return h.Sum(nil)
}

type Store interface {
	ListDevices(ctx context.Context, selector map[string]string) ([]Device, error)
	Acquire(ctx context.Context, p AcquireParams) (Lease, error)
	// Release ends a lease early. Releasing a lease that has already ended is
	// a no-op, so clients can retry it safely.
	Release(ctx context.Context, leaseID, holder string) error
}
