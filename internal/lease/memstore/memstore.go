// Package memstore is the in-memory lease store: one mutex around a few maps.
// It's fast and correct within a single process, but every lease is lost on
// restart and it can't be shared between herd replicas.
package memstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/registry"
)

type record struct {
	lease.Lease
	requestHash []byte
	releasedAt  time.Time
}

func (r *record) active(now time.Time) bool {
	return r.releasedAt.IsZero() && now.Before(r.ExpiresAt)
}

type idemKey struct{ holder, key string }

type Store struct {
	reg *registry.Registry
	now func() time.Time

	mu       sync.Mutex
	leases   map[string]*record  // lease ID -> lease
	byDevice map[string]*record  // device ID -> latest lease on it
	byIdem   map[idemKey]*record // (holder, key) -> lease it produced
}

func New(reg *registry.Registry) *Store {
	return &Store{
		reg:      reg,
		now:      time.Now,
		leases:   make(map[string]*record),
		byDevice: make(map[string]*record),
		byIdem:   make(map[idemKey]*record),
	}
}

func (s *Store) ListDevices(_ context.Context, selector map[string]string) ([]lease.Device, error) {
	devices := s.reg.ListDevices(selector)
	sort.Slice(devices, func(i, j int) bool { return devices[i].ID < devices[j].ID })

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	out := make([]lease.Device, 0, len(devices))
	for _, d := range devices {
		r, ok := s.byDevice[d.ID]
		out = append(out, lease.Device{ID: d.ID, Labels: d.Labels, Leased: ok && r.active(now)})
	}

	return out, nil
}

func (s *Store) Acquire(_ context.Context, p lease.AcquireParams) (lease.Lease, error) {
	if err := p.Validate(); err != nil {
		return lease.Lease{}, err
	}
	hash := p.Hash()

	devices := s.reg.ListDevices(p.Selector)
	sort.Slice(devices, func(i, j int) bool { return devices[i].ID < devices[j].ID })

	s.mu.Lock()
	defer s.mu.Unlock()

	if r, ok := s.byIdem[idemKey{p.Holder, p.IdempotencyKey}]; ok {
		if !bytes.Equal(r.requestHash, hash) {
			return lease.Lease{}, lease.ErrIdempotencyMismatch
		}
		return r.Lease, nil
	}

	now := s.now()
	for _, d := range devices {
		if r, ok := s.byDevice[d.ID]; ok && r.active(now) {
			continue
		}

		r := &record{
			Lease: lease.Lease{
				ID:        newID(),
				DeviceID:  d.ID,
				Holder:    p.Holder,
				ExpiresAt: now.Add(p.Duration),
			},
			requestHash: hash,
		}
		s.leases[r.ID] = r
		s.byDevice[d.ID] = r
		s.byIdem[idemKey{p.Holder, p.IdempotencyKey}] = r

		return r.Lease, nil
	}

	return lease.Lease{}, lease.ErrNoDeviceAvailable
}

func (s *Store) Release(_ context.Context, leaseID, holder string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	r, ok := s.leases[leaseID]
	if !ok {
		return lease.ErrLeaseNotFound
	}
	if r.Holder != holder {
		return lease.ErrNotHolder
	}
	if now := s.now(); r.active(now) {
		r.releasedAt = now
	}

	return nil
}

// newID returns a random UUIDv4, matching the IDs Postgres hands out.
func newID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
