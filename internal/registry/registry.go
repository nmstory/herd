package registry

import (
	"fmt"
	"sync"
	"time"

	"github.com/nmstory/herd/internal/config"
)

type DeviceState int

const (
	Idle DeviceState = iota
	Leased
	Unhealthy
	Offline
)

type Device struct {
	ID            string
	Labels        map[string]string
	State         DeviceState
	LastHeartbeat time.Time
}

type Registry struct {
	Mutex   sync.RWMutex
	Devices map[string]*Device
}

func New(cfg *config.Config) *Registry {
	r := &Registry{
		Devices: make(map[string]*Device, len(cfg.DeviceConfigs)),
	}

	for _, dc := range cfg.DeviceConfigs {
		r.Devices[dc.ID] = &Device{ID: dc.ID, Labels: dc.Labels, State: Idle}
	}

	return r
}

func (r *Registry) Register(d *Device) {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	r.Devices[d.ID] = d
}

func (r *Registry) Heartbeat(id string) error {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	d, ok := r.Devices[id]
	if !ok {
		return fmt.Errorf("unknown device %q", id)
	}
	d.LastHeartbeat = time.Now()

	return nil
}

// Matches reports whether every key/value in the selector is present in labels.
// An empty selector matches everything.
func Matches(labels, selector map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}

	return true
}

func (r *Registry) ListDevices(selector map[string]string) []*Device {
	r.Mutex.RLock()
	defer r.Mutex.RUnlock()

	var out []*Device

	for _, d := range r.Devices {
		if Matches(d.Labels, selector) {
			out = append(out, d)
		}
	}

	return out
}
