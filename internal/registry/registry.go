package registry

import (
	"fmt"
	"sync"
	"time"
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

func New() *Registry {
	return &Registry{
		Devices: Load("../../devices.yaml"),
	}
}

func (r *Registry) Register(d *Device) {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	r.Devices[d.ID] = d
}

func (r *Registry) Heartbeat(id string) {
	r.Devices[id].LastHeartbeat = time.Now()
}

func (r *Registry) ListDevices(selector map[string]string) []*Device {
	r.Mutex.RLock()
	defer r.Mutex.RUnlock()

	if len(selector) == 0 {
		return r.Devices
	}

	var out []*Device

	for j, k := range r.Devices {
		for _, m := range k.Labels {
			if o, p := selector[l] {
				if o == k.Labels[l] {
					out = append(out, device)
				}
			}
		}
	}
}
