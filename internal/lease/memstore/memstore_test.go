package memstore

import (
	"testing"

	"github.com/nmstory/herd/internal/config"
	"github.com/nmstory/herd/internal/lease"
	"github.com/nmstory/herd/internal/lease/storetest"
	"github.com/nmstory/herd/internal/registry"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T, devices []config.DeviceConfig) lease.Store {
		return New(registry.New(&config.Config{DeviceConfigs: devices}))
	})
}
