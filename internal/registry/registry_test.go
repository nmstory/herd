package registry

import (
	"testing"

	"github.com/nmstory/herd/internal/config"
)

func testRegistry() *Registry {
	return New(&config.Config{DeviceConfigs: []config.DeviceConfig{
		{ID: "a", Labels: map[string]string{"os": "linux", "zone": "us-east"}},
		{ID: "b", Labels: map[string]string{"os": "linux", "zone": "us-west"}},
		{ID: "c", Labels: map[string]string{"os": "darwin", "zone": "us-east"}},
	}})
}

func TestListDevicesSelector(t *testing.T) {
	r := testRegistry()

	tests := []struct {
		selector map[string]string
		want     int
	}{
		{nil, 3},
		{map[string]string{"os": "linux"}, 2},
		{map[string]string{"os": "linux", "zone": "us-east"}, 1},
		{map[string]string{"os": "windows"}, 0},
	}

	for _, tt := range tests {
		if got := len(r.ListDevices(tt.selector)); got != tt.want {
			t.Errorf("ListDevices(%v) = %d devices, want %d", tt.selector, got, tt.want)
		}
	}
}

func TestHeartbeatUnknownDevice(t *testing.T) {
	if err := testRegistry().Heartbeat("nope"); err == nil {
		t.Fatal("expected error for unknown device")
	}
}
