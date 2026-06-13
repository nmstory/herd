package config

import (
	"os"

	"github.com/goccy/go-yaml"
)

type DeviceConfig struct {
	ID     string            `yaml:"id"`
	Labels map[string]string `yaml:"labels"` // map, as it's easily extensible when labels are added/removed
}

type Config struct {
	DeviceConfigs []DeviceConfig `yaml:"devices"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
