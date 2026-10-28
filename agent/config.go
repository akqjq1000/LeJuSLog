package main

import (
	"encoding/json"
	"os"
)

// FileInput 은 tail 할 로그 파일 1개.
type FileInput struct {
	Path    string `json:"path"`
	Service string `json:"service"`
}

// JournaldInput 은 systemd journal 수집 설정.
type JournaldInput struct {
	Enabled bool   `json:"enabled"`
	Service string `json:"service"`
}

// Config 는 에이전트 설정(JSON 파일).
type Config struct {
	ServerURL        string        `json:"server_url"`
	Token            string        `json:"token"`
	Host             string        `json:"host"`
	BatchSize        int           `json:"batch_size"`
	FlushIntervalSec int           `json:"flush_interval_sec"`
	SpoolDir         string        `json:"spool_dir"`
	StateDir         string        `json:"state_dir"`
	Files            []FileInput   `json:"files"`
	Journald         JournaldInput `json:"journald"`
}

// LoadConfig 는 JSON 설정을 읽고 기본값을 채운다.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := &Config{
		BatchSize:        500,
		FlushIntervalSec: 2,
		SpoolDir:         "/var/lib/lejus-agent/spool",
		StateDir:         "/var/lib/lejus-agent/state",
	}
	if err := json.Unmarshal(b, c); err != nil {
		return nil, err
	}
	if c.Host == "" {
		if h, herr := os.Hostname(); herr == nil {
			c.Host = h
		}
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.FlushIntervalSec <= 0 {
		c.FlushIntervalSec = 2
	}
	if c.SpoolDir == "" {
		c.SpoolDir = "/var/lib/lejus-agent/spool"
	}
	if c.StateDir == "" {
		c.StateDir = "/var/lib/lejus-agent/state"
	}
	return c, nil
}
