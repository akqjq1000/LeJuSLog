//go:build !linux

package main

// journald 는 linux 전용. 다른 OS 빌드에서는 비활성(스텁).
func journaldSources(cfg *Config, sender *Sender) []Source {
	return nil
}
