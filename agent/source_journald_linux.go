//go:build linux

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// journaldSources 는 linux 에서 journald 소스를 구성한다.
func journaldSources(cfg *Config, sender *Sender) []Source {
	if !cfg.Journald.Enabled {
		return nil
	}
	return []Source{NewJournaldSource(cfg, sender)}
}

// JournaldSource 는 `journalctl -o json -f` 를 구동해 systemd journal 을 수집한다.
// 커서는 journalctl 의 --cursor-file 이 직접 영속화한다.
type JournaldSource struct {
	cfg        *Config
	sender     *Sender
	cursorFile string
}

func NewJournaldSource(cfg *Config, sender *Sender) *JournaldSource {
	return &JournaldSource{
		cfg:        cfg,
		sender:     sender,
		cursorFile: filepath.Join(cfg.StateDir, "journald.cursor"),
	}
}

func (j *JournaldSource) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// -n 0: 과거 로그 미출력(커서 없을 때), --cursor-file: 커서 영속화
		args := []string{"-o", "json", "-f", "-n", "0", "--cursor-file=" + j.cursorFile}
		cmd := exec.CommandContext(ctx, "journalctl", args...)
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		if err := cmd.Start(); err != nil {
			log.Printf("journalctl start failed: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		j.consume(ctx, stdout)
		_ = cmd.Wait()
		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(time.Second) // 비정상 종료 시 재기동
		}
	}
}

func (j *JournaldSource) consume(ctx context.Context, r io.Reader) {
	lines := make(chan string, 1024)
	go func() {
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
		for sc.Scan() {
			select {
			case lines <- sc.Text():
			case <-ctx.Done():
				return
			}
		}
		close(lines)
	}()

	batch := make([]Record, 0, j.cfg.BatchSize)
	ticker := time.NewTicker(time.Duration(j.cfg.FlushIntervalSec) * time.Second)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		recs := make([]Record, len(batch))
		copy(recs, batch)
		// 커서는 journalctl 이 영속화하므로 Ack 불필요(nil)
		j.sender.Submit(Batch{Records: recs})
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case <-ticker.C:
			flush()
		case line, ok := <-lines:
			if !ok {
				flush()
				return
			}
			rec, err := j.parse(line)
			if err != nil {
				continue
			}
			batch = append(batch, rec)
			if len(batch) >= j.cfg.BatchSize {
				flush()
			}
		}
	}
}

func (j *JournaldSource) parse(line string) (Record, error) {
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return Record{}, err
	}
	ts := time.Now().UTC()
	if rt, ok := m["__REALTIME_TIMESTAMP"].(string); ok {
		if us, err := strconv.ParseInt(rt, 10, 64); err == nil {
			ts = time.UnixMicro(us).UTC()
		}
	}
	service := asString(m["SYSLOG_IDENTIFIER"])
	if service == "" {
		service = asString(m["_SYSTEMD_UNIT"])
	}
	if service == "" {
		service = j.cfg.Journald.Service
	}
	rec := Record{
		TS:      ts.Format(time.RFC3339Nano),
		Host:    j.cfg.Host,
		Source:  "journald",
		Service: service,
		Level:   priorityToLevel(asString(m["PRIORITY"])),
		Message: asString(m["MESSAGE"]),
		Fields:  map[string]string{},
	}
	if u := asString(m["_SYSTEMD_UNIT"]); u != "" {
		rec.Fields["unit"] = u
	}
	if p := asString(m["_PID"]); p != "" {
		rec.Fields["pid"] = p
	}
	if len(rec.Fields) == 0 {
		rec.Fields = nil
	}
	return rec, nil
}

func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// priorityToLevel 은 syslog PRIORITY(0~7) 를 레벨 문자열로 변환.
func priorityToLevel(p string) string {
	switch p {
	case "0", "1", "2":
		return "crit"
	case "3":
		return "error"
	case "4":
		return "warn"
	case "5", "6":
		return "info"
	case "7":
		return "debug"
	}
	return ""
}
