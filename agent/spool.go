package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"
)

// Spool 은 전송 실패분을 디스크에 보관했다가 복구 시 재전송하게 한다.
// 각 파일은 gzip 압축된 NDJSON 배치 1개다.
type Spool struct {
	dir string
	seq int64
}

func NewSpool(dir string) (*Spool, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Spool{dir: dir}, nil
}

// Write 는 배치를 임시파일로 쓴 뒤 원자적으로 rename 한다.
func (s *Spool) Write(gz []byte) error {
	n := atomic.AddInt64(&s.seq, 1)
	base := fmt.Sprintf("%020d-%06d.ndjson.gz", time.Now().UnixNano(), n)
	tmp := filepath.Join(s.dir, base+".tmp")
	final := filepath.Join(s.dir, base)
	if err := os.WriteFile(tmp, gz, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, final)
}

// List 는 전송 대기 중인 스풀 파일을 오래된 순으로 반환한다(.tmp 제외).
func (s *Spool) List() ([]string, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) == ".gz" {
			files = append(files, filepath.Join(s.dir, e.Name()))
		}
	}
	sort.Strings(files)
	return files, nil
}
