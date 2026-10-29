package main

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// FileSource 는 로그 파일을 폴링 tail 한다.
//
// 오프셋(바이트 위치)을 state 파일에 저장해 재시작 시 이어 읽는다.
// 오프셋은 "배치가 내구적으로 수용된 뒤"(Ack)에만 확정되므로,
// 전송 전 크래시 시 재읽기(중복)는 있어도 유실은 없다(at-least-once).
type FileSource struct {
	cfg       *Config
	input     FileInput
	sender    *Sender
	statePath string
	offset    int64 // 다음에 읽을 바이트 위치(메모리상 진행)
}

func NewFileSource(cfg *Config, in FileInput, sender *Sender) *FileSource {
	st := filepath.Join(cfg.StateDir, "file-"+sanitize(in.Path)+".offset")
	fs := &FileSource{cfg: cfg, input: in, sender: sender, statePath: st}
	fs.offset = fs.loadOffset()
	return fs
}

func (fs *FileSource) Run(ctx context.Context) {
	_ = os.MkdirAll(fs.cfg.StateDir, 0o755)
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fs.readOnce()
		}
	}
}

func (fs *FileSource) readOnce() {
	f, err := os.Open(fs.input.Path)
	if err != nil {
		return // 파일이 아직 없을 수 있음 -> 다음 주기에 재시도
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return
	}
	size := info.Size()

	switch {
	case fs.offset < 0:
		// 최초 실행: 과거 로그 폭주를 막기 위해 파일 끝에서 시작
		fs.offset = size
		fs.saveOffset(size)
		return
	case fs.offset > size:
		// 로테이션/트렁케이션 감지 -> 처음부터
		fs.offset = 0
	}
	if fs.offset >= size {
		return // 새 데이터 없음
	}

	if _, err := f.Seek(fs.offset, io.SeekStart); err != nil {
		return
	}
	data := make([]byte, size-fs.offset)
	n, err := io.ReadFull(f, data)
	if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
		return
	}
	data = data[:n]

	last := bytes.LastIndexByte(data, '\n')
	if last < 0 {
		return // 완결된 라인이 아직 없음(부분 라인) -> 다음 주기에
	}
	chunk := data[:last+1]
	base := fs.offset

	batch := make([]Record, 0, fs.cfg.BatchSize)
	flushAt := func(endOff int64) {
		if len(batch) == 0 {
			return
		}
		recs := make([]Record, len(batch))
		copy(recs, batch)
		off := endOff
		fs.sender.Submit(Batch{Records: recs, Ack: func() { fs.saveOffset(off) }})
		fs.offset = off // 메모리상 진행(디스크 확정은 Ack 에서)
		batch = batch[:0]
	}

	start := 0
	for i := 0; i < len(chunk); i++ {
		if chunk[i] != '\n' {
			continue
		}
		line := strings.TrimRight(string(chunk[start:i]), "\r")
		endOff := base + int64(i+1)
		if line != "" {
			batch = append(batch, fs.toRecord(line))
		}
		if len(batch) >= fs.cfg.BatchSize {
			flushAt(endOff)
		}
		start = i + 1
	}
	flushAt(base + int64(len(chunk)))
}

func (fs *FileSource) toRecord(line string) Record {
	return Record{
		TS:      time.Now().UTC().Format(time.RFC3339Nano),
		Host:    fs.cfg.Host,
		Source:  "file",
		Service: fs.input.Service,
		Message: line,
	}
}

func (fs *FileSource) loadOffset() int64 {
	b, err := os.ReadFile(fs.statePath)
	if err != nil {
		return -1 // 최초 실행 신호
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return -1
	}
	return v
}

func (fs *FileSource) saveOffset(off int64) {
	tmp := fs.statePath + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(off, 10)), 0o644); err != nil {
		log.Printf("offset save failed (%s): %v", fs.input.Path, err)
		return
	}
	_ = os.Rename(tmp, fs.statePath)
}

// sanitize 는 파일 경로를 state 파일명으로 쓸 수 있게 변환한다.
func sanitize(p string) string {
	r := strings.NewReplacer("/", "_", "\\", "_", ":", "_", " ", "_")
	return strings.Trim(r.Replace(p), "_")
}
