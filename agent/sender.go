package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"
)

// Batch 는 전송 단위. Ack 는 "내구적으로 수용됨(전송 성공 또는 스풀 기록)"
// 이후 호출되어, 소스가 오프셋/커서를 확정(persist)하게 한다.
type Batch struct {
	Records []Record
	Ack     func()
}

// Sender 는 배치를 받아 gzip NDJSON 으로 수집기에 POST 한다.
// 실패 시 스풀에 적재하고, 별도 루프가 주기적으로 재전송한다(at-least-once).
type Sender struct {
	cfg    *Config
	spool  *Spool
	in     chan Batch
	client *http.Client
}

func NewSender(cfg *Config, spool *Spool) *Sender {
	return &Sender{
		cfg:    cfg,
		spool:  spool,
		in:     make(chan Batch, 64),
		client: &http.Client{Timeout: 15 * time.Second},
	}
}

// Submit 은 배치를 전송 큐에 넣는다.
func (s *Sender) Submit(b Batch) {
	s.in <- b
}

// Run 은 전송 큐를 소비한다.
func (s *Sender) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case b := <-s.in:
			s.handle(ctx, b)
		}
	}
}

func (s *Sender) handle(ctx context.Context, b Batch) {
	if len(b.Records) == 0 {
		if b.Ack != nil {
			b.Ack()
		}
		return
	}
	payload, err := encodeNDJSONGzip(b.Records)
	if err != nil {
		log.Printf("encode error (drop %d records): %v", len(b.Records), err)
		if b.Ack != nil {
			b.Ack()
		}
		return
	}
	if perr := s.post(ctx, payload); perr != nil {
		// 네트워크 실패 -> 스풀에 적재해 내구성 확보
		if werr := s.spool.Write(payload); werr != nil {
			log.Printf("send+spool failed (will retry after restart): send=%v spool=%v", perr, werr)
			return // Ack 하지 않음 -> 재시작 후 소스가 재전송(무손실)
		}
		log.Printf("send failed, spooled %d records: %v", len(b.Records), perr)
	}
	if b.Ack != nil {
		b.Ack()
	}
}

// DrainSpool 은 주기적으로 스풀 파일을 재전송하고 성공분을 삭제한다.
func (s *Sender) DrainSpool(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			files, err := s.spool.List()
			if err != nil {
				continue
			}
			for _, f := range files {
				gz, rerr := os.ReadFile(f)
				if rerr != nil {
					continue
				}
				if perr := s.post(ctx, gz); perr != nil {
					break // 아직 서버 다운 -> 다음 주기에 재시도
				}
				_ = os.Remove(f)
			}
		}
	}
}

func (s *Sender) post(ctx context.Context, gz []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.cfg.ServerURL, bytes.NewReader(gz))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.Token)
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", "gzip")
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("server status %d", resp.StatusCode)
	}
	return nil
}

// encodeNDJSONGzip 은 레코드들을 라인당 1 JSON(NDJSON)으로 직렬화 후 gzip 압축.
func encodeNDJSONGzip(records []Record) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	enc := json.NewEncoder(gz) // Encode 는 객체마다 개행을 붙여 NDJSON 이 된다
	for i := range records {
		if err := enc.Encode(&records[i]); err != nil {
			return nil, err
		}
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
