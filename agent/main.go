package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// Source 는 로그 공급원(파일 tail, journald 등)의 공통 인터페이스.
type Source interface {
	Run(ctx context.Context)
}

func main() {
	cfgPath := flag.String("config", "/etc/lejus-agent/config.json", "설정 파일 경로")
	flag.Parse()

	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("설정 로드 실패: %v", err)
	}
	if cfg.ServerURL == "" {
		log.Fatal("server_url 이 필요합니다")
	}

	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		log.Fatalf("state 디렉터리 생성 실패: %v", err)
	}
	spool, err := NewSpool(cfg.SpoolDir)
	if err != nil {
		log.Fatalf("스풀 초기화 실패: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sender := NewSender(cfg, spool)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); sender.Run(ctx) }()
	go func() { defer wg.Done(); sender.DrainSpool(ctx) }()

	var sources []Source
	for _, fi := range cfg.Files {
		sources = append(sources, NewFileSource(cfg, fi, sender))
	}
	sources = append(sources, journaldSources(cfg, sender)...)

	for _, s := range sources {
		wg.Add(1)
		go func(src Source) { defer wg.Done(); src.Run(ctx) }(s)
	}
	log.Printf("lejus-agent 시작: 소스 %d개 -> %s", len(sources), cfg.ServerURL)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("종료 중...")
	cancel()
	wg.Wait()
}
