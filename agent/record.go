package main

// Record 는 수집기(POST /ingest)가 받는 로그 1건.
// 서버의 logs_raw 스키마 및 수집 계약과 1:1로 맞춘다.
type Record struct {
	TS      string            `json:"ts"`
	Host    string            `json:"host"`
	Source  string            `json:"source"`
	Service string            `json:"service"`
	Level   string            `json:"level,omitempty"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields,omitempty"`
}
