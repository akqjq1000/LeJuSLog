# LeJuSLog — 로그 수집·분석 토이 프로젝트 설계서

> 중앙 서버로 로그를 수집·저장하고, 로컬 LLM으로 파싱·분석하여 대시보드로 보여주는
> Server-Agent 구조의 토이 프로젝트.

- 최종 수정: 2026-10-28
- 상태: 설계 (구현 전)

---

## 1. 목표와 범위

### 목표
- OS별(Ubuntu 24.04 / Windows 11) 로그 수집 **에이전트를 직접 구현**한다.
- 수집된 로그를 중앙 서버(ClickHouse)에 저장한다.
- **로컬 LLM**을 활용해 비정형 로그를 구조화·분류·요약한다.
- GUI 대시보드와 조회 API로 로그를 탐색·분석한다.

### 범위 (토이 프로젝트 전제)
- 대상 머신: **Ubuntu 24.04 1대 + Windows 11 Pro 1대**, 총 2대.
- 처리량·고가용성·수평 확장은 목표가 아니다. 단, 로그 유실 방지(at-least-once)는
  학습 포인트로 포함한다.
- 중앙 서버 스택은 `docker compose`로 단일 호스트에서 구동한다.
- 에이전트와 로컬 LLM 서버는 compose 밖에서 각 머신에 배포한다.

### 비범위
- 멀티테넌시, RBAC, 클러스터링, Kafka 등 대규모 버퍼링.
- 외부(클라우드) LLM 사용 — 본 프로젝트는 로컬 LLM만 사용.

---

## 2. 전체 아키텍처

```
[Ubuntu 24.04 Agent (Go)] ┐
                          ├─(HTTP batch: JSON lines + gzip + token)─> [FastAPI 수집기] ─> [ClickHouse]
[Windows 11 Agent (Go)]   ┘                                                │
                                                                           │
                                   [AI 파서 워커] ──(OpenAI 호환 API)──> [Mac mini 로컬 LLM]
                                        │  Drain3 선클러스터링 → 새 템플릿만 LLM → 캐시
                                        └───────────────> [ClickHouse] (구조화 결과)
                                                                           │
                                                      [FastAPI 조회 API] <─┘
                                                                │
                                                      [React + shadcn/ui 대시보드]
```

데이터 흐름:
1. 에이전트가 로컬 로그 소스를 읽어 배치로 중앙 수집기에 전송.
2. 수집기는 원시 로그를 ClickHouse에 배치 insert.
3. AI 워커가 원시 로그를 Drain3로 템플릿 클러스터링 → 새 템플릿만 로컬 LLM으로 구조화.
4. 구조화 결과(필드/심각도/요약)를 ClickHouse에 저장, 캐시.
5. 조회 API가 ClickHouse를 쿼리, 대시보드가 시각화.

---

## 3. 기술 스택

| 구성요소 | 기술 | 선택 이유 |
|---|---|---|
| 에이전트 | **Go** | 단일 정적 바이너리, 크로스 컴파일(`GOOS`), 런타임 의존성 없음 |
| 전송 | **HTTP 배치(JSON lines + gzip) + 토큰 인증** | 양 끝을 직접 구현·2대 규모 → 디버깅 쉬운 단순 프로토콜이 유리 |
| 중앙 저장소 | **ClickHouse** | 컬럼형, 로그 분석 쿼리 빠름, SQL로 AI 질의 확장 용이 |
| 수집/조회 API | **FastAPI (Python)** | AI/LLM SDK와 동일 언어, async 수집에 적합 |
| AI 파싱 | **Drain3 + 로컬 LLM** | 템플릿 선클러스터링으로 LLM 호출 최소화 |
| 로컬 LLM | **Ollama (또는 LM Studio/MLX)** on Mac mini M4 Pro 64GB | OpenAI 호환 API, 32B 4bit 모델 여유 구동 |
| 대시보드 | **React + Vite + shadcn/ui + Recharts** | 커스텀 분석 뷰, 빠른 개발 |
| 배포 | **docker compose** | 중앙 스택 단일 호스트 구동 |

### 전송 프로토콜에 대한 메모
- 초기에는 OTLP/gRPC도 검토했으나, 머신 2대 + 양 끝 자체 구현 환경에서는
  복잡도 대비 이득이 적어 **HTTP 배치**로 결정.
- gRPC는 학습 목적의 2차 개선 과제로 남겨둔다.

---

## 4. 에이전트 설계 (Go)

### 구조
- OS별 소스를 공통 인터페이스 뒤로 추상화한다.
  ```
  type Source interface {
      Read(ctx) <-chan LogRecord  // 라인/이벤트 스트림
  }
  ```
- 파일: `os_common.go` — 파일 tail (`nxadm/tail`).
- Ubuntu 24.04 (`os_linux.go`):
  - `/var/log/*` 파일 tail.
  - **systemd journal** — `journalctl -o json -f` 파이프(가장 단순) 또는 네이티브
    `coreos/go-systemd`.
- Windows 11 (`os_windows.go`):
  - **Windows Event Log** — `golang.org/x/sys/windows/svc/eventlog`,
    또는 `wevtutil`/ETW.

### 신뢰성 (핵심 학습 포인트)
1. **오프셋 추적(체크포인트)**: 어디까지 읽었는지 로컬 파일에 저장 →
   재시작 시 중복/유실 방지.
2. **로컬 스풀 버퍼**: 네트워크 단절 시 디스크에 적재 → 복구 시 재전송.
   목표 보장 수준은 **at-least-once**.
3. 전송 실패 시 지수 백오프 재시도.

### 배포
- 정적 바이너리 크로스 컴파일.
- Ubuntu: systemd unit 서비스로 등록.
- Windows: Windows 서비스(`golang.org/x/sys/windows/svc`)로 등록.

### 전송 포맷 (초안)
- 엔드포인트: `POST /ingest`
- 본문: gzip 압축된 NDJSON(라인당 1 레코드).
- 헤더: `Authorization: Bearer <token>`, `Content-Encoding: gzip`.
- 레코드 예:
  ```json
  {
    "ts": "2026-10-28T12:34:56.789Z",
    "host": "ubuntu-01",
    "source": "journald",
    "service": "nginx",
    "level": "info",
    "message": "GET /api/logs 200 12ms"
  }
  ```

---

## 5. 중앙 저장소 설계 (ClickHouse)

### 원시 로그 테이블 (초안)
```sql
CREATE TABLE logs_raw
(
    ts        DateTime64(3),
    host      LowCardinality(String),
    source    LowCardinality(String),
    service   LowCardinality(String),
    level     LowCardinality(String),
    message   String,
    fields    Map(String, String)      -- AI/파서가 추출한 구조화 필드
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (host, service, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY;   -- 토이용 자동 삭제
```

### 설계 포인트
- **배치 insert 필수** — 건별 insert 금지. 수집기에서 N건/수초 모아 삽입.
- 자주 쓰는 필드(`level`, `service`)는 컬럼으로, 가변 필드는 `Map`/`JSON` 타입.
- 파티션 + TTL로 보존기간 자동 관리.
- AI 결과 테이블은 템플릿 단위로 캐시(아래 참조).

### AI 결과/템플릿 캐시 테이블 (초안)
```sql
CREATE TABLE log_templates
(
    template_id   String,          -- Drain3 클러스터 식별자
    template      String,          -- 마스킹된 템플릿 문자열
    schema_json   String,          -- LLM이 추출한 필드 스키마
    severity      LowCardinality(String),
    summary       String,
    updated_at    DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY template_id;
```

---

## 6. AI 로그 파싱 설계

### 2단계 하이브리드 전략
로컬 LLM은 비용은 0이지만 **처리량이 유한**하므로 호출을 최소화한다.

1. **Drain3 선클러스터링 (비-LLM)**: 유사 로그를 템플릿으로 묶음.
   동일 패턴 수천 줄 → 템플릿 1개로 압축.
2. **새 템플릿만 LLM 호출**: 처음 보는 템플릿에 대해서만 로컬 LLM으로
   구조화 스키마·심각도·요약 생성.
3. **결과 캐시**: `log_templates`에 저장 → 같은 템플릿 재등장 시 LLM 없이 재사용.
4. (선택) 이상탐지/유사검색: 임베딩 + 벡터 검색(pgvector 또는 ClickHouse 벡터).

### 구조화 출력 강제
- Ollama `format: "json"` 또는 JSON schema / GBNF 문법으로 **유효한 JSON만** 생성.
- 파싱 실패 제거가 품질 안정성의 핵심.

---

## 7. 로컬 LLM 설계 (Mac mini M4 Pro, 64GB)

- **런타임**:
  - 1순위 **Ollama** — OpenAI 호환 API 자동 제공, FastAPI에서 `base_url`만 교체.
  - 대안 **LM Studio / MLX** — Apple Silicon 네이티브, 토큰 속도 우위.
- **모델**:
  - 메인(구조화 추출/분류): **Qwen2.5-32B-Instruct** 4bit (속도 우선 시 14B).
  - 보조(고볼륨 템플릿 라벨링): 7~8B 소형 모델. 어려운 케이스만 메인 모델로.
- **네트워크**: Mac mini를 LAN 추론 서버로(`OLLAMA_HOST=0.0.0.0`),
  AI 워커가 OpenAI 호환 엔드포인트로 호출.
- 64GB 통합메모리 → 32B 4bit + 컨텍스트 여유 구동 가능.

---

## 8. 대시보드 / API 설계

- **백엔드 (FastAPI)**:
  - 수집: `POST /ingest` (에이전트 → ClickHouse 배치 insert).
  - 조회: 로그 검색/필터/집계, 템플릿·심각도·요약 조회 API.
- **프런트엔드**: React + Vite + shadcn/ui, 차트는 Recharts(또는 ECharts).
- **지름길 옵션**: 초기엔 Grafana를 대시보드로 쓰고, 차별화 기능(AI 분석 뷰)만
  React로 직접 구현 가능.

---

## 9. 디렉터리 구조

```
LeJuSLog/
├─ agent/              # Go. 공통 코드 + os_linux.go / os_windows.go
├─ server/
│  ├─ ingest/          # FastAPI 수집 엔드포인트 → ClickHouse 배치 insert
│  ├─ api/             # 대시보드용 조회 API
│  └─ aiworker/        # Drain3 + 로컬 LLM 호출 파서
├─ dashboard/          # React + Vite + shadcn/ui
├─ deploy/
│  └─ docker-compose.yml   # clickhouse + ingest + api + aiworker
├─ docs/
│  └─ DESIGN.md        # 본 문서
└─ README.md
```
(에이전트와 Mac mini LLM은 compose 밖 — 각 머신에서 실행)

---

## 10. 구현 로드맵 (제안)

1. **파이프라인 뼈대**: docker-compose(ClickHouse) + FastAPI 수집기 + 스키마.
   수동으로 샘플 로그를 넣어 저장→조회까지 확인.
2. **Go 에이전트**: 파일 tail → 오프셋 → 스풀 → HTTP 전송. Ubuntu 먼저, Windows 확장.
3. **AI 파서**: Drain3 클러스터링 → 로컬 LLM 구조화 → 캐시.
4. **대시보드**: 조회 API + React 뷰.
5. (선택) 이상탐지·임베딩, gRPC 전송, Grafana 연동.

---

## 11. 열린 질문 / 추후 결정

- 로그 레코드 공통 스키마 최종 확정(타임존, 멀티라인 스택트레이스 처리).
- 에이전트 설정 배포 방식(파일/환경변수) 및 수집 대상 경로 지정 방법.
- LLM 구조화 스키마의 표준화 범위(서비스별 커스텀 vs 공통).
- 인증 토큰 관리/로테이션 방식.
