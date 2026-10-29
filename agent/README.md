# LeJuSLog Agent

로그를 읽어 중앙 수집기(`POST /ingest`)로 전송하는 Go 에이전트.
현재 **Ubuntu 26.04(Linux)** 를 1차 타깃으로 구현되어 있다. (Windows 소스는 추후 추가)

## 기능

- **소스**
  - 파일 tail (`files[]`) — 폴링 방식, 바이트 **오프셋 체크포인트**로 재시작 시 이어 읽기
  - systemd **journald** (`journalctl -o json -f`, `--cursor-file` 로 커서 영속화)
- **전송** — gzip NDJSON + `Bearer` 토큰, 배치 단위 POST
- **신뢰성** — 전송 실패 시 디스크 **스풀**에 적재 후 재전송(at-least-once).
  오프셋은 배치가 내구적으로 수용된 뒤에만 확정 → 유실 없음(중복 가능).

## 빌드

Linux 타깃 바이너리 (Windows/다른 OS에서 교차 컴파일):

```bash
cd agent
GOOS=linux GOARCH=amd64 go build -o bin/lejus-agent .
```

Ubuntu 머신에서 직접 빌드:

```bash
cd agent && go build -o bin/lejus-agent .
```

> 외부 의존성 없이 표준 라이브러리만 사용한다.

## 설정

`config.example.json` 참고. 핵심 필드:

| 필드 | 설명 |
|---|---|
| `server_url` | 수집기 주소 (예: `http://192.168.0.10:8080/ingest`) |
| `token` | 수집기 `INGEST_TOKEN` 과 동일 |
| `host` | 비우면 OS 호스트명 사용 |
| `files[]` | `{path, service}` tail 대상 |
| `journald` | `{enabled, service}` |

## 실행 (수동)

```bash
sudo mkdir -p /etc/lejus-agent /var/lib/lejus-agent
sudo cp config.example.json /etc/lejus-agent/config.json
sudo $EDITOR /etc/lejus-agent/config.json     # server_url/token 수정
sudo ./bin/lejus-agent --config /etc/lejus-agent/config.json
```

## 서비스 등록 (systemd)

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin lejus-agent
sudo cp bin/lejus-agent /usr/local/bin/
sudo cp systemd/lejus-agent.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now lejus-agent
journalctl -u lejus-agent -f
```

권한 메모: `/var/log/*` 읽기는 `adm` 그룹, journald 읽기는 `systemd-journal` 그룹이
필요하다(유닛 파일의 `SupplementaryGroups`). 간단 테스트는 root 로 실행해도 된다.

## 동작 확인

에이전트 기동 후 로그를 유발하면 수집기 통계에 반영된다:

```bash
logger "lejuslog agent test $(date)"      # journald/syslog 에 1건 기록
curl -s http://<server>:8080/stats
```
