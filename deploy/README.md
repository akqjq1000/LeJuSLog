# LeJuSLog 파이프라인 실행 가이드

중앙 수집 파이프라인(ClickHouse + FastAPI 수집기)을 docker compose 로 띄운다.

## 1. 기동

```bash
cd deploy
cp .env.example .env        # 필요 시 토큰/비밀번호 수정
docker compose up -d --build
```

- ClickHouse: `localhost:8123`(HTTP), `localhost:9000`(native)
- 수집기(ingest): `localhost:8080`
- 최초 기동 시 `clickhouse/init/01_schema.sql` 이 실행되어
  `lejuslog.logs_raw`, `lejuslog.log_templates` 테이블이 생성된다.

## 2. 상태 확인

```bash
curl localhost:8080/healthz      # {"status":"ok"}
curl localhost:8080/stats        # 수신/삽입 카운터
```

## 3. 샘플 로그 전송 (에이전트 흉내)

```bash
python ../tools/send_sample.py --count 50
```

응답 예: `200 {"accepted": 50, "rejected": 0, "buffered": 50}`

## 4. ClickHouse 에서 조회

```bash
docker exec -it lejus-clickhouse clickhouse-client \
  --query "SELECT level, count() FROM lejuslog.logs_raw GROUP BY level"
```

또는 HTTP:

```bash
curl "localhost:8123/?query=SELECT%20count()%20FROM%20lejuslog.logs_raw" \
  --user lejus:lejus-pass
```

## 5. 수집 프로토콜

- `POST /ingest`
- 헤더: `Authorization: Bearer <INGEST_TOKEN>`, `Content-Encoding: gzip`
- 본문: gzip 압축된 NDJSON(라인당 1 레코드)
- 레코드 필드: `ts, host, source, service, level, message, fields(object)`

## 6. 정리

```bash
docker compose down          # 컨테이너 중지
docker compose down -v        # 데이터 볼륨까지 삭제
```
