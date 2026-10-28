-- LeJuSLog ClickHouse 스키마
-- /docker-entrypoint-initdb.d 에 마운트되어 최초 기동 시 1회 실행된다.

CREATE DATABASE IF NOT EXISTS lejuslog;

-- 원시 로그 ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lejuslog.logs_raw
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
TTL toDateTime(ts) + INTERVAL 30 DAY;   -- 토이용 보존기간 자동 관리

-- AI 파싱 결과 / 템플릿 캐시 --------------------------------------------------
CREATE TABLE IF NOT EXISTS lejuslog.log_templates
(
    template_id   String,          -- Drain3 클러스터 식별자
    template      String,          -- 마스킹된 템플릿 문자열
    schema_json   String,          -- LLM이 추출한 필드 스키마(JSON)
    severity      LowCardinality(String),
    summary       String,
    updated_at    DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY template_id;
