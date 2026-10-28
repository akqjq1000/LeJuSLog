import os


class Settings:
    """환경변수 기반 설정. docker-compose 에서 주입된다."""

    # ClickHouse 접속 (clickhouse-connect 는 HTTP 포트 8123 사용)
    ch_host: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    ch_port: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    ch_user: str = os.getenv("CLICKHOUSE_USER", "default")
    ch_password: str = os.getenv("CLICKHOUSE_PASSWORD", "")
    ch_database: str = os.getenv("CLICKHOUSE_DB", "lejuslog")

    # 수집 인증 토큰 (에이전트가 Bearer 로 전송)
    ingest_token: str = os.getenv("INGEST_TOKEN", "dev-secret-token")

    # 배치 insert 튜닝
    batch_size: int = int(os.getenv("BATCH_SIZE", "1000"))
    flush_interval: float = float(os.getenv("FLUSH_INTERVAL", "2.0"))


settings = Settings()
