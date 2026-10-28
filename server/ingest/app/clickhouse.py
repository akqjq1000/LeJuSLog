import clickhouse_connect

from .config import settings

# logs_raw 컬럼 순서 (insert 시 이 순서를 지킨다)
LOG_COLUMNS = ["ts", "host", "source", "service", "level", "message", "fields"]


def make_client():
    """ClickHouse HTTP 클라이언트 생성."""
    return clickhouse_connect.get_client(
        host=settings.ch_host,
        port=settings.ch_port,
        username=settings.ch_user,
        password=settings.ch_password,
        database=settings.ch_database,
    )


def insert_logs(client, rows):
    """원시 로그 배치 insert. rows 는 LOG_COLUMNS 순서의 리스트들."""
    if not rows:
        return
    client.insert("logs_raw", rows, column_names=LOG_COLUMNS)
