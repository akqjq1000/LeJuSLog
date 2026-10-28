import asyncio
import gzip
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import Depends, FastAPI, Header, HTTPException, Request

from .clickhouse import insert_logs, make_client
from .config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ingest")


# --- 유틸 ------------------------------------------------------------------
def parse_ts(v):
    """다양한 형태의 타임스탬프를 UTC datetime 으로 변환."""
    if v is None:
        return datetime.now(timezone.utc)
    if isinstance(v, (int, float)):
        return datetime.fromtimestamp(v, tz=timezone.utc)
    s = str(v).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return datetime.now(timezone.utc)


def build_row(obj: dict):
    """수집 레코드(dict) → logs_raw 컬럼 순서의 리스트."""
    fields = obj.get("fields") or {}
    if not isinstance(fields, dict):
        fields = {}
    return [
        parse_ts(obj.get("ts")),
        str(obj.get("host", "")),
        str(obj.get("source", "")),
        str(obj.get("service", "")),
        str(obj.get("level", "")),
        str(obj.get("message", "")),
        {str(k): str(val) for k, val in fields.items()},
    ]


# --- 수집 버퍼 --------------------------------------------------------------
class Ingestor:
    """메모리 버퍼에 쌓았다가 배치로 ClickHouse 에 insert."""

    def __init__(self, client):
        self.client = client
        self.buffer: list = []
        self.lock = asyncio.Lock()
        self.stats = {"received": 0, "inserted": 0, "flushes": 0, "errors": 0}

    async def add(self, rows: list) -> int:
        async with self.lock:
            self.buffer.extend(rows)
            self.stats["received"] += len(rows)
            return len(self.buffer)

    async def flush(self) -> int:
        async with self.lock:
            if not self.buffer:
                return 0
            batch = self.buffer
            self.buffer = []
        try:
            await asyncio.to_thread(insert_logs, self.client, batch)
        except Exception:
            # 실패분은 버퍼 앞으로 되돌려 재전송 시도 (at-least-once)
            async with self.lock:
                self.buffer = batch + self.buffer
                self.stats["errors"] += 1
            raise
        async with self.lock:
            self.stats["inserted"] += len(batch)
            self.stats["flushes"] += 1
        return len(batch)


async def flush_loop(ing: Ingestor):
    while True:
        await asyncio.sleep(settings.flush_interval)
        try:
            await ing.flush()
        except Exception as e:  # noqa: BLE001
            logger.warning("flush 실패 (다음 주기에 재시도): %s", e)


# --- 앱 수명주기 ------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    client = make_client()
    app.state.ingestor = Ingestor(client)
    task = asyncio.create_task(flush_loop(app.state.ingestor))
    logger.info("ingest 시작: ClickHouse %s:%s/%s", settings.ch_host, settings.ch_port, settings.ch_database)
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        try:
            await app.state.ingestor.flush()  # 종료 전 잔여분 flush
        except Exception as e:  # noqa: BLE001
            logger.warning("종료 flush 실패: %s", e)
        client.close()


app = FastAPI(title="LeJuSLog Ingest", lifespan=lifespan)


# --- 인증 -------------------------------------------------------------------
async def require_token(authorization: str = Header(None)):
    if authorization != f"Bearer {settings.ingest_token}":
        raise HTTPException(status_code=401, detail="unauthorized")


# --- 엔드포인트 -------------------------------------------------------------
@app.post("/ingest", dependencies=[Depends(require_token)])
async def ingest(request: Request):
    raw = await request.body()
    if request.headers.get("content-encoding", "").lower() == "gzip":
        try:
            raw = gzip.decompress(raw)
        except Exception:
            raise HTTPException(status_code=400, detail="invalid gzip body")

    rows, rejected = [], 0
    for line in raw.decode("utf-8", "replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            rejected += 1
            continue
        if not isinstance(obj, dict):
            rejected += 1
            continue
        rows.append(build_row(obj))

    buffered = await request.app.state.ingestor.add(rows)
    if buffered >= settings.batch_size:
        asyncio.create_task(request.app.state.ingestor.flush())

    return {"accepted": len(rows), "rejected": rejected, "buffered": buffered}


@app.get("/healthz")
async def healthz(request: Request):
    try:
        await asyncio.to_thread(request.app.state.ingestor.client.command, "SELECT 1")
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"clickhouse unavailable: {e}")
    return {"status": "ok"}


@app.get("/stats")
async def stats(request: Request):
    ing = request.app.state.ingestor
    async with ing.lock:
        return {**ing.stats, "buffered": len(ing.buffer)}
