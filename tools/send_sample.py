#!/usr/bin/env python3
"""수집기 테스트용 샘플 로그 전송기 (에이전트 흉내).

표준 라이브러리만 사용한다.

    python tools/send_sample.py                 # 기본 20건 전송
    python tools/send_sample.py --count 100
    python tools/send_sample.py --url http://localhost:8080/ingest --token dev-secret-token
"""
import argparse
import gzip
import json
import random
import socket
import urllib.request
from datetime import datetime, timezone

SERVICES = ["nginx", "sshd", "postgres", "app"]
LEVELS = ["debug", "info", "warn", "error"]
MESSAGES = [
    "GET /api/logs 200 12ms",
    "connection accepted from 10.0.0.5",
    "slow query: SELECT * FROM events (1420ms)",
    "failed login for user admin",
    "worker started pid=4821",
]


def make_record(host: str) -> dict:
    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "host": host,
        "source": random.choice(["journald", "eventlog", "file"]),
        "service": random.choice(SERVICES),
        "level": random.choice(LEVELS),
        "message": random.choice(MESSAGES),
        "fields": {"pid": str(random.randint(100, 9999))},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8080/ingest")
    ap.add_argument("--token", default="dev-secret-token")
    ap.add_argument("--count", type=int, default=20)
    args = ap.parse_args()

    host = socket.gethostname()
    ndjson = "\n".join(json.dumps(make_record(host)) for _ in range(args.count))
    body = gzip.compress(ndjson.encode("utf-8"))

    req = urllib.request.Request(
        args.url,
        data=body,
        headers={
            "Authorization": f"Bearer {args.token}",
            "Content-Type": "application/x-ndjson",
            "Content-Encoding": "gzip",
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        print(resp.status, resp.read().decode())


if __name__ == "__main__":
    main()
