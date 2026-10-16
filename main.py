import base64
import json
import os
import time
import uuid
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from redis.asyncio import Redis

# For local execution, Redis should be running on localhost:6379.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
TOTAL_ORDERS = 57
RATE_LIMIT = 15
RATE_WINDOW_SECONDS = 10
IDEMPOTENCY_TTL_SECONDS = 24 * 60 * 60

redis_client: Redis = Redis.from_url(REDIS_URL, decode_responses=True)
app = FastAPI(title="Orders API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)

RATE_LIMIT_SCRIPT = redis_client.register_script("""
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local max_requests = tonumber(ARGV[3])
local member = ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, '-inf', now_ms - window_ms)
local count = redis.call('ZCARD', key)
if count >= max_requests then
    local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
    local retry_ms = window_ms
    if oldest[2] then
        retry_ms = math.max(1, tonumber(oldest[2]) + window_ms - now_ms)
    end
    redis.call('PEXPIRE', key, window_ms)
    return {0, retry_ms}
end
redis.call('ZADD', key, now_ms, member)
redis.call('PEXPIRE', key, window_ms)
return {1, 0}
""")


def encode_cursor(offset: int) -> str:
    raw = f"orders:{offset}".encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> int:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        prefix, value = raw.split(":", 1)
        offset = int(value)
        if prefix != "orders" or not 0 <= offset <= TOTAL_ORDERS:
            raise ValueError
        return offset
    except (ValueError, UnicodeDecodeError, base64.binascii.Error):
        raise HTTPException(status_code=400, detail="Invalid cursor")


@app.middleware("http")
async def per_client_rate_limit(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id", "").strip() or "anonymous"
    result = await RATE_LIMIT_SCRIPT(
        keys=[f"rate-limit:{client_id}"],
        args=[int(time.time() * 1000), RATE_WINDOW_SECONDS * 1000,
              RATE_LIMIT, str(uuid.uuid4())],
    )
    allowed, retry_ms = int(result[0]), int(result[1])
    if not allowed:
        return JSONResponse(
            status_code=429,
            content={"detail": "Rate limit exceeded"},
            headers={"Retry-After": str(max(1, (retry_ms + 999) // 1000))},
        )
    return await call_next(request)


@app.get("/healthz")
async def healthz():
    try:
        await redis_client.ping()
        return {"status": "ok", "redis": "up"}
    except Exception:
        raise HTTPException(status_code=503, detail="Redis unavailable")


@app.post("/orders", status_code=201)
async def create_order(
    body: dict[str, Any] | None = None,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    if not idempotency_key or not idempotency_key.strip():
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")

    key = f"idempotency:{idempotency_key.strip()}"
    existing = await redis_client.get(key)
    if existing:
        return JSONResponse(status_code=201, content=json.loads(existing))

    order = {"id": str(uuid.uuid4()), **(body or {})}
    serialized = json.dumps(order, separators=(",", ":"))
    created = await redis_client.set(
        key, serialized, nx=True, ex=IDEMPOTENCY_TTL_SECONDS
    )
    if not created:
        existing = await redis_client.get(key)
        if existing:
            return JSONResponse(status_code=201, content=json.loads(existing))
        raise HTTPException(status_code=503, detail="Could not resolve idempotency key")

    return JSONResponse(status_code=201, content=order)


@app.get("/orders")
async def list_orders(
    limit: int = Query(default=10, ge=1, le=100),
    cursor: str | None = Query(default=None),
):
    offset = decode_cursor(cursor) if cursor else 0
    ids = list(range(1, TOTAL_ORDERS + 1))[offset:offset + limit]
    items = [{"id": order_id, "status": "created"} for order_id in ids]
    next_offset = offset + len(items)
    next_cursor = encode_cursor(next_offset) if next_offset < TOTAL_ORDERS else None
    return {"items": items, "next_cursor": next_cursor}


@app.on_event("shutdown")
async def close_redis():
    await redis_client.aclose()
