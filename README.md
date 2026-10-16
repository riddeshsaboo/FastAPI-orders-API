# Orders API (local setup)

## Requirements
- Python 3.10+
- Redis running locally at `localhost:6379`

## Install and run
```bash
cd orders_api_service_local
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m uvicorn main:app --host 127.0.0.1 --port 8000 --reload
```

If Redis runs at another address, set `REDIS_URL` before starting Uvicorn.

## Endpoints
- `GET /healthz` — Redis health check
- `GET /orders?limit=10&cursor=...` — cursor pagination through order IDs 1–57
- `POST /orders` — create an idempotent order; requires `Idempotency-Key`

Every request is rate-limited per `X-Client-Id`: 15 requests per rolling 10-second window. A blocked request receives HTTP 429 and `Retry-After`.
