# FastAPI Runbook — deng-hydro-climate

**Last updated:** 2026-05-29 — Session 33
**Location in repo:** `docs/runbook/fastapi_runbook.md`

---

## 1. Overview

FastAPI serves the public REST API at `https://api.deng.ee`. It reads from the `api` schema in `hydro_climate_db` — a dedicated dbt serving layer separate from the gold schema. The service runs in the `deng-fastapi` container behind a Caddy reverse proxy.

| Item | Detail |
|---|---|
| Container | `deng-fastapi` |
| Internal port | 8000 |
| Public URL | https://api.deng.ee |
| Swagger UI | https://api.deng.ee/docs |
| Rate limit | 60 requests/minute per IP |
| Authentication | None — open public API |
| Source files | `docker/fastapi/main.py`, `docker/fastapi/Dockerfile`, `docker/fastapi/requirements.txt` |

---

## 2. Restart and Rebuild

### Restart (config change or container crash)

```bash
cd /srv/deng-hydro-climate
docker compose up -d --build fastapi
```

Always use `--build` — the container is built from `docker/fastapi/` and changes to `main.py` or `requirements.txt` require a rebuild to take effect.

### Check container status and logs

```bash
# Status
docker compose ps deng-fastapi

# Live logs
docker compose logs -f deng-fastapi

# Last 50 lines
docker compose logs --tail=50 deng-fastapi
```

### Health check

```bash
curl http://localhost:8000/health
```

Expected response: `{"status": "ok"}`

> If the container shows `unhealthy`, the default 24h time window is the likely cause — the health check may be querying for data that doesn't exist in that window. See Section 6 — Pending Fixes.

---

## 3. Endpoints

### Station endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/v1/stations/hydro` | All 76 hydrometric stations |
| GET | `/v1/stations/hydro/{station_code}` | Single hydrometric station by code |
| GET | `/v1/stations/meteo` | All 25 meteorological stations |
| GET | `/v1/stations/meteo/{station_code}` | Single meteorological station by code |
| GET | `/v1/elements` | All measurement element codes |
| GET | `/health` | Health check — not in Swagger schema |

### Observation endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/v1/observations/hydro` | Hydro observations — filtered by parameters below |
| GET | `/v1/observations/hydro/latest` | Latest observation per station per element |
| GET | `/v1/observations/meteo` | Meteo observations — filtered by parameters below |

### Observation query parameters

| Parameter | Type | Description | Example |
|---|---|---|---|
| `station_code` | comma-separated integers | Filter by one or more stations | `41061,26227` |
| `element_code` | comma-separated strings | Filter by one or more elements | `wl_avg,wl_min` |
| `from_ts` | ISO 8601 datetime | Start of observation window | `2025-06-01T00:00:00` |
| `to_ts` | ISO 8601 datetime | End of observation window | `2025-06-02T00:00:00` |
| `limit` | integer 1–50000 | Max rows returned (default 10000) | `5000` |

### Element codes

All element codes are **lowercase** in the api schema.

**Hydro:** `wl_avg`, `wl_min`, `wl_max`, `wt_avg`, `wt_min`, `wt_max`, `q_avg`, `q_min`, `q_max`

**Meteo:** `pr1h`, `ta`, `tan1h`, `tax1h`, `rh`, `pa0`, `ws10m`, `wsx1h`, `wd10m`, `sdur1h`

### Example requests

```bash
# All hydrometric stations
curl "https://api.deng.ee/v1/stations/hydro"

# Single station
curl "https://api.deng.ee/v1/stations/hydro/41061"

# Water level for one station, explicit date range
curl "https://api.deng.ee/v1/observations/hydro?station_code=41061&element_code=wl_avg&from_ts=2025-06-01T00:00:00&to_ts=2025-06-08T00:00:00"

# Latest water level and temperature for two stations
curl "https://api.deng.ee/v1/observations/hydro/latest?station_code=41061,26227&element_code=wl_avg,wt_avg"

# Precipitation for all stations, last week
curl "https://api.deng.ee/v1/observations/meteo?element_code=pr1h&from_ts=2025-06-01T00:00:00&to_ts=2025-06-08T00:00:00"

# Filter elements by source
curl "https://api.deng.ee/v1/elements?source=hydro"
```

> **Important:** Due to `API_LAG_DAYS=3`, the freshest data is ~3 days old. Always pass explicit `from_ts`/`to_ts` for meaningful results, or use the `/latest` endpoint. The default 24h window currently returns empty — see Section 6.

---

## 4. API Usage Monitoring

Every request is logged to `monitoring.request_log` in `hydro_climate_db` by a FastAPI middleware. Browser noise is excluded (favicon, touch icons, `/docs`, `/openapi.json`, `/health`).

### Table schema

| Column | Type | Description |
|---|---|---|
| `id` | BIGSERIAL | Primary key |
| `requested_at` | TIMESTAMPTZ | Request timestamp (UTC) |
| `method` | TEXT | HTTP method |
| `endpoint` | TEXT | URL path e.g. `/v1/stations/hydro` |
| `query_params` | JSONB | All query parameters, NULL if none |
| `status_code` | INTEGER | HTTP response code |
| `response_ms` | NUMERIC(10,2) | Response time in milliseconds |
| `client_ip` | TEXT | Client IP — reads `x-forwarded-for` from Caddy |

### Useful queries

```bash
# Connect to analytics DB
docker exec -it deng-analytics-db psql -U analytics -d hydro_climate_db
```

```sql
-- Recent requests
SELECT requested_at, method, endpoint, status_code, response_ms, client_ip
FROM monitoring.request_log
ORDER BY requested_at DESC
LIMIT 20;

-- Request counts by endpoint
SELECT endpoint, count(*) AS requests
FROM monitoring.request_log
GROUP BY endpoint
ORDER BY requests DESC;

-- Average response time by endpoint
SELECT endpoint,
       round(avg(response_ms), 2) AS avg_ms,
       round(min(response_ms), 2) AS min_ms,
       round(max(response_ms), 2) AS max_ms,
       count(*) AS requests
FROM monitoring.request_log
GROUP BY endpoint
ORDER BY avg_ms DESC;

-- Error rate by status code
SELECT status_code, count(*) AS count
FROM monitoring.request_log
GROUP BY status_code
ORDER BY status_code;

-- Requests per day
SELECT date_trunc('day', requested_at) AS day,
       count(*) AS requests
FROM monitoring.request_log
GROUP BY day
ORDER BY day DESC;

-- Truncate and reset (clean slate)
TRUNCATE monitoring.request_log RESTART IDENTITY;
```

---

## 5. Caddy Configuration

The `api.deng.ee` reverse proxy block lives in `/etc/caddy/Caddyfile` on the server. This file is **not in Git**.

```
api.deng.ee {
    reverse_proxy localhost:8000
}
```

To reload Caddy after a config change:

```bash
sudo caddy reload --config /etc/caddy/Caddyfile
```

To check Caddy status:

```bash
sudo systemctl status caddy
sudo caddy validate --config /etc/caddy/Caddyfile
```

---

## 6. Pending Fixes

### Default time window returns empty results

**Problem:** The default observation query window is `timedelta(hours=24)` from now. Because `API_LAG_DAYS=3`, the freshest data in the database is ~3 days old. The default window therefore returns empty results for all observation endpoints.

**Fix:** In `docker/fastapi/main.py` around line 45, change:
```python
default_from = datetime.utcnow() - timedelta(hours=24)
```
to:
```python
default_from = datetime.utcnow() - timedelta(days=4)
```

Then rebuild the container:
```bash
docker compose up -d --build fastapi
```

### Async monitoring middleware refactor

**Problem:** The request logging middleware currently `await`s the database insert before returning the response to the client. This adds latency to every request.

**Fix:** Refactor to use `asyncio.create_task()` with an extracted `_write_request_log()` helper function so the DB insert runs in the background and the client receives the response immediately.

```python
# Current pattern (blocks response)
await _write_request_log(...)

# Target pattern (non-blocking)
asyncio.create_task(_write_request_log(...))
```

---

## 7. Technical Details

- **Python:** 3.12-slim base image
- **Framework:** FastAPI 0.115.6 + uvicorn
- **DB driver:** asyncpg connection pool — min 2, max 10 connections to `deng-analytics-db`
- **Rate limiting:** slowapi 0.1.9 — 60 requests/minute per IP, keyed on `x-forwarded-for` header (set by Caddy)
- **Swagger UI:** Auto-generated at `/docs`. Version and OAS badges have a CSS override in `DOCS_HTML` in `main.py` — `.info .version` and related selectors forced to `color: #374151` (dark text on light background fix)
- **Monitoring excluded paths:** `/health`, `/openapi.json`, `/docs`, `/favicon.ico`, `/apple-touch-icon.png`, `/apple-touch-icon-precomposed.png`
- **`published_at`** in observation tables — captured at dbt run time (06:00 UTC daily), represents when data became available on the API, not ingestion time

---

## 8. Diagnosing Issues

### Container unhealthy

```bash
docker compose logs --tail=100 deng-fastapi
```

Common causes:
- Default time window returning empty (see Section 6)
- `deng-analytics-db` not reachable — check analytics-db container is healthy
- asyncpg pool exhausted — check for slow queries in `monitoring.request_log`

### 500 errors on observation endpoints

Check whether the api schema models are populated:

```bash
docker exec -it deng-analytics-db psql -U analytics -d hydro_climate_db -c \
  "SELECT count(*) FROM api.observations_hydro;
   SELECT count(*) FROM api.observations_meteo;"
```

If counts are zero, the dbt api models need a full rebuild — see [`docs/runbook/dbt_runbook.md`](dbt_runbook.md).

### Rate limit errors (429)

Expected behaviour for >60 requests/minute from a single IP. If legitimate traffic is being blocked, the limit can be adjusted in `main.py` — search for `slowapi` limiter configuration and rebuild the container.

### Empty observation results

1. Check `from_ts`/`to_ts` — data is ~3 days old due to API lag
2. Verify element codes are lowercase (`wl_avg` not `WL_AVG`)
3. Check station code exists: `GET /v1/stations/hydro/{station_code}`
4. Query `monitoring.request_log` to confirm the request reached the API and returned 200

---

*deng-hydro-climate — FastAPI Runbook — v1.0 — 2026-05-29*
