"""
Estonian Hydro-Meteo API — Public API
FastAPI + asyncpg + slowapi rate limiter
Swagger UI at /docs
"""

import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

import asyncpg
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DB_HOST     = os.getenv("ANALYTICS_DB_HOST", "analytics-db")
DB_PORT     = int(os.getenv("ANALYTICS_DB_PORT", "5432"))
DB_NAME     = os.getenv("ANALYTICS_DB_NAME", "hydro_climate_db")
DB_USER     = os.getenv("ANALYTICS_DB_USER", "analytics")
DB_PASSWORD = os.getenv("ANALYTICS_DB_PASSWORD", "")

RATE_LIMIT    = os.getenv("RATE_LIMIT", "60/minute")
DEFAULT_LIMIT = 5  # rows returned when no filters provided

# ---------------------------------------------------------------------------
# API descriptions — English
# ---------------------------------------------------------------------------

CONTENT = {
    "app_description": (
        "Public API for Estonian hydrological and meteorological observations.\n\n"
        "**Fact/dim design** — fetch station metadata once, query observations by "
        "station and element code. No authentication required. "
        "Rate limited to 60 requests/minute.\n\n"
        "**Endpoints overview:**\n"
        "- `/v1/stations/*` — dimension endpoints, fetch once and cache\n"
        "- `/v1/elements` — measurement type catalogue\n"
        "- `/v1/observations/*` — fact endpoints, filter by station, element, and time range\n\n"
        "**Default behaviour:** when no filters are provided, returns the 5 most recent "
        "rows at the latest available timestamp. Due to pipeline lag (~3 days), use "
        "explicit `from_ts`/`to_ts` for historical queries."
    ),
    "tag_stations_desc":     "Dimension endpoints — hydrometric and meteorological station metadata. Fetch once and cache.",
    "tag_elements_desc":     "Measurement type catalogue — all available element codes with description and unit.",
    "tag_observations_desc": "Fact endpoints — time-series observations filtered by station, element code, and time range.",
    "stations_hydro_list":   "Returns all 76 hydrometric stations with full metadata. Fetch once and cache — station data changes rarely.",
    "stations_hydro_get":    "Returns metadata for a single hydrometric station by station_code.",
    "stations_meteo_list":   "Returns all 25 meteorological stations with full metadata. Fetch once and cache — station data changes rarely.",
    "stations_meteo_get":    "Returns metadata for a single meteorological station by station_code.",
    "elements":              "Returns all available element codes with description, unit, and source. Use `source=hydro` or `source=meteo` to filter.",
    "obs_hydro": (
        "Returns hydrological observations filtered by station, element code, and time range. "
        "All timestamps are in local Estonian time (EET/EEST).\n\n"
        "**Default (no filters):** returns 5 rows at the latest available timestamp.\n"
        "**With filters:** returns up to `limit` rows ordered by `obs_ts` descending."
    ),
    "obs_hydro_latest": "Returns the most recent observation per station per element code. Useful for dashboard current-state views.",
    "obs_meteo": (
        "Returns meteorological observations filtered by station, element code, and time range. "
        "All timestamps are in local Estonian time (EET/EEST).\n\n"
        "**Default (no filters):** returns 5 rows at the latest available timestamp.\n"
        "**With filters:** returns up to `limit` rows ordered by `obs_ts` descending."
    ),
}

# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

limiter = Limiter(key_func=get_remote_address, default_limits=[RATE_LIMIT])

# ---------------------------------------------------------------------------
# Lifespan — connection pool
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        min_size=2,
        max_size=10,
    )
    yield
    await app.state.pool.close()

# ---------------------------------------------------------------------------
# App — docs disabled, served via custom route below
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Estonian Hydro-Meteo API",
    description=CONTENT["app_description"],
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# OpenAPI spec — patched with descriptions, error schemas removed
# ---------------------------------------------------------------------------

def build_openapi_spec() -> dict:
    """Return a patched OpenAPI spec — descriptions injected, error schemas removed."""
    app.openapi_schema = None  # clear FastAPI internal cache
    base = app.openapi()
    spec = json.loads(json.dumps(base))

    spec["info"]["title"]       = "Estonian Hydro-Meteo API"
    spec["info"]["description"] = CONTENT["app_description"]

    spec["tags"] = [
        {"name": "Stations",     "description": CONTENT["tag_stations_desc"]},
        {"name": "Elements",     "description": CONTENT["tag_elements_desc"]},
        {"name": "Observations", "description": CONTENT["tag_observations_desc"]},
    ]

    patches = {
        "/v1/stations/hydro":                {"get": {"description": CONTENT["stations_hydro_list"]}},
        "/v1/stations/hydro/{station_code}": {"get": {"description": CONTENT["stations_hydro_get"]}},
        "/v1/stations/meteo":                {"get": {"description": CONTENT["stations_meteo_list"]}},
        "/v1/stations/meteo/{station_code}": {"get": {"description": CONTENT["stations_meteo_get"]}},
        "/v1/elements":                      {"get": {"description": CONTENT["elements"]}},
        "/v1/observations/hydro":            {"get": {"description": CONTENT["obs_hydro"]}},
        "/v1/observations/hydro/latest":     {"get": {"description": CONTENT["obs_hydro_latest"]}},
        "/v1/observations/meteo":            {"get": {"description": CONTENT["obs_meteo"]}},
    }

    for path, methods in patches.items():
        if path in spec.get("paths", {}):
            for method, patch in methods.items():
                if method in spec["paths"][path]:
                    spec["paths"][path][method].update(patch)

    # Remove error schemas
    schemas_to_remove = {"HTTPValidationError", "ValidationError"}
    if "components" in spec and "schemas" in spec["components"]:
        for s in schemas_to_remove:
            spec["components"]["schemas"].pop(s, None)
    if "components" in spec and not spec["components"].get("schemas"):
        spec.pop("components", None)

    # Remove 422 responses
    for path_item in spec.get("paths", {}).values():
        for op in path_item.values():
            if isinstance(op, dict) and "responses" in op:
                op["responses"].pop("422", None)

    return spec


@app.get("/openapi.json", include_in_schema=False)
async def openapi():
    return JSONResponse(build_openapi_spec())

# ---------------------------------------------------------------------------
# Swagger UI — /docs
# ---------------------------------------------------------------------------

DOCS_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Estonian Hydro-Meteo API</title>
  <link href="https://fonts.googleapis.com/css2?family=Nunito:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist/swagger-ui.css">
  <style>
    /* ── Base ── */
    body {
      margin: 0;
      background: #f8f9fc;
      font-family: 'Nunito', sans-serif;
    }

    /* ── Apply Nunito everywhere ── */
    .swagger-ui,
    .swagger-ui .info,
    .swagger-ui .info p,
    .swagger-ui .info li,
    .swagger-ui .opblock-summary-description,
    .swagger-ui .opblock-description-wrapper p,
    .swagger-ui .opblock-description-wrapper li,
    .swagger-ui .parameter__name,
    .swagger-ui .parameter__type,
    .swagger-ui table thead tr th,
    .swagger-ui table tbody tr td,
    .swagger-ui .response-col_status,
    .swagger-ui .btn,
    .swagger-ui select,
    .swagger-ui label,
    .swagger-ui .tab li,
    .swagger-ui .scheme-container,
    .swagger-ui .servers,
    .swagger-ui .servers label {
      font-family: 'Nunito', sans-serif !important;
    }

    /* ── Code / mono ── */
    .swagger-ui .opblock-summary-path,
    .swagger-ui .opblock-summary-path__deprecated,
    pre, code,
    .swagger-ui textarea,
    .swagger-ui input[type="text"],
    .swagger-ui .curl {
      font-family: 'JetBrains Mono', monospace !important;
    }

    /* ── Topbar — clean white ── */
    .swagger-ui .topbar {
      background: #ffffff;
      border-bottom: 1px solid #e5e7eb;
      padding: 10px 0;
    }

    .swagger-ui .topbar .download-url-wrapper input[type=text] {
      border-color: #e5e7eb;
      border-radius: 6px;
      font-family: 'JetBrains Mono', monospace !important;
    }

    .swagger-ui .topbar .download-url-wrapper .download-url-button {
      background: #2563eb;
      border-radius: 6px;
      font-family: 'Nunito', sans-serif !important;
      font-weight: 600;
    }

    /* ── Title ── */
    .swagger-ui .info .title {
      font-family: 'Nunito', sans-serif !important;
      font-weight: 700;
      font-size: 2rem;
      color: #1e293b;
    }

    .swagger-ui .info .title small.version-stamp {
      background: #2563eb;
      border-radius: 4px;
      font-family: 'Nunito', sans-serif !important;
    }

    /* ── Tag headings ── */
    .swagger-ui .opblock-tag {
      font-family: 'Nunito', sans-serif !important;
      font-weight: 700;
      font-size: 1.1rem;
      color: #1e293b;
      border-bottom: 1px solid #e5e7eb;
    }

    /* ── Operation blocks ── */
    .swagger-ui .opblock {
      border-radius: 8px;
      border: 1px solid #e5e7eb;
      box-shadow: none;
      margin-bottom: 8px;
    }

    .swagger-ui .opblock.opblock-get {
      background: #f0f7ff;
      border-color: #bfdbfe;
    }

    .swagger-ui .opblock.opblock-get .opblock-summary {
      border-color: #bfdbfe;
    }

    /* ── GET badge ── */
    .swagger-ui .opblock-summary-method {
      border-radius: 5px;
      font-family: 'Nunito', sans-serif !important;
      font-weight: 700;
      font-size: 0.75rem;
      letter-spacing: 0.05em;
      min-width: 60px;
    }

    .swagger-ui .opblock.opblock-get .opblock-summary-method {
      background: #2563eb;
    }

    /* ── Buttons ── */
    .swagger-ui .btn {
      border-radius: 6px;
      font-weight: 600;
      font-size: 0.85rem;
    }

    .swagger-ui .btn.execute {
      background: #2563eb;
      border-color: #2563eb;
    }

    .swagger-ui .btn.execute:hover {
      background: #1d4ed8;
      border-color: #1d4ed8;
    }

    .swagger-ui .btn.try-out__btn {
      border-color: #2563eb;
      color: #2563eb;
    }

    /* ── Wrapper ── */
    .swagger-ui .wrapper {
      max-width: 1200px;
      padding: 0 24px;
    }

    /* ── Models section — hide it ── */
    .swagger-ui section.models {
      display: none;
    }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({
      url: "/openapi.json",
      dom_id: "#swagger-ui",
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: "BaseLayout",
      deepLinking: true,
      defaultModelsExpandDepth: -1,
    });
  </script>
</body>
</html>"""


@app.get("/docs", include_in_schema=False)
async def docs():
    return HTMLResponse(DOCS_HTML)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def default_from_ts() -> datetime:
    return now_utc() - timedelta(days=4)


def row_to_dict(record: asyncpg.Record) -> dict:
    """Convert asyncpg Record to JSON-serialisable dict."""
    result = {}
    for key, value in record.items():
        if isinstance(value, datetime):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result

# ---------------------------------------------------------------------------
# Dimension endpoints
# ---------------------------------------------------------------------------

@app.get(
    "/v1/stations/hydro",
    summary="List all hydrometric stations",
    tags=["Stations"],
)
@limiter.limit(RATE_LIMIT)
async def list_hydro_stations(request: Request):
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM api.stations_hydro ORDER BY station_code")
    return [row_to_dict(r) for r in rows]


@app.get(
    "/v1/stations/hydro/{station_code}",
    summary="Get a single hydrometric station",
    tags=["Stations"],
)
@limiter.limit(RATE_LIMIT)
async def get_hydro_station(request: Request, station_code: int):
    async with request.app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM api.stations_hydro WHERE station_code = $1",
            station_code,
        )
    if row is None:
        return JSONResponse(status_code=404, content={"detail": f"Station {station_code} not found."})
    return row_to_dict(row)


@app.get(
    "/v1/stations/meteo",
    summary="List all meteorological stations",
    tags=["Stations"],
)
@limiter.limit(RATE_LIMIT)
async def list_meteo_stations(request: Request):
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM api.stations_meteo ORDER BY station_code")
    return [row_to_dict(r) for r in rows]


@app.get(
    "/v1/stations/meteo/{station_code}",
    summary="Get a single meteorological station",
    tags=["Stations"],
)
@limiter.limit(RATE_LIMIT)
async def get_meteo_station(request: Request, station_code: str):
    async with request.app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM api.stations_meteo WHERE station_code = $1",
            station_code,
        )
    if row is None:
        return JSONResponse(status_code=404, content={"detail": f"Station {station_code} not found."})
    return row_to_dict(row)


@app.get(
    "/v1/elements",
    summary="List all measurement element codes",
    tags=["Elements"],
)
@limiter.limit(RATE_LIMIT)
async def list_element_codes(
    request: Request,
    source: Optional[str] = Query(None, description="Filter by source: 'hydro' or 'meteo'"),
):
    async with request.app.state.pool.acquire() as conn:
        if source:
            rows = await conn.fetch(
                "SELECT * FROM api.measurement_types WHERE source = $1 ORDER BY element_code",
                source,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM api.measurement_types ORDER BY source, element_code"
            )
    return [row_to_dict(r) for r in rows]

# ---------------------------------------------------------------------------
# Fact endpoints — hydro observations
# ---------------------------------------------------------------------------

@app.get(
    "/v1/observations/hydro",
    summary="Query hydrological observations",
    tags=["Observations"],
)
@limiter.limit(RATE_LIMIT)
async def get_hydro_observations(
    request: Request,
    station_code: Optional[str] = Query(None, description="Comma-separated station codes, e.g. 41061,26227"),
    element_code: Optional[str] = Query(None, description="Comma-separated element codes, e.g. wl_avg,wl_min"),
    from_ts: Optional[datetime] = Query(None, description="Start of time range (ISO 8601)."),
    to_ts: Optional[datetime] = Query(None, description="End of time range (ISO 8601). Defaults to now."),
    limit: int = Query(10000, ge=1, le=50000, description="Max rows returned."),
):
    has_filters = any([station_code, element_code, from_ts, to_ts])

    async with request.app.state.pool.acquire() as conn:

        if not has_filters:
            latest_ts = await conn.fetchval("SELECT max(obs_ts) FROM api.observations_hydro")
            if latest_ts is None:
                return []
            rows = await conn.fetch(
                """
                SELECT station_code, element_code, obs_value, obs_ts, published_at
                FROM api.observations_hydro
                WHERE obs_ts = $1
                LIMIT $2
                """,
                latest_ts, DEFAULT_LIMIT,
            )
            return [row_to_dict(r) for r in rows]

        from_dt = from_ts or default_from_ts()
        to_dt   = to_ts   or now_utc()

        station_codes = [s.strip() for s in station_code.split(",")] if station_code else None
        element_codes = [e.strip().lower() for e in element_code.split(",")] if element_code else None

        query = """
            SELECT station_code, element_code, obs_value, obs_ts, published_at
            FROM api.observations_hydro
            WHERE obs_ts >= $1 AND obs_ts <= $2
        """
        params = [from_dt, to_dt]
        idx = 3

        if station_codes:
            query += f" AND station_code = ANY(${idx}::int[])"
            params.append([int(s) for s in station_codes])
            idx += 1

        if element_codes:
            query += f" AND element_code = ANY(${idx}::text[])"
            params.append(element_codes)
            idx += 1

        query += f" ORDER BY obs_ts DESC, station_code LIMIT ${idx}"
        params.append(limit)

        rows = await conn.fetch(query, *params)

    return [row_to_dict(r) for r in rows]


@app.get(
    "/v1/observations/hydro/latest",
    summary="Latest hydrological observation per station",
    tags=["Observations"],
)
@limiter.limit(RATE_LIMIT)
async def get_hydro_latest(
    request: Request,
    element_code: Optional[str] = Query(None, description="Comma-separated element codes, e.g. wl_avg,wt_avg"),
):
    element_codes = [e.strip().lower() for e in element_code.split(",")] if element_code else None

    query = """
        SELECT DISTINCT ON (station_code, element_code)
            station_code, element_code, obs_value, obs_ts, published_at
        FROM api.observations_hydro
    """
    params = []

    if element_codes:
        query += " WHERE element_code = ANY($1::text[])"
        params.append(element_codes)

    query += " ORDER BY station_code, element_code, obs_ts DESC"

    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [row_to_dict(r) for r in rows]

# ---------------------------------------------------------------------------
# Fact endpoints — meteo observations
# ---------------------------------------------------------------------------

@app.get(
    "/v1/observations/meteo",
    summary="Query meteorological observations",
    tags=["Observations"],
)
@limiter.limit(RATE_LIMIT)
async def get_meteo_observations(
    request: Request,
    station_code: Optional[str] = Query(None, description="Comma-separated station codes, e.g. 26242,26231"),
    element_code: Optional[str] = Query(None, description="Comma-separated element codes, e.g. pr1h,ta"),
    from_ts: Optional[datetime] = Query(None, description="Start of time range (ISO 8601)."),
    to_ts: Optional[datetime] = Query(None, description="End of time range (ISO 8601). Defaults to now."),
    limit: int = Query(10000, ge=1, le=50000, description="Max rows returned."),
):
    has_filters = any([station_code, element_code, from_ts, to_ts])

    async with request.app.state.pool.acquire() as conn:

        if not has_filters:
            latest_ts = await conn.fetchval("SELECT max(obs_ts) FROM api.observations_meteo")
            if latest_ts is None:
                return []
            rows = await conn.fetch(
                """
                SELECT station_code, element_code, obs_value, obs_ts, published_at
                FROM api.observations_meteo
                WHERE obs_ts = $1
                LIMIT $2
                """,
                latest_ts, DEFAULT_LIMIT,
            )
            return [row_to_dict(r) for r in rows]

        from_dt = from_ts or default_from_ts()
        to_dt   = to_ts   or now_utc()

        station_codes = [s.strip() for s in station_code.split(",")] if station_code else None
        element_codes = [e.strip().lower() for e in element_code.split(",")] if element_code else None

        query = """
            SELECT station_code, element_code, obs_value, obs_ts, published_at
            FROM api.observations_meteo
            WHERE obs_ts >= $1 AND obs_ts <= $2
        """
        params = [from_dt, to_dt]
        idx = 3

        if station_codes:
            query += f" AND station_code = ANY(${idx}::text[])"
            params.append(station_codes)
            idx += 1

        if element_codes:
            query += f" AND element_code = ANY(${idx}::text[])"
            params.append(element_codes)
            idx += 1

        query += f" ORDER BY obs_ts DESC, station_code LIMIT ${idx}"
        params.append(limit)

        rows = await conn.fetch(query, *params)

    return [row_to_dict(r) for r in rows]

# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health", include_in_schema=False)
async def health(request: Request):
    try:
        async with request.app.state.pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "error", "detail": str(e)})
