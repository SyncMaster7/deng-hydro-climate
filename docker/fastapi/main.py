"""
Estonian Hydro-Meteo API — Public API
FastAPI + asyncpg + slowapi rate limiter
Swagger UI at /docs
"""

import json
import os
import time
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
# API content — Estonian
# ---------------------------------------------------------------------------

APP_DESCRIPTION = (
    "---\n\n"
    "## Ülevaade\n\n"
    "Eesti Hydro-Meteo API pakub avalikku ja struktureeritud juurdepääsu Eesti hüdroloogilistele ja "
    "meteoroloogilistele mõõtmisandmetele. API on loodud eestkätt arendajatele ja andmeanalüüsi "
    "projektidele, võimaldades usaldusväärset ligipääsu Keskkonnaagentuuri (`keskkonnaandmed.envir.ee`) "
    "seireandmetele läbi kaasaegse ning ressursisäästliku liidese.\n\n"
    "Platvorm koondab nii mõõtmisandmeid enam kui 100 seirejaamast üle Eesti, sealhulgas "
    "hüdromeetria- ja meteoroloogiajaamadest. Andmed sobivad kasutamiseks rakendustes, teadustöös, "
    "keskkonnaseires ning ETL-andmetorudes.\n\n"
    "---\n\n"
    "## Andmed\n\n"
    "API sisaldab hüdroloogilisi ja meteoroloogilisi vaatlusandmeid alates 2025 aastast.\n\n"
    "### Hüdroloogilised seireandmed\n\n"
    "Sisaldavad:\n"
    "- veetaset\n"
    "- veetemperatuuri\n"
    "- vee äravoolu mõõtmisi\n\n"
    "### Meteoroloogilised seireandmed\n\n"
    "Sisaldavad:\n"
    "- sademete andmeid\n"
    "- õhutemperatuuri\n"
    "- tuule kiirust ja suunda\n"
    "- õhurõhku\n"
    "- suhtelist õhuniiskust\n"
    "- päikesepaiste kestust\n\n"
    "> Kõik ajatemplid on esitatud Eesti kohalikus ajas (EET/EEST).\n"
    "> Andmeid uuendatakse automaatselt iga päev kell **09:00** kohaliku aja järgi.\n\n"
    "Tulenevalt lähteandmete avaldamise protsessist avaldatakse seireandmed viivitusega:\n"
    "- hüdroloogilised seireandmed ligikaudu 46 tundi pärast mõõtmist\n"
    "- meteoroloogilised seireandmed ligikaudu 28 tundi pärast mõõtmist\n\n"
    "---\n\n"
    "## Arhitektuur\n\n"
    "API on üles ehitatud tähtskeemi (*star schema*) põhimõttel, eraldades mõõtmisandmed ja metaandmed "
    "erinevatesse api otspunktidesse: dimensioonitabelid — harva ajas muutuvad seirejaamade metaandmed "
    "ning faktitabelid — ajas pidevalt uuenevad seireandmed.\n\n"
    "Antud lahendus võimaldab:\n"
    "- väiksemat andmemahtu päringutes\n"
    "- kiiremat töötlemist\n"
    "- efektiivsemat ETL- ja analüütikaprotsessi\n"
    "- paremat skaleeritavust rakenduste ja andmeplatvormide jaoks\n\n"
    "---\n\n"
    "## Päringud\n\n"
    "Päringute koostamisel on soovitatav kasutada ajavahemiku filtreerimise parameetreid `from_ts` ja `to_ts`. "
    "Kui päringu koostamisel filtreid ei määrata, tagastab API vaikimisi kuni 5 viimast mõõtmist "
    "viimasel saadaoleval ajatemplil.\n\n"
    "API toetab paindlikku filtreerimist:\n"
    "- seirejaam\n"
    "- mõõdetav element\n"
    "- ajavahemik\n\n"
    "---\n\n"
    "## API päringu näide\n\n"
    "Python script:\n\n"
    "```python\n"
    "import requests\n\n"
    "## Laadi seirejaamade metaandmed\n"
    "stations = requests.get(\n"
    "    \"https://api.deng.ee/v1/stations/hydro\"\n"
    ").json()\n\n"
    "## Päri viimase 3 päeva veetaseme andmed\n"
    "obs = requests.get(\n"
    "    \"https://api.deng.ee/v1/observations/hydro\",\n"
    "    params={\n"
    "        \"station_code\": \"41061\",\n"
    "        \"element_code\": \"wl_avg\",\n"
    "        \"from_ts\": \"2026-05-20T00:00:00\",\n"
    "        \"to_ts\": \"2026-05-23T00:00:00\",\n"
    "        \"limit\": 100\n"
    "    }\n"
    ").json()\n"
    "```\n\n"
    "---\n\n"
    "## Litsents ja viited\n\n"
    "Andmed on avaldatud Creative Commonsi litsentsi [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) alusel.\n\n"
    "Andmete kasutamisel tuleb viidata:\n"
    "- [Keskkonnaagentuur](https://www.keskkonnaagentur.ee)\n"
    "- käesolevale API-le (`api.deng.ee`)\n\n"
    "**Allikas:** [keskkonnaandmed.envir.ee](https://keskkonnaandmed.envir.ee)"
)

CONTENT = {
    "tag_stations_desc":     "Hüdromeetria- ja meteoroloogiajaamade metaandmed.",
    "tag_elements_desc":     "Mõõtmistüüpide kataloog — kõik saadaolevad seirenäitaja koodid koos kirjelduste ja ühikutega.",
    "tag_observations_desc": "Vaatluste aegread filtreeritud jaama, seirenäitaja koodi ja ajavahemiku järgi.",
    "stations_hydro_list":   "Kõik hüdromeetrijaamad koos metaandmetega.",
    "stations_hydro_get":    "Hüdromeetrijaama metaandmed jaama koodi (station_code) alusel.",
    "stations_meteo_list":   "Kõik meteoroloogiajaamad koos metaandmetega.",
    "stations_meteo_get":    "Meteoroloogiajaama metaandmed jaama koodi (station_code) alusel.",
    "elements":              "Kõik saadaolevad seirenäitaja koodid koos kirjelduse, ühiku ja allikaga. Kasuta filtreerimiseks source=hydro või source=meteo.",
    "obs_hydro": (
        "Hüdroloogilised vaatlused filtreeritud jaama, seirenäitaja ja ajavahemiku järgi. "
        "Kõik ajatemplid on Eesti kohalikus ajas (EET/EEST).\n\n"
        "**Vaikimisi (filtrid puuduvad):** tagastab 5 rida viimasel saadaoleval ajatemplil.\n"
        "**Filtritega:** tagastab kuni `limit` rida, järjestatud `obs_ts` kahanevas järjekorras."
    ),
    "obs_hydro_latest": "Iga jaama ja seirenäitaja koodi viimane vaatlus. Kasulik näidikulaua hetkeseisu kuvamiseks.",
    "obs_meteo": (
        "Meteoroloogilised vaatlused filtreeritud jaama, seirenäitaja ja ajavahemiku järgi. "
        "Kõik ajatemplid on Eesti kohalikus ajas (EET/EEST).\n\n"
        "**Vaikimisi (filtrid puuduvad):** tagastab 5 rida viimasel saadaoleval ajatemplil.\n"
        "**Filtritega:** tagastab kuni `limit` rida, järjestatud `obs_ts` kahanevas järjekorras."
    ),
    "param_station_code_hydro": "Komaga eraldatud jaama koodid, nt 41061,26227",
    "param_station_code_meteo": "Komaga eraldatud jaama koodid, nt 26242,26231",
    "param_element_code_hydro": "Komaga eraldatud seirenäitaja koodid, nt wl_avg,wl_min",
    "param_element_code_meteo": "Komaga eraldatud seirenäitaja koodid, nt pr1h,ta",
    "param_element_code_latest": "Komaga eraldatud seirenäitaja koodid, nt wl_avg,wt_avg",
    "param_from_ts":  "Ajavahemiku algus (ISO 8601).",
    "param_to_ts":    "Ajavahemiku lõpp (ISO 8601). Vaikimisi praegune aeg.",
    "param_limit":    "Maksimaalne tagastatavate ridade arv.",
    "param_source":   "Filtreeri allika järgi: hydro või meteo",
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
    title="Eesti Hydro-Meteo API",
    description=APP_DESCRIPTION,
    version="2.1.1",
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
# Request logging middleware — writes to monitoring.request_log after response
# ---------------------------------------------------------------------------

@app.middleware("http")
async def log_request(request: Request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    elapsed_ms = round((time.monotonic() - start) * 1000, 2)

    # Skip health check and internal routes
    if request.url.path in ("/health", "/openapi.json", "/docs"):
        return response

    try:
        query_params = dict(request.query_params) or None
        client_ip = (
            request.headers.get("x-forwarded-for", "").split(",")[0].strip()
            or request.client.host
        )
        async with request.app.state.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO monitoring.request_log
                    (method, endpoint, query_params, status_code, response_ms, client_ip)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                request.method,
                request.url.path,
                json.dumps(query_params) if query_params else None,
                response.status_code,
                elapsed_ms,
                client_ip,
            )
    except Exception:
        pass  # Never let logging failure affect the response

    return response

# ---------------------------------------------------------------------------
# OpenAPI spec — patched with descriptions, contact, licence, schemas removed
# ---------------------------------------------------------------------------

def build_openapi_spec() -> dict:
    """Return a patched OpenAPI spec — descriptions injected, error schemas removed."""
    app.openapi_schema = None  # clear FastAPI internal cache
    base = app.openapi()
    spec = json.loads(json.dumps(base))

    # Info
    spec["info"]["title"]          = "Eesti Hydro-Meteo API"
    spec["info"]["description"]    = APP_DESCRIPTION
    spec["info"]["termsOfService"] = "https://creativecommons.org/licenses/by/4.0/"
    spec["info"]["contact"] = {
        "email": "info@deng.ee",
    }

    # Tags
    spec["tags"] = [
        {"name": "Seirejaamad",   "description": CONTENT["tag_stations_desc"]},
        {"name": "Seirenäitajad",   "description": CONTENT["tag_elements_desc"]},
        {"name": "Vaatlused", "description": CONTENT["tag_observations_desc"]},
    ]

    # Remap tag names on operations
    tag_map = {
        "Stations":     "Seirejaamad",
        "Elements":     "Seirenäitajad",
        "Observations": "Vaatlused",
    }
    for path_item in spec.get("paths", {}).values():
        for op in path_item.values():
            if isinstance(op, dict) and "tags" in op:
                op["tags"] = [tag_map.get(t, t) for t in op["tags"]]

    # Endpoint descriptions
    patches = {
        "/v1/stations/hydro":                {"get": {"description": CONTENT["stations_hydro_list"], "summary": "Kuva kõik hüdromeetriajaamad"}},
        "/v1/stations/hydro/{station_code}": {"get": {"description": CONTENT["stations_hydro_get"],  "summary": "Kuva üks hüdromeetriajaam"}},
        "/v1/stations/meteo":                {"get": {"description": CONTENT["stations_meteo_list"], "summary": "Kuva kõik meteoroloogiajaamad"}},
        "/v1/stations/meteo/{station_code}": {"get": {"description": CONTENT["stations_meteo_get"],  "summary": "Kuva üks meteoroloogiajaam"}},
        "/v1/elements":                      {"get": {"description": CONTENT["elements"],            "summary": "Kuva kõik seirenäitaja koodid"}},
        "/v1/observations/hydro":            {"get": {"description": CONTENT["obs_hydro"],           "summary": "Päri hüdroloogilisi vaatlusi"}},
        "/v1/observations/hydro/latest":     {"get": {"description": CONTENT["obs_hydro_latest"],    "summary": "Viimane hüdroloogiline vaatlus jaama kohta"}},
        "/v1/observations/meteo":            {"get": {"description": CONTENT["obs_meteo"],           "summary": "Päri meteoroloogilisi vaatlusi"}},
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
<html lang="et">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Eesti Hydro-Meteo API</title>
  <link href="https://fonts.googleapis.com/css2?family=Nunito:wght@300;400;500;600;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist/swagger-ui.css">
  <style>
    body { margin: 0; font-family: 'Nunito', sans-serif; }
    .swagger-ui,
    .swagger-ui *:not(code):not(pre):not(.opblock-summary-path):not(textarea):not(input) {
      font-family: 'Nunito', sans-serif !important;
    }
    .swagger-ui section.models { display: none; }

    /* ── Inline code — subtle, no box, flows with text ── */
    .swagger-ui .info p code,
    .swagger-ui .info li code {
      font-family: 'Nunito', sans-serif !important;
      font-size: 1em !important;
      font-weight: 600 !important;
      color: #374151 !important;
      background: none !important;
      border: none !important;
      padding: 0 !important;
      border-radius: 0 !important;
    }

    /* ── Code block — neutral colour, normal weight ── */
    .swagger-ui .info pre {
      background: #f3f4f6 !important;
      border: 1px solid #e5e7eb !important;
      border-radius: 6px !important;
      padding: 1rem !important;
    }

    .swagger-ui .info pre code {
      font-family: 'Courier New', monospace !important;
      font-size: 0.85em !important;
      font-weight: 400 !important;
      color: #374151 !important;
      background: none !important;
      border: none !important;
      padding: 0 !important;
    }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({
      url: window.location.origin + "/openapi.json",
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
    summary="Kuva kõik hüdromeetriajaamad",
    tags=["Stations"],
)
@limiter.limit(RATE_LIMIT)
async def list_hydro_stations(request: Request):
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM api.stations_hydro ORDER BY station_code")
    return [row_to_dict(r) for r in rows]


@app.get(
    "/v1/stations/hydro/{station_code}",
    summary="Kuva üks hüdromeetriajaam",
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
        return JSONResponse(status_code=404, content={"detail": f"Jaama koodiga {station_code} ei leitud."})
    return row_to_dict(row)


@app.get(
    "/v1/stations/meteo",
    summary="Kuva kõik meteoroloogiajaamad",
    tags=["Stations"],
)
@limiter.limit(RATE_LIMIT)
async def list_meteo_stations(request: Request):
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM api.stations_meteo ORDER BY station_code")
    return [row_to_dict(r) for r in rows]


@app.get(
    "/v1/stations/meteo/{station_code}",
    summary="Kuva üks meteoroloogiajaam",
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
        return JSONResponse(status_code=404, content={"detail": f"Jaama koodiga {station_code} ei leitud."})
    return row_to_dict(row)


@app.get(
    "/v1/elements",
    summary="Kuva kõik seirenäitaja koodid",
    tags=["Elements"],
)
@limiter.limit(RATE_LIMIT)
async def list_element_codes(
    request: Request,
    source: Optional[str] = Query(None, description="Filtreeri allika järgi: hydro või meteo"),
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
    summary="Päri hüdroloogilisi vaatlusi",
    tags=["Observations"],
)
@limiter.limit(RATE_LIMIT)
async def get_hydro_observations(
    request: Request,
    station_code: Optional[str] = Query(None, description="Komaga eraldatud jaama koodid, nt 41061,26227"),
    element_code: Optional[str] = Query(None, description="Komaga eraldatud seirenäitaja koodid, nt wl_avg,wl_min"),
    from_ts: Optional[datetime] = Query(None, description="Ajavahemiku algus (ISO 8601)."),
    to_ts: Optional[datetime] = Query(None, description="Ajavahemiku lõpp (ISO 8601). Vaikimisi praegune aeg."),
    limit: int = Query(10000, ge=1, le=50000, description="Maksimaalne tagastatavate ridade arv."),
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
    summary="Viimane hüdroloogiline vaatlus jaama kohta",
    tags=["Observations"],
)
@limiter.limit(RATE_LIMIT)
async def get_hydro_latest(
    request: Request,
    element_code: Optional[str] = Query(None, description="Komaga eraldatud seirenäitaja koodid, nt wl_avg,wt_avg"),
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
    summary="Päri meteoroloogilisi vaatlusi",
    tags=["Observations"],
)
@limiter.limit(RATE_LIMIT)
async def get_meteo_observations(
    request: Request,
    station_code: Optional[str] = Query(None, description="Komaga eraldatud jaama koodid, nt 26242,26231"),
    element_code: Optional[str] = Query(None, description="Komaga eraldatud seirenäitaja koodid, nt pr1h,ta"),
    from_ts: Optional[datetime] = Query(None, description="Ajavahemiku algus (ISO 8601)."),
    to_ts: Optional[datetime] = Query(None, description="Ajavahemiku lõpp (ISO 8601). Vaikimisi praegune aeg."),
    limit: int = Query(10000, ge=1, le=50000, description="Maksimaalne tagastatavate ridade arv."),
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
