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
# API content — Estonian
# ---------------------------------------------------------------------------

APP_DESCRIPTION = (
    "## Ülevaade\n\n"
    "Eesti Hydro-Meteo API pakub avalikku juurdepääsu Eesti hüdroloogiliste ja meteoroloogiliste "
    "seirejaamade mõõtmisandmetele. Andmed pärinevad Keskkonnaagentuuri keskkonnaportaalist "
    "(keskkonnaandmed.envir.ee) ning hõlmavad 76 hüdromeeetria ja 25 meteoroloogia jaama üle kogu Eesti.\n\n"
    "## Andmed\n\n"
    "Hüdroloogilised andmed sisaldavad veetaseme, veetemperatuuri ja vee äravoolu tunniseid "
    "mõõtmistulemusi alates 2025-01. Meteoroloogilised andmed sisaldavad andmeid sademete, "
    "õhutemperatuuri, tuule kiiruse ja suuna, õhurõhu, suhtelise õhuniiskuse ning päikesepaiste "
    "kestuse kohta. Kõik vaatlused on esitatud Eesti kohalikus ajas (EET/EEST). "
    "Andmed avaldatakse viivitusega — hüdroloogilised andmed on ligikaudu 46 tundi ja "
    "meteoroloogilised andmed 28 tundi reaalajast maas.\n\n"
    "## Päringud\n\n"
    "Päringute tegemisel on soovitatav kasutada ajavahemiku määramise parameetreid `from_ts` ja `to_ts`. "
    "Kui päringu tegemisel filtreid ei ole määratud, tagastatakse vaikimisi tulemuseks viimasel "
    "saadaoleval ajatemplil kuni 5 viimast väärtust.\n\n"
    "API on üles ehitatud tähtskeemi põhimõttel mis sisaldab dimensioonide- ja faktitabelit. "
    "Andmed mis ajast püsivad muutumatud või muutuvad väga harva, näiteks seirejaamade metaandmed — "
    "koordinaadid, valgala, kõrgus merepinnast jms — on saadaval eraldi API otspunktidest. "
    "Mõõtmisandmed on eraldatud seirejaamade metaandmetest ning sisaldavad mõõtmisandmeid. "
    "Seeläbi vähendame andmete tarbimiseks vajalikku ressursikasutust.\n\n"
    "## Näide\n\n"
    "Lihtne API kasutamise näide kasutades Pythonit:\n\n"
    "```python\n"
    "import requests\n\n"
    "# Laadi seirejaamade metaandmed üks kord\n"
    "stations = requests.get(\"https://api.deng.ee/v1/stations/hydro\").json()\n\n"
    "# Päri viimase 3 päeva veetaseme andmed konkreetsele jaamale\n"
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
    "## Litsents ja viited\n\n"
    "Andmed on avaldatud Creative Commonsi litsentsi [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) "
    "alusel. Andmete kasutamisel tuleb viidata allikale: Keskkonnaagentur, "
    "[keskkonnaandmed.envir.ee](https://keskkonnaandmed.envir.ee) ning käesolevale API-le (api.deng.ee).\n\n"
    "Allikas: [Eesti keskkonnaportaal](https://www.keskkonnaportaal.ee)"
)

CONTENT = {
    "tag_stations_desc":     "Hüdromeetria- ja meteoroloogiajaamade metaandmed.",
    "tag_elements_desc":     "Mõõtmistüüpide kataloog — kõik saadaolevad elemendikoodid koos kirjelduste ja ühikutega.",
    "tag_observations_desc": "Vaatluste aegread filtreeritud jaama, elemendi koodi ja ajavahemiku järgi.",
    "stations_hydro_list":   "Kõik hüdromeetrijaamad koos metaandmetega.",
    "stations_hydro_get":    "Hüdromeetrijaama metaandmed jaama koodi (station_code) alusel.",
    "stations_meteo_list":   "Kõik meteoroloogiajaamad koos metaandmetega.",
    "stations_meteo_get":    "Meteoroloogiajaama metaandmed jaama koodi (station_code) alusel.",
    "elements":              "Kõik saadaolevad elemendikoodid koos kirjelduse, ühiku ja allikaga. Kasuta filtreerimiseks source=hydro või source=meteo.",
    "obs_hydro": (
        "Hüdroloogilised vaatlused filtreeritud jaama, elemendi ja ajavahemiku järgi. "
        "Kõik ajatemplid on Eesti kohalikus ajas (EET/EEST).\n\n"
        "**Vaikimisi (filtrid puuduvad):** tagastab 5 rida viimasel saadaoleval ajatemplil.\n"
        "**Filtritega:** tagastab kuni `limit` rida, järjestatud `obs_ts` kahanevas järjekorras."
    ),
    "obs_hydro_latest": "Iga jaama ja elemendi koodi viimane vaatlus. Kasulik näidikulaua hetkeseisu kuvamiseks.",
    "obs_meteo": (
        "Meteoroloogilised vaatlused filtreeritud jaama, elemendi ja ajavahemiku järgi. "
        "Kõik ajatemplid on Eesti kohalikus ajas (EET/EEST).\n\n"
        "**Vaikimisi (filtrid puuduvad):** tagastab 5 rida viimasel saadaoleval ajatemplil.\n"
        "**Filtritega:** tagastab kuni `limit` rida, järjestatud `obs_ts` kahanevas järjekorras."
    ),
    "param_station_code_hydro": "Komaga eraldatud jaama koodid, nt 41061,26227",
    "param_station_code_meteo": "Komaga eraldatud jaama koodid, nt 26242,26231",
    "param_element_code_hydro": "Komaga eraldatud elemendikoodid, nt wl_avg,wl_min",
    "param_element_code_meteo": "Komaga eraldatud elemendikoodid, nt pr1h,ta",
    "param_element_code_latest": "Komaga eraldatud elemendikoodid, nt wl_avg,wt_avg",
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
    spec["info"]["license"] = {
        "name": "CC BY 4.0",
    }

    # Tags
    spec["tags"] = [
        {"name": "Seirejaamad",   "description": CONTENT["tag_stations_desc"]},
        {"name": "Elemendid",   "description": CONTENT["tag_elements_desc"]},
        {"name": "Vaatlused", "description": CONTENT["tag_observations_desc"]},
    ]

    # Remap tag names on operations
    tag_map = {
        "Stations":     "Seirejaamad",
        "Elements":     "Elemendid",
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
        "/v1/elements":                      {"get": {"description": CONTENT["elements"],            "summary": "Kuva kõik mõõtmiselemendi koodid"}},
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

    /* ── Soften inline code in descriptions ── */
    .swagger-ui .info p code,
    .swagger-ui .info li code,
    .swagger-ui .opblock-description-wrapper p code,
    .swagger-ui .opblock-description-wrapper li code {
      font-family: 'JetBrains Mono', monospace !important;
      font-size: 0.82em;
      font-weight: 400;
      background: #eef0f4;
      color: #374151;
      padding: 1px 5px;
      border-radius: 3px;
      border: none;
    }

    /* ── Soften code blocks (python example etc) ── */
    .swagger-ui .info pre,
    .swagger-ui .info .highlight-code {
      background: #f3f4f6 !important;
      border: 1px solid #e5e7eb !important;
      border-radius: 6px !important;
    }

    .swagger-ui .info pre code,
    .swagger-ui .info .highlight-code code {
      background: transparent !important;
      color: #374151 !important;
      font-weight: 400 !important;
      font-size: 0.85em !important;
      padding: 0 !important;
      border-radius: 0 !important;
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

    /* ── Info links (contact, licence, ToS) ── */
    .swagger-ui .info a {
      color: #2563eb;
      text-decoration: none;
    }
    .swagger-ui .info a:hover {
      text-decoration: underline;
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

    /* ── Models section — hide ── */
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
    summary="Kuva kõik mõõtmiselemendi koodid",
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
    element_code: Optional[str] = Query(None, description="Komaga eraldatud elemendikoodid, nt wl_avg,wl_min"),
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
    element_code: Optional[str] = Query(None, description="Komaga eraldatud elemendikoodid, nt wl_avg,wt_avg"),
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
    element_code: Optional[str] = Query(None, description="Komaga eraldatud elemendikoodid, nt pr1h,ta"),
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
