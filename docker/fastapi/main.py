"""
deng-hydro-climate — Public API
FastAPI + asyncpg + slowapi rate limiter
ReDoc at /docs (bilingual EN/ET), Swagger UI at /docs-dev
"""

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

RATE_LIMIT  = os.getenv("RATE_LIMIT", "60/minute")

DEFAULT_LIMIT = 5  # rows returned when no filters provided

# ---------------------------------------------------------------------------
# Bilingual descriptions
# ---------------------------------------------------------------------------

DESCRIPTIONS = {
    "en": {
        "app": (
            "Public API for Estonian hydrological and meteorological observations. "
            "**Fact/dim design** — fetch station metadata once, query observations by station and element code. "
            "No authentication required. Rate limited to 60 requests/minute per IP.\n\n"
            "**Endpoints overview:**\n"
            "- `/v1/stations/*` — dimension endpoints, fetch once and cache\n"
            "- `/v1/elements` — measurement type catalogue\n"
            "- `/v1/observations/*` — fact endpoints, filter by station, element, and time range\n\n"
            "**Default behaviour:** when no filters are provided, returns the 5 most recent rows at the latest available timestamp. "
            "Due to pipeline lag (~3 days), use explicit `from_ts`/`to_ts` for historical queries."
        ),
        "stations_hydro_list": "Returns all 76 hydrometric stations with full metadata. Fetch once and cache — station data changes rarely.",
        "stations_hydro_get": "Returns metadata for a single hydrometric station by station_code.",
        "stations_meteo_list": "Returns all 25 meteorological stations with full metadata. Fetch once and cache — station data changes rarely.",
        "stations_meteo_get": "Returns metadata for a single meteorological station by station_code.",
        "elements": (
            "Returns all available element codes with description, unit, and source. "
            "Use `source=hydro` or `source=meteo` to filter. "
            "Element codes are used as filters in observation endpoints."
        ),
        "obs_hydro": (
            "Returns hydrological observations filtered by station, element code, and time range. "
            "All timestamps are in local Estonian time (EET/EEST).\n\n"
            "**Default (no filters):** returns 5 rows at the latest available timestamp.\n"
            "**With filters:** returns up to `limit` rows ordered by `obs_ts` descending."
        ),
        "obs_hydro_latest": (
            "Returns the most recent observation per station per element code. "
            "Useful for dashboard current-state views. No time range filter needed."
        ),
        "obs_meteo": (
            "Returns meteorological observations filtered by station, element code, and time range. "
            "All timestamps are in local Estonian time (EET/EEST).\n\n"
            "**Default (no filters):** returns 5 rows at the latest available timestamp.\n"
            "**With filters:** returns up to `limit` rows ordered by `obs_ts` descending."
        ),
        "param_station_code_hydro": "Comma-separated hydrometric station codes, e.g. `41061,26227`",
        "param_station_code_meteo": "Comma-separated meteorological station codes, e.g. `26242,26231`",
        "param_element_code_hydro": "Comma-separated element codes, e.g. `wl_avg,wl_min`",
        "param_element_code_meteo": "Comma-separated element codes, e.g. `pr1h,ta`",
        "param_element_code_filter": "Comma-separated element codes, e.g. `wl_avg,wt_avg`",
        "param_from_ts": "Start of time range (ISO 8601). When omitted and other filters present, defaults to 4 days ago.",
        "param_to_ts": "End of time range (ISO 8601). Defaults to now.",
        "param_limit": "Maximum number of rows returned (1–50000).",
        "param_source": "Filter by source: `hydro` or `meteo`",
    },
    "et": {
        "app": (
            "Avalik API Eesti hüdroloogiliste ja meteoroloogiliste vaatluste jaoks. "
            "**Fakt/mõõde disain** — laadi jaama metaandmed üks kord, päri vaatlusi jaama ja elemendi koodi järgi. "
            "Autentimine pole vajalik. Päringuid on piiratud 60-ni minutis IP-aadressi kohta.\n\n"
            "**Otspunktide ülevaade:**\n"
            "- `/v1/stations/*` — mõõtme-otspunktid, laadi üks kord ja vahemälusta\n"
            "- `/v1/elements` — mõõtmistüüpide kataloog\n"
            "- `/v1/observations/*` — fakti-otspunktid, filtreeri jaama, elemendi ja ajavahemiku järgi\n\n"
            "**Vaikekäitumine:** kui filtreid pole määratud, tagastatakse 5 viimast rida viimasel saadaoleval ajatemplil. "
            "Torujuhtme viivituse tõttu (~3 päeva) kasuta ajalooliste päringute jaoks `from_ts`/`to_ts` parameetreid."
        ),
        "stations_hydro_list": "Tagastab kõik 76 hüdromeetrijaama täieliku metaandmetega. Laadi üks kord ja vahemälusta — jaama andmed muutuvad harva.",
        "stations_hydro_get": "Tagastab ühe hüdromeetrijaama metaandmed jaama koodi (station_code) järgi.",
        "stations_meteo_list": "Tagastab kõik 25 meteoroloogiajaama täieliku metaandmetega. Laadi üks kord ja vahemälusta — jaama andmed muutuvad harva.",
        "stations_meteo_get": "Tagastab ühe meteoroloogiajaama metaandmed jaama koodi (station_code) järgi.",
        "elements": (
            "Tagastab kõik saadaolevad elemendikoodid koos kirjelduse, ühiku ja allikaga. "
            "Kasuta `source=hydro` või `source=meteo` filtreerimiseks. "
            "Elemendikoodid on kasutatavad filtritena vaatluste otspunktides."
        ),
        "obs_hydro": (
            "Tagastab hüdroloogilised vaatlused filtreeritud jaama, elemendi ja ajavahemiku järgi. "
            "Kõik ajatemplid on Eesti kohalikus ajas (EET/EEST).\n\n"
            "**Vaikimisi (filtrid puuduvad):** tagastab 5 rida viimasel saadaoleval ajatemplil.\n"
            "**Filtritega:** tagastab kuni `limit` rida, järjestatud `obs_ts` kahanevas järjekorras."
        ),
        "obs_hydro_latest": (
            "Tagastab iga jaama ja elemendi koodi viimase vaatluse. "
            "Kasulik armatuurlaua hetkeseisu kuvamiseks. Ajavahemiku filtrit pole vaja."
        ),
        "obs_meteo": (
            "Tagastab meteoroloogilised vaatlused filtreeritud jaama, elemendi ja ajavahemiku järgi. "
            "Kõik ajatemplid on Eesti kohalikus ajas (EET/EEST).\n\n"
            "**Vaikimisi (filtrid puuduvad):** tagastab 5 rida viimasel saadaoleval ajatemplil.\n"
            "**Filtritega:** tagastab kuni `limit` rida, järjestatud `obs_ts` kahanevas järjekorras."
        ),
        "param_station_code_hydro": "Komaga eraldatud hüdromeetrijaama koodid, nt `41061,26227`",
        "param_station_code_meteo": "Komaga eraldatud meteoroloogiajaama koodid, nt `26242,26231`",
        "param_element_code_hydro": "Komaga eraldatud elemendikoodid, nt `wl_avg,wl_min`",
        "param_element_code_meteo": "Komaga eraldatud elemendikoodid, nt `pr1h,ta`",
        "param_element_code_filter": "Komaga eraldatud elemendikoodid, nt `wl_avg,wt_avg`",
        "param_from_ts": "Ajavahemiku algus (ISO 8601). Kui puudub ja muud filtrid on määratud, vaikimisi 4 päeva tagasi.",
        "param_to_ts": "Ajavahemiku lõpp (ISO 8601). Vaikimisi praegune aeg.",
        "param_limit": "Tagastatavate ridade maksimaalne arv (1–50000).",
        "param_source": "Filtreeri allika järgi: `hydro` või `meteo`",
    },
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
# App — docs disabled, served via custom routes below
# ---------------------------------------------------------------------------

app = FastAPI(
    title="deng-hydro-climate API",
    description=DESCRIPTIONS["en"]["app"],
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
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
# Custom docs — ReDoc at /docs
# ---------------------------------------------------------------------------

REDOC_HTML = """<!DOCTYPE html>
<html>
<head>
  <title>deng-hydro-climate API</title>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    body {{ margin: 0; font-family: 'Inter', sans-serif; }}
    #lang-bar {{
      position: fixed;
      top: 0; right: 0;
      z-index: 9999;
      padding: 10px 20px;
      background: #fff;
      border-bottom: 1px solid #e5e7eb;
      border-left: 1px solid #e5e7eb;
      border-radius: 0 0 0 8px;
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 13px;
      font-weight: 500;
      color: #6b7280;
      box-shadow: -2px 2px 8px rgba(0,0,0,0.06);
    }}
    #lang-bar span {{ color: #9ca3af; font-size: 11px; letter-spacing: 0.05em; text-transform: uppercase; }}
    .lang-btn {{
      padding: 4px 12px;
      border-radius: 5px;
      border: 1px solid #e5e7eb;
      background: #f9fafb;
      cursor: pointer;
      font-size: 12px;
      font-weight: 600;
      color: #374151;
      transition: all 0.15s;
    }}
    .lang-btn.active {{
      background: #1a56db;
      color: #fff;
      border-color: #1a56db;
    }}
    .lang-btn:hover:not(.active) {{ background: #f3f4f6; }}
  </style>
</head>
<body>
  <div id="lang-bar">
    <span>Docs language</span>
    <button class="lang-btn active" id="btn-en" onclick="setLang('en')">EN</button>
    <button class="lang-btn" id="btn-et" onclick="setLang('et')">ET</button>
  </div>
  <div id="redoc-container"></div>
  <script src="https://cdn.jsdelivr.net/npm/redoc/bundles/redoc.standalone.js"></script>
  <script>
    const DESCRIPTIONS = {descriptions_json};

    function setLang(lang) {{
      localStorage.setItem('api_lang', lang);
      document.getElementById('btn-en').classList.toggle('active', lang === 'en');
      document.getElementById('btn-et').classList.toggle('active', lang === 'et');
      renderDocs(lang);
    }}

    async function renderDocs(lang) {{
      const resp = await fetch('/openapi.json');
      const spec = await resp.json();
      const d = DESCRIPTIONS[lang];

      // App description
      spec.info.description = d.app;

      // Patch endpoint descriptions
      const patches = {{
        '/v1/stations/hydro':              {{ get: {{ description: d.stations_hydro_list }} }},
        '/v1/stations/hydro/{{station_code}}': {{ get: {{ description: d.stations_hydro_get }} }},
        '/v1/stations/meteo':              {{ get: {{ description: d.stations_meteo_list }} }},
        '/v1/stations/meteo/{{station_code}}': {{ get: {{ description: d.stations_meteo_get }} }},
        '/v1/elements':                    {{ get: {{ description: d.elements }} }},
        '/v1/observations/hydro':          {{ get: {{ description: d.obs_hydro }} }},
        '/v1/observations/hydro/latest':   {{ get: {{ description: d.obs_hydro_latest }} }},
        '/v1/observations/meteo':          {{ get: {{ description: d.obs_meteo }} }},
      }};

      for (const [path, methods] of Object.entries(patches)) {{
        if (spec.paths[path]) {{
          for (const [method, patch] of Object.entries(methods)) {{
            if (spec.paths[path][method]) {{
              Object.assign(spec.paths[path][method], patch);
            }}
          }}
        }}
      }}

      document.getElementById('redoc-container').innerHTML = '';
      Redoc.init(spec, {{
        scrollYOffset: 50,
        hideDownloadButton: false,
        theme: {{
          colors: {{ primary: {{ main: '#1a56db' }} }},
          typography: {{ fontFamily: 'Inter, sans-serif' }},
        }}
      }}, document.getElementById('redoc-container'));
    }}

    const lang = localStorage.getItem('api_lang') || 'en';
    setLang(lang);
  </script>
</body>
</html>"""

SWAGGER_HTML = """<!DOCTYPE html>
<html>
<head>
  <title>deng-hydro-climate API — Developer Docs</title>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist/swagger-ui.css">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
  <style>
    body {{ margin: 0; font-family: 'Inter', sans-serif; }}
    #lang-bar {{
      position: fixed;
      top: 0; right: 0;
      z-index: 9999;
      padding: 10px 20px;
      background: #fff;
      border-bottom: 1px solid #e5e7eb;
      border-left: 1px solid #e5e7eb;
      border-radius: 0 0 0 8px;
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 13px;
      font-weight: 500;
      color: #6b7280;
      box-shadow: -2px 2px 8px rgba(0,0,0,0.06);
    }}
    #lang-bar span {{ color: #9ca3af; font-size: 11px; letter-spacing: 0.05em; text-transform: uppercase; }}
    .lang-btn {{
      padding: 4px 12px;
      border-radius: 5px;
      border: 1px solid #e5e7eb;
      background: #f9fafb;
      cursor: pointer;
      font-size: 12px;
      font-weight: 600;
      color: #374151;
      transition: all 0.15s;
    }}
    .lang-btn.active {{
      background: #1a56db;
      color: #fff;
      border-color: #1a56db;
    }}
    .lang-btn:hover:not(.active) {{ background: #f3f4f6; }}
    .swagger-ui .topbar {{ display: none; }}
  </style>
</head>
<body>
  <div id="lang-bar">
    <span>Docs language</span>
    <button class="lang-btn active" id="btn-en" onclick="setLang('en')">EN</button>
    <button class="lang-btn" id="btn-et" onclick="setLang('et')">ET</button>
  </div>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script>
  <script>
    const DESCRIPTIONS = {descriptions_json};

    function setLang(lang) {{
      localStorage.setItem('api_lang', lang);
      document.getElementById('btn-en').classList.toggle('active', lang === 'en');
      document.getElementById('btn-et').classList.toggle('active', lang === 'et');
      renderDocs(lang);
    }}

    async function renderDocs(lang) {{
      const resp = await fetch('/openapi.json');
      const spec = await resp.json();
      const d = DESCRIPTIONS[lang];

      spec.info.description = d.app;

      const patches = {{
        '/v1/stations/hydro':              {{ get: {{ description: d.stations_hydro_list }} }},
        '/v1/stations/hydro/{{station_code}}': {{ get: {{ description: d.stations_hydro_get }} }},
        '/v1/stations/meteo':              {{ get: {{ description: d.stations_meteo_list }} }},
        '/v1/stations/meteo/{{station_code}}': {{ get: {{ description: d.stations_meteo_get }} }},
        '/v1/elements':                    {{ get: {{ description: d.elements }} }},
        '/v1/observations/hydro':          {{ get: {{ description: d.obs_hydro }} }},
        '/v1/observations/hydro/latest':   {{ get: {{ description: d.obs_hydro_latest }} }},
        '/v1/observations/meteo':          {{ get: {{ description: d.obs_meteo }} }},
      }};

      for (const [path, methods] of Object.entries(patches)) {{
        if (spec.paths[path]) {{
          for (const [method, patch] of Object.entries(methods)) {{
            if (spec.paths[path][method]) {{
              Object.assign(spec.paths[path][method], patch);
            }}
          }}
        }}
      }}

      SwaggerUIBundle({{
        spec: spec,
        dom_id: '#swagger-ui',
        presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
        layout: 'BaseLayout',
        deepLinking: true,
      }});
    }}

    const lang = localStorage.getItem('api_lang') || 'en';
    setLang(lang);
  </script>
</body>
</html>"""

# ---------------------------------------------------------------------------
# Custom doc routes
# ---------------------------------------------------------------------------

@app.get("/docs", include_in_schema=False)
async def redoc():
    import json
    html = REDOC_HTML.format(descriptions_json=json.dumps(DESCRIPTIONS))
    return HTMLResponse(html)


@app.get("/docs-dev", include_in_schema=False)
async def swagger():
    import json
    html = SWAGGER_HTML.format(descriptions_json=json.dumps(DESCRIPTIONS))
    return HTMLResponse(html)

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

        # Default — no filters: return DEFAULT_LIMIT rows at latest timestamp
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

        # Filtered query
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

        # Default — no filters: return DEFAULT_LIMIT rows at latest timestamp
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

        # Filtered query
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
