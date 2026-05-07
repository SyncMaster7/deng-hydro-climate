-- =============================================================================
-- silver.meteo
-- Pivots bronze.meteo from long format to wide format
-- One row per station per hour with columns for each element
-- Constructs observation_ts from aasta/kuu/paev/tund
-- =============================================================================

WITH source AS (
    SELECT
        jaam_kood,
        jaam_nimi,
        aasta,
        kuu,
        paev,
        tund,
        element_kood,
        vaartus,
        -- Construct observation timestamp from date parts (Estonia = UTC+2/UTC+3)
        -- Stored as UTC by adding the hour as-is — adjust in gold if timezone matters
        make_timestamptz(aasta, kuu, paev, tund, 0, 0, 'Europe/Tallinn') AS observation_ts
    FROM {{ source('bronze', 'meteo') }}
),

pivoted AS (
    SELECT
        jaam_kood,
        jaam_nimi,
        aasta,
        kuu,
        paev,
        tund,
        observation_ts,

        -- Precipitation (mm)
        MAX(CASE WHEN element_kood = 'PR1H'  THEN vaartus END) AS precipitation_mm,

        -- Air temperature (°C)
        MAX(CASE WHEN element_kood = 'TA'    THEN vaartus END) AS temp_avg,
        MAX(CASE WHEN element_kood = 'TAN1H' THEN vaartus END) AS temp_min,
        MAX(CASE WHEN element_kood = 'TAX1H' THEN vaartus END) AS temp_max,

        -- Humidity (%)
        MAX(CASE WHEN element_kood = 'RH'    THEN vaartus END) AS humidity_pct,

        -- Pressure (hPa)
        MAX(CASE WHEN element_kood = 'PA0'   THEN vaartus END) AS pressure_hpa,

        -- Wind
        MAX(CASE WHEN element_kood = 'WS10M' THEN vaartus END) AS wind_speed_ms,
        MAX(CASE WHEN element_kood = 'WSX1H' THEN vaartus END) AS wind_gust_ms,
        MAX(CASE WHEN element_kood = 'WD10M' THEN vaartus END) AS wind_direction_deg,

        -- Sunshine duration (minutes)
        MAX(CASE WHEN element_kood = 'SDUR1H' THEN vaartus END) AS sunshine_duration_min

    FROM source
    GROUP BY
        jaam_kood, jaam_nimi,
        aasta, kuu, paev, tund,
        observation_ts
)

SELECT * FROM pivoted
