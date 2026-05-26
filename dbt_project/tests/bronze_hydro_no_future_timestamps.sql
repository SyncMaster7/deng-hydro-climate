-- Test: No future timestamps in bronze.hydro
-- timeline_ts_utc must not be ahead of current time
-- Catches timestamp parsing bugs or malformed API responses
-- Returns rows that violate the check — test passes when zero rows returned

SELECT
    jaam_kood,
    jaam_nimi,
    timeline_ts_utc
FROM {{ source('bronze', 'hydro') }}
WHERE timeline_ts_utc > now()
