-- Test: Water level (WL) values must be within plausible range (-100 to 1500 cm)
-- Covers WL avg, WL min, WL max measurement types
-- Returns rows that violate the range — test passes when zero rows returned

SELECT
    jaam_kood,
    jaam_nimi,
    timeline_ts_utc,
    aegrida_nimi,
    vaartus
FROM {{ source('bronze', 'hydro') }}
WHERE aegrida_nimi IN ('WL avg', 'WL min', 'WL max')
  AND (vaartus < -100 OR vaartus > 1500)
