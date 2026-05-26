-- Test: Water temperature (WT) values must be within plausible range (-5 to 30°C)
-- Covers WT avg, WT min, WT max measurement types
-- Returns rows that violate the range — test passes when zero rows returned

SELECT
    jaam_kood,
    jaam_nimi,
    timeline_ts_utc,
    aegrida_nimi,
    vaartus
FROM {{ source('bronze', 'hydro') }}
WHERE aegrida_nimi IN ('WT avg', 'WT min', 'WT max')
  AND (vaartus < -5 OR vaartus > 30)
