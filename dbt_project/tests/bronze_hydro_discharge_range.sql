-- Test: Discharge (Äravool) values must be within plausible range (-300 to 15000 m³/s)
-- Negative values are valid for coastal/tidal stations where flow reversal is possible
-- Covers Äravool avg, Äravool min, Äravool max measurement types
-- Returns rows that violate the range — test passes when zero rows returned

SELECT
    jaam_kood,
    jaam_nimi,
    timeline_ts_utc,
    aegrida_nimi,
    vaartus
FROM {{ source('bronze', 'hydro') }}
WHERE aegrida_nimi IN ('Äravool avg', 'Äravool min', 'Äravool max')
  AND (vaartus < -300 OR vaartus > 15000)
