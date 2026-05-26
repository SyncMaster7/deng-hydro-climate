-- Test: Relative humidity (RH) must be within 0 to 100%
-- Returns rows that violate the range — test passes when zero rows returned

SELECT
    jaam_kood,
    jaam_nimi,
    aasta,
    kuu,
    paev,
    tund,
    vaartus
FROM {{ source('bronze', 'meteo') }}
WHERE element_kood = 'RH'
  AND (vaartus < 0 OR vaartus > 100)
