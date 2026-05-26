-- Test: Atmospheric pressure (PA0) must be within plausible range (950 to 1060 hPa)
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
WHERE element_kood = 'PA0'
  AND (vaartus < 950 OR vaartus > 1060)
