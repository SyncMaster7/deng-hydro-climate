-- Test: Precipitation (PR1H) must not be negative
-- Physically impossible for precipitation to be below zero
-- Returns rows that violate the check — test passes when zero rows returned

SELECT
    jaam_kood,
    jaam_nimi,
    aasta,
    kuu,
    paev,
    tund,
    vaartus
FROM {{ source('bronze', 'meteo') }}
WHERE element_kood = 'PR1H'
  AND vaartus < 0
