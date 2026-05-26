-- Test: Wind speed values (WS10M, WSX1H) must not be negative
-- Physically impossible for wind speed to be below zero
-- Returns rows that violate the check — test passes when zero rows returned

SELECT
    jaam_kood,
    jaam_nimi,
    aasta,
    kuu,
    paev,
    tund,
    element_kood,
    vaartus
FROM {{ source('bronze', 'meteo') }}
WHERE element_kood IN ('WS10M', 'WSX1H')
  AND vaartus < 0
