-- Test: Hour (tund) must be within 0 to 23
-- Catches malformed hourly data from the API
-- Returns rows that violate the range — test passes when zero rows returned

SELECT
    jaam_kood,
    jaam_nimi,
    aasta,
    kuu,
    paev,
    tund
FROM {{ source('bronze', 'meteo') }}
WHERE tund < 0 OR tund > 23
