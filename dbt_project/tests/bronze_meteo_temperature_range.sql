-- Test: Air temperature values must be within plausible range (-40 to 35°C)
-- Covers TA (avg), TAN1H (min), TAX1H (max) element codes
-- Returns rows that violate the range — test passes when zero rows returned

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
WHERE element_kood IN ('TA', 'TAN1H', 'TAX1H')
  AND (vaartus < -40 OR vaartus > 35)
