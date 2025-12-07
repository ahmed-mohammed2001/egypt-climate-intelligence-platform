{{
    config(
        materialized='table',
        tags=['dimension', 'run_once']
    )
}}

WITH all_governorates AS (
    SELECT DISTINCT GOVERNORATE FROM {{ ref('silver_air_pollution') }}
    UNION
    SELECT DISTINCT GOVERNORATE FROM {{ ref('silver_solar_energy') }}
    UNION
    SELECT DISTINCT GOVERNORATE FROM {{ ref('silver_weather') }}
)

SELECT 
    ROW_NUMBER() OVER (ORDER BY GOVERNORATE) AS GOVERNORATE_KEY,
    GOVERNORATE AS GOVERNORATE_NAME,
    CASE 
        WHEN GOVERNORATE IN ('CAIRO', 'GIZA', 'QALYUBIA') THEN 'Greater Cairo'
        WHEN GOVERNORATE IN ('ALEXANDRIA', 'BEHEIRA', 'KAFR EL SHEIKH', 'MATROUH') THEN 'Alexandria and North Coast'
        WHEN GOVERNORATE IN ('DAKAHLIA', 'SHARQIA', 'GHARBIA', 'MONUFIA', 'DAMIETTA', 'PORT SAID', 'ISMAILIA', 'SUEZ') THEN 'Delta and Canal'
        WHEN GOVERNORATE IN ('ASWAN', 'LUXOR', 'QENA', 'SOHAG', 'ASYUT', 'MINYA', 'BENI SUEF', 'FAYOUM') THEN 'Upper Egypt'
        WHEN GOVERNORATE IN ('SOUTH SINAI', 'NORTH SINAI', 'RED SEA', 'NEW VALLEY') THEN 'Frontier Governorates'
        ELSE 'Other'
    END AS REGION,
    CURRENT_TIMESTAMP() AS CREATED_AT,
    CURRENT_TIMESTAMP() AS UPDATED_AT
FROM all_governorates
WHERE GOVERNORATE IS NOT NULL
ORDER BY GOVERNORATE