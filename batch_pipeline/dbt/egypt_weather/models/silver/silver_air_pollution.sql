{{
    config(
        materialized='incremental',
        unique_key=['GOVERNORATE', 'DATETIME'],
        on_schema_change='append_new_columns'
    )
}}

WITH source_data AS (
    SELECT
        -- Standardize city names
        CASE 
            WHEN UPPER(TRIM(CITY)) = 'SIWAH' THEN 'MATROUH'
            WHEN UPPER(TRIM(CITY)) = 'SHARM EL SHEIKH' THEN 'SOUTH SINAI'
            WHEN UPPER(TRIM(CITY)) = 'ELWADY_ELGADED' THEN 'NEW VALLEY'
            WHEN UPPER(TRIM(CITY)) = 'EL WADI EL GEDID' THEN 'NEW VALLEY'
            WHEN UPPER(TRIM(CITY)) = 'SHARM _AL-SHAYKH' THEN 'SOUTH SINAI'
            ELSE UPPER(TRIM(CITY))
        END AS GOVERNORATE,
        
        DATETIME,
        AQI AS AIR_QUALITY_INDEX,
        CO AS CARBON_MONOXIDE,
        NO AS NITROGEN_MONOXIDE,
        NO2 AS NITROGEN_DIOXIDE,
        O3 AS OZONE,
        SO2 AS SULFUR_DIOXIDE,
        PM2_5 AS PARTICULATE_MATTER_2_5,
        PM10 AS PARTICULATE_MATTER_10,
        NH3 AS AMMONIA,
        LOAD_TIMESTAMP
    FROM {{ source('bronze', 'AIR_POLLUTION') }}
    
    {% if is_incremental() %}
        WHERE LOAD_TIMESTAMP > (
            SELECT COALESCE(MAX(LOAD_TIMESTAMP), '1970-01-01'::TIMESTAMP_NTZ)
            FROM {{ this }}
        )
    {% endif %}
)

SELECT 
    GOVERNORATE,
    DATETIME,
    AIR_QUALITY_INDEX,
    CARBON_MONOXIDE,
    NITROGEN_MONOXIDE,
    NITROGEN_DIOXIDE,
    OZONE,
    SULFUR_DIOXIDE,
    PARTICULATE_MATTER_2_5,
    PARTICULATE_MATTER_10,
    AMMONIA,
    LOAD_TIMESTAMP,
    CURRENT_TIMESTAMP() AS TRANSFORM_TIMESTAMP
FROM source_data
WHERE GOVERNORATE IS NOT NULL
    AND DATETIME IS NOT NULL