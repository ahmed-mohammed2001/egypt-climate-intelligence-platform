{{
    config(
        materialized='incremental',
        unique_key='AIR_POLLUTION_KEY',
        on_schema_change='append_new_columns',
        tags=['fact', 'incremental']
    )
}}

WITH air_pollution_data AS (
    SELECT
        ap.*,
        dg.GOVERNORATE_KEY,
        dd.DATE_KEY,
        TO_NUMBER(TO_CHAR(ap.DATETIME, 'HH24')) AS HOUR_OF_DAY
    FROM {{ ref('silver_air_pollution') }} ap
    LEFT JOIN {{ ref('dim_governorate') }} dg 
        ON ap.GOVERNORATE = dg.GOVERNORATE_NAME
    LEFT JOIN {{ ref('dim_date') }} dd 
        ON DATE(ap.DATETIME) = dd.DATE_VALUE
    
    {% if is_incremental() %}
    WHERE ap.TRANSFORM_TIMESTAMP > (
        SELECT COALESCE(MAX(TRANSFORM_TIMESTAMP), '1970-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['DATETIME', 'GOVERNORATE']) }} AS AIR_POLLUTION_KEY,
    GOVERNORATE_KEY,
    DATE_KEY,
    DATETIME,
    HOUR_OF_DAY,
    AIR_QUALITY_INDEX,
    CARBON_MONOXIDE,
    NITROGEN_MONOXIDE,
    NITROGEN_DIOXIDE,
    OZONE,
    SULFUR_DIOXIDE,
    PARTICULATE_MATTER_2_5,
    PARTICULATE_MATTER_10,
    AMMONIA,
    CASE
        WHEN AIR_QUALITY_INDEX <= 1 THEN 'Good'
        WHEN AIR_QUALITY_INDEX <= 2 THEN 'Fair'
        WHEN AIR_QUALITY_INDEX <= 3 THEN 'Moderate'
        WHEN AIR_QUALITY_INDEX <= 4 THEN 'Poor'
        WHEN AIR_QUALITY_INDEX <= 5 THEN 'Very Poor'
        ELSE 'Unknown'
    END AS AIR_QUALITY_CATEGORY,
    LOAD_TIMESTAMP,
    TRANSFORM_TIMESTAMP,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM air_pollution_data
WHERE GOVERNORATE_KEY IS NOT NULL
ORDER BY DATETIME, GOVERNORATE