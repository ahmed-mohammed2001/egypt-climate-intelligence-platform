{{
    config(
        materialized='incremental',
        unique_key='WEATHER_KEY',
        on_schema_change='append_new_columns',
        tags=['fact', 'incremental']
    )
}}

WITH weather_data AS (
    SELECT
        w.*,
        dg.GOVERNORATE_KEY,
        dd.DATE_KEY
    FROM {{ ref('silver_weather') }} w
    LEFT JOIN {{ ref('dim_governorate') }} dg 
        ON w.GOVERNORATE = dg.GOVERNORATE_NAME
    LEFT JOIN {{ ref('dim_date') }} dd 
        ON w.DATE = dd.DATE_VALUE
    
    {% if is_incremental() %}
    WHERE w.TRANSFORM_TIMESTAMP > (
        SELECT COALESCE(MAX(TRANSFORM_TIMESTAMP), '1970-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['DATE', 'GOVERNORATE']) }} AS WEATHER_KEY,
    GOVERNORATE_KEY,
    DATE_KEY,
    DATE,
    TEMPERATURE_2M_MEAN,
    TEMPERATURE_2M_DEW_POINT,
    TEMPERATURE_2M_WET_BULB,
    EARTH_SKIN_TEMPERATURE,
    TEMPERATURE_2M_RANGE,
    TEMPERATURE_2M_MAXIMUM,
    TEMPERATURE_2M_MINIMUM,
    SPECIFIC_HUMIDITY_2M,
    RELATIVE_HUMIDITY_2M,
    PRECIPITATION_CORRECTED,
    SURFACE_PRESSURE,
    WIND_SPEED_10M_MEAN,
    WIND_SPEED_10M_MAXIMUM,
    WIND_SPEED_10M_MINIMUM,
    CASE
        WHEN TEMPERATURE_2M_MEAN < 10 THEN 'Cold'
        WHEN TEMPERATURE_2M_MEAN < 20 THEN 'Mild'
        WHEN TEMPERATURE_2M_MEAN < 30 THEN 'Warm'
        WHEN TEMPERATURE_2M_MEAN >= 30 THEN 'Hot'
        ELSE 'Unknown'
    END AS TEMPERATURE_CATEGORY,
    CASE
        WHEN RELATIVE_HUMIDITY_2M < 30 THEN 'Dry'
        WHEN RELATIVE_HUMIDITY_2M < 60 THEN 'Comfortable'
        WHEN RELATIVE_HUMIDITY_2M >= 60 THEN 'Humid'
        ELSE 'Unknown'
    END AS HUMIDITY_CATEGORY,
    CASE
        WHEN PRECIPITATION_CORRECTED = 0 THEN 'No Rain'
        WHEN PRECIPITATION_CORRECTED < 2.5 THEN 'Light Rain'
        WHEN PRECIPITATION_CORRECTED < 10 THEN 'Moderate Rain'
        WHEN PRECIPITATION_CORRECTED >= 10 THEN 'Heavy Rain'
        ELSE 'Unknown'
    END AS PRECIPITATION_CATEGORY,
    LOAD_TIMESTAMP,
    TRANSFORM_TIMESTAMP,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM weather_data
WHERE GOVERNORATE_KEY IS NOT NULL
ORDER BY DATE, GOVERNORATE