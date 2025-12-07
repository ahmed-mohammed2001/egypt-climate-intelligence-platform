{{
    config(
        materialized='incremental',
        unique_key='SOLAR_ENERGY_KEY',
        on_schema_change='append_new_columns',
        tags=['fact', 'incremental']
    )
}}

WITH solar_energy_data AS (
    SELECT
        se.*,
        dg.GOVERNORATE_KEY,
        dd.DATE_KEY,
        TO_NUMBER(TO_CHAR(se.DATETIME, 'HH24')) AS HOUR_OF_DAY
    FROM {{ ref('silver_solar_energy') }} se
    LEFT JOIN {{ ref('dim_governorate') }} dg 
        ON se.GOVERNORATE = dg.GOVERNORATE_NAME
    LEFT JOIN {{ ref('dim_date') }} dd 
        ON DATE(se.DATETIME) = dd.DATE_VALUE
    
    {% if is_incremental() %}
    WHERE se.TRANSFORM_TIMESTAMP > (
        SELECT COALESCE(MAX(TRANSFORM_TIMESTAMP), '1970-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['DATETIME', 'GOVERNORATE']) }} AS SOLAR_ENERGY_KEY,
    GOVERNORATE_KEY,
    DATE_KEY,
    DATETIME,
    HOUR_OF_DAY,
    ALL_SKY_SURFACE_SHORTWAVE_DOWNWARD,
    CLEAR_SKY_SURFACE_SHORTWAVE_DOWNWARD,
    ALL_SKY_SURFACE_SHORTWAVE_DIRECT_NORMAL,
    ALL_SKY_SURFACE_SHORTWAVE_DIFFUSE,
    ALL_SKY_SURFACE_PAR_TOTAL,
    CLEAR_SKY_SURFACE_PAR_TOTAL,
    ALL_SKY_SURFACE_UVA,
    ALL_SKY_SURFACE_UVB,
    ALL_SKY_SURFACE_UV_INDEX,
    CASE
        WHEN ALL_SKY_SURFACE_UV_INDEX <= 2 THEN 'Low'
        WHEN ALL_SKY_SURFACE_UV_INDEX <= 5 THEN 'Moderate'
        WHEN ALL_SKY_SURFACE_UV_INDEX <= 7 THEN 'High'
        WHEN ALL_SKY_SURFACE_UV_INDEX <= 10 THEN 'Very High'
        WHEN ALL_SKY_SURFACE_UV_INDEX > 10 THEN 'Extreme'
        ELSE 'Unknown'
    END AS UV_INDEX_CATEGORY,
    LOAD_TIMESTAMP,
    TRANSFORM_TIMESTAMP,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM solar_energy_data
WHERE GOVERNORATE_KEY IS NOT NULL
ORDER BY DATETIME, GOVERNORATE