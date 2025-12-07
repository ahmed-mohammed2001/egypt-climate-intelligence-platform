{{
    config(
        materialized='table',
        tags=['dimension', 'run_once']
    )
}}

WITH date_range AS (
    SELECT 
        DATE '1981-01-01' AS start_date,
        DATE '2030-12-31' AS end_date
),

date_series AS (
    SELECT 
        DATEADD(DAY, SEQ4(), start_date) AS DATE_VALUE
    FROM date_range,
         TABLE(GENERATOR(ROWCOUNT => 18263))
    WHERE DATE_VALUE <= end_date
)

SELECT
    TO_NUMBER(TO_CHAR(DATE_VALUE, 'YYYYMMDD')) AS DATE_KEY,
    DATE_VALUE,
    YEAR(DATE_VALUE) AS YEAR,
    QUARTER(DATE_VALUE) AS QUARTER,
    MONTH(DATE_VALUE) AS MONTH,
    MONTHNAME(DATE_VALUE) AS MONTH_NAME,
    DAY(DATE_VALUE) AS DAY,
    DAYOFWEEK(DATE_VALUE) AS DAY_OF_WEEK,
    DAYNAME(DATE_VALUE) AS DAY_NAME,
    WEEKOFYEAR(DATE_VALUE) AS WEEK_OF_YEAR,
    CASE 
        WHEN DAYOFWEEK(DATE_VALUE) IN (6, 7) THEN TRUE 
        ELSE FALSE 
    END AS IS_WEEKEND,
    CASE
        WHEN MONTH(DATE_VALUE) IN (12, 1, 2) THEN 'Winter'
        WHEN MONTH(DATE_VALUE) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(DATE_VALUE) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(DATE_VALUE) IN (9, 10, 11) THEN 'Autumn'
    END AS SEASON,
    CURRENT_TIMESTAMP() AS CREATED_AT
FROM date_series
ORDER BY DATE_VALUE