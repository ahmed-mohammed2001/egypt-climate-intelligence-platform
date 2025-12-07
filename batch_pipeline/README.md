# Egypt Weather Batch Pipeline

Historical weather, air pollution, and solar energy data processing pipeline using Airflow, Azure Blob Storage, Snowflake, and dbt.

## 📋 Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Running the Pipeline](#running-the-pipeline)
- [Adding Incremental Data](#adding-incremental-data)
- [Data Models](#data-models)

## Overview

This pipeline implements a **medallion architecture**:
- **Bronze Layer**: Raw data from Azure Blob Storage loaded into Snowflake
- **Silver Layer**: Cleaned and standardized data
- **Gold Layer**: Dimensional model (facts and dimensions)

### Pipeline Flow
```
new_data/ → Azure Blob → Snowflake Bronze → dbt Silver → dbt Gold → Tests
```

## Prerequisites

### Required Accounts & Credentials
1. **Azure Blob Storage**
   - Storage account name
   - Container name
   - Connection string or SAS token

2. **Snowflake**
   - Account identifier
   - Username and password
   - Warehouse, database, and schema created
   - Integration with Azure Blob Storage configured

## Project Structure

```
batch-pipeline/
├── README.md                              # This file
├── docker-compose.yml                     # All services definition
├── .env                                   # Your credentials (create this)
├── .gitignore                             # Ignore sensitive files
│
├── airflow/                               # Orchestration
│   ├── Dockerfile                         # Airflow image
│   ├── requirements.txt                   # Python dependencies
│   ├── dags/
│   │   └── egypt_weather_pipeline.py      # Main DAG
│   ├── logs/                              # Airflow logs (auto-created)
│   └── plugins/                           # Custom plugins (if needed)
│
├── dbt/                                   # Transformations
│   ├── Dockerfile                         # dbt image
│   ├── requirements.txt                   # dbt dependencies
│   └── egypt_weather/                     # dbt project
│       ├── dbt_project.yml
│       ├── profiles.yml
│       ├── models/
│           ├── silver/                    # Cleaned data
│           │   ├── silver_air_pollution.sql
│           │   ├── silver_solar_energy.sql
│           │   └── silver_weather.sql
│           └── gold/                      # Dimensional model
│               ├── dim_date.sql
│               ├── dim_governorate.sql
│               ├── fact_air_pollution.sql
│               ├── fact_solar_energy.sql
│               └── fact_weather.sql
│
└── new_data/                              # Drop zone for incremental files
```

## Setup Instructions

### Step 1: Clone and Navigate
```bash
git clone <your-repo-url>
cd egypt_weather_project/batch_pipeline
```

### Step 2: Configure Environment Variables

Make `.env` file with your credentials.

### Step 3: Prepare Snowflake
Run the Snowflake setup script to create:
- Warehouse, database, schemas
- User and role with permissions
- Azure Blob Storage integration
- File format for CSV
- External stage pointing to Azure

```sql
------------------------------------------
-- 1) Create Warehouse (Ensures it exists)
------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS WEATHER_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

------------------------------------------------------
-- 2) Create Database + Schemas (Bronze, Silver, Gold)
------------------------------------------------------
CREATE DATABASE IF NOT EXISTS EGYPT_WEATHER_DB;
USE DATABASE EGYPT_WEATHER_DB;

CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

---------------------------------------------------------------------------
-- 2) Create AIR_POLLUTION, SOLAR_ENERGY and WEATHER tables (inside BRONZE)
---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS BRONZE.AIR_POLLUTION (
        CITY VARCHAR(100),
        DATETIME TIMESTAMP,
        AQI FLOAT,
        CO FLOAT,
        NO FLOAT,
        NO2 FLOAT,
        O3 FLOAT,
        SO2 FLOAT,
        PM2_5 FLOAT,
        PM10 FLOAT,
        NH3 FLOAT,
        LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

CREATE TABLE IF NOT EXISTS BRONZE.SOLAR_ENERGY (
        YEAR INT,
        MO INT,
        DY INT,
        HR INT,
        ALLSKY_SFC_SW_DWN FLOAT,
        CLRSKY_SFC_SW_DWN FLOAT,
        ALLSKY_SFC_SW_DNI FLOAT,
        ALLSKY_SFC_SW_DIFF FLOAT,
        ALLSKY_SFC_PAR_TOT FLOAT,
        CLRSKY_SFC_PAR_TOT FLOAT,
        ALLSKY_SFC_UVA FLOAT,
        ALLSKY_SFC_UVB FLOAT,
        ALLSKY_SFC_UV_INDEX FLOAT,
        CITY VARCHAR(100),
        LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

CREATE TABLE IF NOT EXISTS BRONZE.WEATHER (
        CITY VARCHAR(100),
        DATE DATE,
        T2M FLOAT,
        T2MDEW FLOAT,
        T2MWET FLOAT,
        TS FLOAT,
        T2M_RANGE FLOAT,
        T2M_MAX FLOAT,
        T2M_MIN FLOAT,
        QV2M FLOAT,
        RH2M FLOAT,
        PRECTOTCORR FLOAT,
        PS FLOAT,
        WS10M FLOAT,
        WS10M_MAX FLOAT,
        WS10M_MIN FLOAT,
        LOAD_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

----------------------------------------
-- 3) Create File Format (inside BRONZE)
----------------------------------------
CREATE OR REPLACE FILE FORMAT BRONZE.CSV_FORMAT
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('');
  
--------------------------------------------
-- 4) Create External Stage for Bronze Layer 
--------------------------------------------
CREATE OR REPLACE STAGE BRONZE.AZURE_BRONZE_STAGE
  URL='azure://weather0project.blob.core.windows.net/egyptian-weather-data/bronze'
  CREDENTIALS=(
    AZURE_SAS_TOKEN='sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2026-02-01T20:34:50Z&st=2025-11-24T12:19:50Z&spr=https&sig=EWd8dbYg1NMiISlRPSlY4ymlLrGaZ3UW0Unb24G%2Fyl8%3D'
  )
  FILE_FORMAT = BRONZE.CSV_FORMAT;

---------------------------------------------
-- 5) List Files in Stage (Verification Step)
---------------------------------------------
LIST @BRONZE.AZURE_BRONZE_STAGE;

----------------------------------------------
-- 6) Create Role + Grants (PERMISSIONS FIRST)
----------------------------------------------
-- Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Drop and recreate the DATA_ENGINEER role to start fresh
DROP ROLE IF EXISTS DATA_ENGINEER;
CREATE ROLE DATA_ENGINEER;

-- Grant usage on the warehouse
GRANT USAGE ON WAREHOUSE WEATHER_WH TO ROLE DATA_ENGINEER;
GRANT OPERATE ON WAREHOUSE WEATHER_WH TO ROLE DATA_ENGINEER;

-- Grant usage on the database
GRANT USAGE ON DATABASE EGYPT_WEATHER_DB TO ROLE DATA_ENGINEER;

-- Grant ALL privileges on the database
GRANT ALL PRIVILEGES ON DATABASE EGYPT_WEATHER_DB TO ROLE DATA_ENGINEER;

-- Grant ALL on all schemas
GRANT ALL PRIVILEGES ON SCHEMA EGYPT_WEATHER_DB.BRONZE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON SCHEMA EGYPT_WEATHER_DB.SILVER TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON SCHEMA EGYPT_WEATHER_DB.GOLD TO ROLE DATA_ENGINEER;

-- Grant ALL on existing tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA EGYPT_WEATHER_DB.BRONZE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA EGYPT_WEATHER_DB.SILVER TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA EGYPT_WEATHER_DB.GOLD TO ROLE DATA_ENGINEER;

-- Grant ALL on future tables
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA EGYPT_WEATHER_DB.BRONZE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA EGYPT_WEATHER_DB.SILVER TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA EGYPT_WEATHER_DB.GOLD TO ROLE DATA_ENGINEER;

-- Grant ALL on existing views
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA EGYPT_WEATHER_DB.BRONZE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA EGYPT_WEATHER_DB.SILVER TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA EGYPT_WEATHER_DB.GOLD TO ROLE DATA_ENGINEER;

-- Grant ALL on future views
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA EGYPT_WEATHER_DB.BRONZE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA EGYPT_WEATHER_DB.SILVER TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA EGYPT_WEATHER_DB.GOLD TO ROLE DATA_ENGINEER;

-- Grant file format and stage privileges
GRANT ALL PRIVILEGES ON ALL FILE FORMATS IN SCHEMA EGYPT_WEATHER_DB.BRONZE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE FILE FORMATS IN SCHEMA EGYPT_WEATHER_DB.BRONZE TO ROLE DATA_ENGINEER;

GRANT ALL PRIVILEGES ON ALL STAGES IN SCHEMA EGYPT_WEATHER_DB.BRONZE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE STAGES IN SCHEMA EGYPT_WEATHER_DB.BRONZE TO ROLE DATA_ENGINEER;

---------------------------------------
-- 7) Create User (AFTER role is ready)
---------------------------------------
CREATE USER IF NOT EXISTS <YOUR_USER>
  PASSWORD = '<YOUR_PASSWORD>'
  DEFAULT_ROLE = DATA_ENGINEER
  DEFAULT_WAREHOUSE = WEATHER_WH
  DEFAULT_NAMESPACE = EGYPT_WEATHER_DB.BRONZE;

-- Assign role to user
GRANT ROLE DATA_ENGINEER TO USER <YOUR_USER>;
```

### Step 4: Upload Historical Data to Azure
Upload these files to Azure Blob Storage under `bronze/` directory:
- `air_pollution_20210101_20241231.csv`
- `solar_energy_20060101_20241231.csv`
- `weather_19810101_20241231.csv`

Verify files are uploaded:
```bash
# In Snowflake
LIST @BRONZE.AZURE_BRONZE_STAGE;
```

### Step 5: Fix Permissions (Linux/Mac Only)
```bash
chmod -R 777 airflow/logs
chmod -R 777 airflow/plugins
sudo chmod 666 /var/run/docker.sock
```

**Windows users**: Skip this step (not needed).

### Step 6: Build and Start Services
```bash
docker-compose up -d --build
```

Wait 2-3 minutes for all services to initialize.

### Step 7: Verify Services
```bash
# Check all containers are running
docker-compose ps

# Should see 5 services:
# - postgres (healthy)
# - airflow_init (exited with code 0)
# - airflow_webserver (healthy)
# - airflow_scheduler (running)
# - dbt_service (can be stopped so don't worry)
```

### Step 8: Create Snowflake Connection in Airflow

1. Open Airflow UI: http://localhost:8080
2. Login with:
   - Username: `admin`
   - Password: `admin`
3. Go to **Admin → Connections**
4. Click **+** to add new connection
5. Fill in:
   - **Connection Id**: `snowflake_default`
   - **Connection Type**: `Snowflake`
   - **Schema**: `BRONZE`
   - **Login**: Your Snowflake username
   - **Password**: Your Snowflake password
   - **Account**: Your Snowflake account (e.g., `gb16096.uaenorth.azure`)
   - **Warehouse**: `WEATHER_WH`
   - **Database**: `EGYPT_WEATHER_DB`
   - **Role**: `DATA_ENGINEER`
6. Click **Save**

## Running the Pipeline

### Initial Run (Historical Data)

1. Go to Airflow UI: http://localhost:8080
2. Find DAG: `egyptian_weather_data_pipeline`
3. Toggle it **ON** (if paused)
4. Click **Trigger DAG** (play button)
5. Monitor progress:
   - Click on the DAG run
   - View task logs by clicking on each task

**Expected Flow**:
```
✓ upload_new_files_to_azure (skips - no files in new_data/)
✓ check_files_in_azure (finds historical files)
✓ load_data_from_azure (loads to Snowflake Bronze)
✓ dbt_run_silver (cleans and standardizes)
✓ dbt_run_gold (builds dimensional model)
✓ dbt_test (runs data quality checks)
```

### Verify in Snowflake
```sql
-- Check data loaded
SELECT 'AIR_POLLUTION' AS TABLE_NAME, COUNT(*) FROM BRONZE.AIR_POLLUTION
UNION ALL
SELECT 'SOLAR_ENERGY', COUNT(*) FROM BRONZE.SOLAR_ENERGY
UNION ALL
SELECT 'WEATHER', COUNT(*) FROM BRONZE.WEATHER;

-- Check Silver layer
SELECT COUNT(*) FROM SILVER.SILVER_AIR_POLLUTION;
SELECT COUNT(*) FROM SILVER.SILVER_SOLAR_ENERGY;
SELECT COUNT(*) FROM SILVER.SILVER_WEATHER;

-- Check Gold layer
SELECT COUNT(*) FROM GOLD.DIM_DATE;
SELECT COUNT(*) FROM GOLD.DIM_GOVERNORATE;
SELECT COUNT(*) FROM GOLD.FACT_AIR_POLLUTION;
SELECT COUNT(*) FROM GOLD.FACT_SOLAR_ENERGY;
SELECT COUNT(*) FROM GOLD.FACT_WEATHER;
```

## Adding Incremental Data

### Step 1: Add New CSV Files
Place incremental CSV files in the `new_data/` directory:

### Step 2: Run Pipeline Again
1. Go to Airflow UI
2. Trigger the DAG manually
3. The pipeline will:
   - Detect new files in `new_data/`
   - Upload them to Azure Blob Storage (`bronze/`)
   - Load incremental data to Snowflake
   - Run dbt transformations (incremental logic)
   - Run tests

### Step 3: Verify Incremental Load
```sql
-- Check new data in Bronze (should have 2025 dates)
SELECT DATE(DATETIME) AS DATE, COUNT(*) 
FROM BRONZE.AIR_POLLUTION 
WHERE YEAR(DATETIME) = 2025 
GROUP BY DATE 
ORDER BY DATE DESC 
LIMIT 10;

-- Check Silver layer updated
SELECT MAX(DATETIME) FROM SILVER.SILVER_AIR_POLLUTION;

-- Check Gold layer updated
SELECT MAX(DATE_VALUE) FROM GOLD.DIM_DATE;
```

## Data Models

### Bronze Layer (Raw Data)
- `AIR_POLLUTION`: Hourly air quality measurements (2021-2025)
- `SOLAR_ENERGY`: Hourly solar radiation data (2006-2025)
- `WEATHER`: Daily weather observations (1981-2025)

### Silver Layer (Cleaned Data)
- `SILVER_AIR_POLLUTION`: Standardized governorate names, cleaned nulls
- `SILVER_SOLAR_ENERGY`: Datetime constructed, governorate standardized
- `SILVER_WEATHER`: Governorate standardized, column names improved

**Key Transformations**:
- City names standardized (Siwah → Matrouh, Alex → Alexandria)
- Column names renamed for clarity
- Null values handled
- Incremental loading logic (new data only)

### Gold Layer (Dimensional Model)

**Dimensions**:
- `DIM_DATE`: Date dimension (1981-2025) with year, quarter, month, week, season
- `DIM_GOVERNORATE`: 27 Egyptian governorates grouped into 5 regions

**Facts**:
- `FACT_AIR_POLLUTION`: Hourly air quality with pollutant levels
- `FACT_SOLAR_ENERGY`: Hourly solar radiation and UV index
- `FACT_WEATHER`: Daily temperature, humidity, precipitation, wind

## Maintenance

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f dbt
```

### Restart Services
```bash
# All services
docker-compose restart

# Specific service
docker-compose restart airflow-scheduler
```

### Stop Pipeline
```bash
docker-compose down
```

### Clean Everything (Fresh Start)
```bash
# WARNING: This deletes all containers, volumes, and logs
docker-compose down -v
rm -rf airflow/logs/*
rm -rf new_data/*.csv
docker-compose up -d --build
```
