FROM python:3.10-slim

# Set working directory
WORKDIR /usr/app/dbt

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Install dbt dependencies
# (installs dbt-utils)
RUN dbt deps

# Create necessary directories
RUN mkdir -p /usr/app/dbt/egypt_weather/logs && \
    chmod -R 777 /usr/app/dbt/egypt_weather

# Set environment variable for DBT profiles
ENV DBT_PROFILES_DIR=/usr/app/dbt/egypt_weather

WORKDIR /usr/app/dbt/egypt_weather