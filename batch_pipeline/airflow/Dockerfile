FROM apache/airflow:2.7.3-python3.10

USER root

# Install build dependencies for connectors
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python packages
# This includes snowflake, docker, and azure providers/libraries
RUN pip install --no-cache-dir -r /requirements.txt