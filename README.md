# Airflow Data Pipeline Project

A complete data pipeline implementation using Apache Airflow to load and transform data from S3 into Amazon Redshift, with automated data quality checks.

## Overview

This project implements an ETL pipeline that:
- Stages JSON data from S3 to Redshift
- Transforms raw data into a star schema (fact and dimension tables)
- Performs automated data quality validation
- Handles failures with retry mechanisms

## Architecture

**Data Flow:**
```
S3 (JSON files) → Redshift Staging Tables → Fact/Dimension Tables → Data Quality Checks
```

**Tables:**
- **Staging:** `staging_events`, `staging_songs`
- **Fact:** `songplays` 
- **Dimensions:** `users`, `songs`, `artists`, `time`

## Prerequisites

- Docker Desktop installed
- AWS account with Redshift Serverless cluster
- S3 bucket with data (copied from udacity-dend bucket)

## Setup Instructions

### 1. Start Airflow
```bash
docker-compose up -d
```
Access Airflow UI: http://localhost:8080 (credentials: `airflow`/`airflow`)

### 2. Configure Connections

In Airflow UI, go to **Admin > Connections** and create:

**AWS Credentials Connection:**
- Conn Id: `aws_credentials`
- Conn Type: `aws`
- Login: `<your-aws-access-key>`
- Password: `<your-aws-secret-key>`

**Redshift Connection:**
- Conn Id: `redshift`
- Conn Type: `Postgres`
- Host: `<your-redshift-endpoint>`
- Database: `<your-database>`
- Login: `<username>`
- Password: `<password>`
- Port: `5439`

### 3. Create Required Tables

Connect to your Redshift database and run the following SQL:

<details>
<summary>Click to expand SQL statements</summary>

```sql
-- Staging Tables
CREATE TABLE staging_events (
    artist VARCHAR(255),
    auth VARCHAR(255),
    firstName VARCHAR(255),
    gender VARCHAR(10),
    itemInSession INTEGER,
    lastName VARCHAR(255),
    length DECIMAL(12,5),
    level VARCHAR(50),
    location VARCHAR(255),
    method VARCHAR(10),
    page VARCHAR(50),
    registration DECIMAL(14,1),
    sessionId INTEGER,
    song VARCHAR(255),
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR(255),
    userId INTEGER
);

CREATE TABLE staging_songs (
    num_songs INTEGER,
    artist_id VARCHAR(50),
    artist_latitude DECIMAL(10,6),
    artist_longitude DECIMAL(10,6),
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    song_id VARCHAR(50),
    title VARCHAR(255),
    duration DECIMAL(12,5),
    year INTEGER
);

-- Fact Table
CREATE TABLE songplays (
    songplay_id VARCHAR(32) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    user_id INTEGER NOT NULL,
    level VARCHAR(50),
    song_id VARCHAR(50),
    artist_id VARCHAR(50),
    session_id INTEGER,
    location VARCHAR(255),
    user_agent VARCHAR(255),
    PRIMARY KEY (songplay_id)
);

-- Dimension Tables
CREATE TABLE users (
    user_id INTEGER NOT NULL,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender VARCHAR(10),
    level VARCHAR(50),
    PRIMARY KEY (user_id)
);

CREATE TABLE songs (
    song_id VARCHAR(50) NOT NULL,
    title VARCHAR(255),
    artist_id VARCHAR(50),
    year INTEGER,
    duration DECIMAL(12,5),
    PRIMARY KEY (song_id)
);

CREATE TABLE artists (
    artist_id VARCHAR(50) NOT NULL,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    PRIMARY KEY (artist_id)
);

CREATE TABLE time (
    start_time TIMESTAMP NOT NULL,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday INTEGER,
    PRIMARY KEY (start_time)
);
```
</details>

### 4. Update S3 Configuration

Update the S3 bucket in `dags/final_project.py` to use your own bucket:
```python
s3_bucket="your-bucket-name"  # Replace with your bucket
```

## Implementation Details

### Custom Operators

**StageToRedshiftOperator:**
- Loads JSON data from S3 to Redshift using COPY commands
- Supports cross-region copying with proper REGION parameter
- Includes error handling and detailed logging
- Uses optimized COPY parameters for performance

**LoadFactOperator:**
- Appends data to fact tables
- Uses SQL transformations from helper class
- Supports incremental loading

**LoadDimensionOperator:**
- Implements truncate-insert pattern for dimension tables
- Configurable insert modes
- Ensures data freshness

**DataQualityOperator:**
- Runs configurable SQL-based data quality checks
- Compares results with expected values
- Raises exceptions on check failures

### DAG Configuration

- **Schedule:** Hourly (`0 * * * *`)
- **Retries:** 3 attempts with 5-minute delays
- **Timeout:** 30 minutes per task
- **Catchup:** Disabled
- **Dependencies:** Properly configured task flow

## Running the Pipeline

1. Ensure all prerequisites are met
2. Start the Airflow services
3. Configure connections and create tables
4. Trigger the `final_project` DAG manually or wait for scheduled run
5. Monitor task execution in the Airflow UI

## Troubleshooting

**Common Issues:**
- **Table not found:** Ensure all tables are created in Redshift
- **Connection errors:** Verify AWS and Redshift connections
- **S3 access denied:** Check AWS credentials and bucket permissions
- **Import errors:** Restart Docker containers if needed

**Logs:** Access detailed task logs via Airflow UI → DAG → Task → Log button

## Project Structure

```
├── dags/
│   └── final_project.py          # Main DAG definition
├── plugins/
│   ├── operators/
│   │   ├── stage_redshift.py     # S3 to Redshift operator
│   │   ├── load_fact.py          # Fact table loader
│   │   ├── load_dimension.py     # Dimension table loader
│   │   └── data_quality.py       # Data quality checker
│   └── helpers/
│       └── sql_queries.py        # SQL transformation queries
└── docker-compose.yml            # Airflow services configuration
```
