# Openweathermap_DataEngineering_project

## Overview
This project is an automated ETL pipeline for processing weather data from the OpenWeather API, transforming it, and storing it in Snowflake. The pipeline is orchestrated using Apache Airflow running on an EC2 instance. Data flows through S3 buckets for staging and transformation before being ingested into Snowflake via Snowpipe for further analysis.

## Architecture
### Data Flow
1. Source (OpenWeather API): Extract weather data for a specific city via OpenWeather API.
2. S3 Staging Bucket: Raw JSON data is stored in an S3 bucket.
3. Transform (EC2): The data is transformed into a structured CSV format and uploaded back to S3.
4. Snowpipe: Automated ingestion from S3 into Snowflake is handled by Snowpipe.
5. Snowflake Database: Transformed data is stored in a Snowflake table for querying and analysis.
### Automation
- Apache Airflow: Orchestrates the entire pipeline, including extraction, transformation, and triggering Snowflake ingestion.
- AWS EC2: Hosts the Airflow instance.
