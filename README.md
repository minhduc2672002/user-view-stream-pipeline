# Project: Real-Time Web User Behavior Analysis with Kafka, Spark, and PostgreSQL

## Overview

This project demonstrates how to build a real-time data processing pipeline using Apache Kafka and Apache Spark. Data is ingested from a Kafka topic, processed using Spark, and the final results are stored in a PostgreSQL database for further analysis and reporting.

## Problem Statement

### Input

- **Kafka**: A locally deployed Kafka cluster where a topic contains user behavior logs from a website.
- **Spark**: A local Spark cluster used for real-time stream processing.
- **Data Format**: Structured JSON messages sent to Kafka (schema detailed below).

### Output

- PostgreSQL data warehouse populated with processed analytics data.
- Program code to generate reports.
- Visualization of insights based on user interaction data.

## Data Schema

| Field         | Type          | Description                                                | Example                                                                 |
|---------------|---------------|------------------------------------------------------------|-------------------------------------------------------------------------|
| `id`          | String        | Unique log identifier                                      | `aea4b823-c5c6-485e-8b3b-6182a7c4ecce`                                 |
| `api_version` | String        | API version                                                | `1.0`                                                                   |
| `collection`  | String        | Type of event/log                                          | `view_product_detail`                                                  |
| `current_url` | String        | URL visited by the user                                   | `https://www.glamira.cl/...`                                           |
| `device_id`   | String        | Device identifier                                          | `874db849-68a6-4e99-bcac-fb6334d0ec80`                                 |
| `email`       | String        | User email (if available)                                 |                                                                         |
| `ip`          | String        | IP address of the user                                    | `190.163.166.122`                                                      |
| `local_time`  | String        | Local time of event (yyyy-MM-dd HH:mm:ss)                 | `2024-05-28 08:31:22`                                                  |
| `option`      | Array<Object> | List of product options                                   | `[{"option_id": "328026", "option_label": "diamond"}]`                |
| `product_id`  | String        | Product identifier                                         | `96672`                                                                |
| `referrer_url`| String        | URL of the referring page                                 | `https://www.google.com/`                                              |
| `store_id`    | String        | Store identifier                                           | `85`                                                                   |
| `time_stamp`  | Long          | Event timestamp (epoch)                                   |                                                                         |
| `user_agent`  | String        | User browser/device information                           | `Mozilla/5.0 (iPhone; CPU iPhone OS 13_4_1...)`                        |

## Requirements

Design and implement a data pipeline and reporting logic to generate the following analytics:

- **Top 10** most viewed `product_id` on the current or latest day.
- **Top 10** countries by view count (derived from domain of `current_url`).
- **Top 5** `referrer_url` by views on the current or latest day.
- View count by `store_id` for a specific country, sorted by views.
- Hourly distribution of views for a specific `product_id`.
- Hourly view statistics by browser and operating system.

## Data Warehouse Design

![Data Warehouse Design](https://github.com/user-attachments/assets/7c744826-0048-445e-8c83-d1a95297fe9f)

## Data Pipeline Architecture

![Data Pipeline](https://github.com/user-attachments/assets/629ce1da-d6e0-40dd-b13d-a5fffce2a04d)

## Reporting & Analysis

- **Top 10 Most Viewed Products**

  ![Top Products](https://github.com/user-attachments/assets/b3c72c4c-0cb0-4320-885d-a5c5b587122c)

- **Top 10 Countries by Views**

  ![Top Countries](https://github.com/user-attachments/assets/baf4fdef-c842-4f12-b313-dda91a733221)

- **Top 5 Referrer URLs**

  ![Top Referrer URLs](https://github.com/user-attachments/assets/871050cf-a05b-4f8b-b084-d194856e5f6c)

- **Store Views by Country**

  ![Store Views](https://github.com/user-attachments/assets/5475ccec-f602-4763-a96b-3e63b90c613e)

- **Hourly Product Views**

  ![Hourly Views](https://github.com/user-attachments/assets/a2b4fc07-c473-4287-9ec1-b9d661278e0c)

- **Hourly Views by OS**

  ![Hourly OS Views](https://github.com/user-attachments/assets/1bb7fef7-fbbb-4394-b4dd-90e5b5f32fc2)

## Visualization

![Dashboard](https://github.com/user-attachments/assets/eb5abfba-64a9-4dd2-8b8a-09f12eaac5e5)

## Running the Pipeline with Docker (No Airflow)

### Step 1: Create Dimension Tables

```bash
docker container stop product-view-create-dimension || true &&
docker container rm product-view-create-dimension || true &&
docker run -ti --name product-view-create-dimension \
  --network=streaming-network \
  -p 4042:4040 \
  -v ./:/spark \
  -v spark_lib:/opt/bitnami/spark/.ivy2 \
  -v spark_data:/data \
  -e PYSPARK_DRIVER_PYTHON='python' \
  -e PYSPARK_PYTHON='./environment/bin/python' \
  unigap/spark:3.5 bash -c "
    python -m venv pyspark_venv &&
    source pyspark_venv/bin/activate &&
    pip install -r /spark/requirements.txt &&
    venv-pack -o pyspark_venv.tar.gz &&
    spark-submit \
      --packages org.postgresql:postgresql:42.7.3 \
      --archives pyspark_venv.tar.gz#environment \
      --py-files /spark/postgres.zip \
      /spark/create_dimension.py"
```

### Step 2: Start Streaming Job
```bash
docker container stop product-view-stream || true &&
docker container rm product-view-stream || true &&
docker run -ti --name product-view-stream \
  --network=streaming-network \
  -p 4042:4040 \
  -v ./:/spark \
  -v spark_lib:/opt/bitnami/spark/.ivy2 \
  -v spark_data:/data \
  -e PYSPARK_DRIVER_PYTHON='python' \
  -e PYSPARK_PYTHON='./environment/bin/python' \
  unigap/spark:3.5 bash -c "
    python -m venv pyspark_venv &&
    source pyspark_venv/bin/activate &&
    pip install -r /spark/requirements.txt &&
    venv-pack -o pyspark_venv.tar.gz &&
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
      --archives pyspark_venv.tar.gz#environment \
      --py-files /spark/postgres.zip \
      /spark/main_streaming.py"
```

## References

- [PySpark Packaging Guide](https://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html)
- [JDBC Data Sources](https://spark.apache.org/docs/latest/sql-data-sou)
