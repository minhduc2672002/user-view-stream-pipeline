# Project requirements

## Overview

The problem combines the use of `kafka` and `spark`. Use `spark` to read data from `kafka` then process, calculate and save
stored in database `postgres`

## Problem

**Input:**

- Kafka: The Kafka cluster is set up locally, and the topic contains data on user behavior on the website, which was implemented in the Kafka module of the project.
- Spark: The Spark cluster is installed locally as part of the course.
- Data schema
- The data stored on Kafka
  ![image](https://github.com/user-attachments/assets/b0e13ff8-85ff-4e62-8286-c7e9801f4b22)

  
**Output:**

- Database design
- Program code to handle project requirements
- Results of the reports as requested
- Data stored in the Postgres database
## Describe

**Data schema:**

| Name          | Data type  | Describe                                                   | example                                                                                                                                                               |
|--------------|---------------|---------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| id           | String        | Log id                                                  | aea4b823-c5c6-485e-8b3b-6182a7c4ecce                                                                                                                                |
| api_version  | String        | Version của api                                         | 1.0                                                                                                                                                                 | 
| collection   | String        | Loại log                                                | view_product_detail                                                                                                                                                 | 
| current_url  | String        | Url của trang web mà người dùng đang vào                | https://www.glamira.cl/glamira-anillo-saphira-skug100335.html?alloy=white-375&diamond=sapphire&stone2=diamond-Brillant&itm_source=recommendation&itm_medium=sorting |
| device_id    | String        | id của thiết bị                                         | 874db849-68a6-4e99-bcac-fb6334d0ec80                                                                                                                                |
| email        | String        | Email của người dùng                                    |                                                                                                                                                                     |
| ip           | String        | Địa chỉ ip                                              | 190.163.166.122                                                                                                                                                     |
| local_time   | String        | Thời gian log được tạo. Format dạng yyyy-MM-dd HH:mm:ss | 2024-05-28 08:31:22                                                                                                                                                 |
| option       | Array<Object> | Danh sách các option của sản phẩm                       | `[{"option_id": "328026", "option_label": "diamond"}]`                                                                                                              |
| product_id   | String        | Mã id của sản phẩm                                      | 96672                                                                                                                                                               |
| referrer_url | String        | Đường dẫn web dẫn đến link `current_url`                | https://www.google.com/                                                                                                                                             |
| store_id     | String        | Mã id của cửa hàng                                      | 85                                                                                                                                                                  |
| time_stamp   | Long          | Timestamp thời điểm bản ghi log được tạo                |                                                                                                                                                                     |
| user_agent   | String        | Thông tin của browser, thiết bị                         | Mozilla/5.0 (iPhone; CPU iPhone OS 13_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1 Mobile/15E148 Safari/604.1                           |

**Requirement:**

Design the database and write the program to generate the following reports:

- Top 10 product_id with the highest views on the current day or the most recent day
- Top 10 countries with the highest views on the current day or the most recent day (countries are identified based on domain)
- Top 5 referrer_url with the highest views on the current day or the most recent day
- For any given country, retrieve the list of store_id and their corresponding view counts, sorted in descending order by views
- View data distribution by hour for a specific product_id on the current day
- View data by hour for each browser and os

## Data warehouse design
![image](https://github.com/user-attachments/assets/7c744826-0048-445e-8c83-d1a95297fe9f)

## Analysis
- Top 10 product_id with the highest views on the current day or the most recent day
  ![image](https://github.com/user-attachments/assets/b3c72c4c-0cb0-4320-885d-a5c5b587122c)
- Top 10 countries with the highest views on the current day or the most recent day (countries are identified based on domain)

  
  ![image](https://github.com/user-attachments/assets/baf4fdef-c842-4f12-b313-dda91a733221)
- Top 5 referrer_url with the highest views on the current day or the most recent day
![image](https://github.com/user-attachments/assets/871050cf-a05b-4f8b-b084-d194856e5f6c)
- For any given country, retrieve the list of store_id and their corresponding view counts, sorted in descending order by views
![image](https://github.com/user-attachments/assets/5475ccec-f602-4763-a96b-3e63b90c613e)
- View data by hour for each browser

  ![image](https://github.com/user-attachments/assets/a2b4fc07-c473-4287-9ec1-b9d661278e0c)
- View data by hour for each os

  ![image](https://github.com/user-attachments/assets/1bb7fef7-fbbb-4394-b4dd-90e5b5f32fc2)
  
## Run command line

**Run a program using external libraries through a virtual environment**

```
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
unigap/spark:3.5 bash -c "python -m venv pyspark_venv &&
source pyspark_venv/bin/activate &&
pip install -r /spark/requirements.txt &&
venv-pack -o pyspark_venv.tar.gz &&
spark-submit \
--packages org.postgresql:postgresql:42.7.3 \
--archives pyspark_venv.tar.gz#environment \
--py-files /spark/postgres.zip \
/spark/create_dimension.py"


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
unigap/spark:3.5 bash -c "python -m venv pyspark_venv &&
source pyspark_venv/bin/activate &&
pip install -r /spark/requirements.txt &&
venv-pack -o pyspark_venv.tar.gz &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
--archives pyspark_venv.tar.gz#environment \
--py-files /spark/postgres.zip \
/spark/main_streaming.py"
```

## Reference links

[Python Package Management](https://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html)

[JDBC To Other Databases](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)
