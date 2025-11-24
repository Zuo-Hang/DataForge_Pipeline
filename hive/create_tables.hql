CREATE TABLE IF NOT EXISTS users_etl (
  user_id BIGINT,
  name STRING,
  age DOUBLE,
  signup_dt DATE
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS orders_etl (
  order_id STRING,
  user_id BIGINT,
  amount DOUBLE,
  order_time TIMESTAMP,
  status STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET;

