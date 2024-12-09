CREATE EXTERNAL TABLE shichen_taxi_trips_by_id_hive (
trip_id STRING,
taxi_id STRING,
trip_start_timestamp STRING,
trip_end_timestamp STRING,
trip_seconds INT,
trip_miles DOUBLE,
fare DOUBLE,
tips DOUBLE,
tolls DOUBLE,
extras DOUBLE,
trip_total DOUBLE,
payment_type STRING,
company STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,trip_info:taxi_id,trip_info:trip_start_timestamp,trip_info:trip_end_timestamp,trip_info:trip_seconds,trip_info:trip_miles,trip_info:fare,trip_info:tips,trip_info:tolls,trip_info:extras,trip_info:trip_total,trip_info:payment_type,trip_info:company"
)
TBLPROPERTIES ("[hbase.table.name](http://hbase.table.name/)" = "shichen_taxi_trips_by_id");

INSERT OVERWRITE TABLE shichen_taxi_trips_by_id_hive
SELECT
trip_id,
taxi_id,
trip_start_timestamp,
trip_end_timestamp,
trip_seconds,
trip_miles,
fare,
tips,
tolls,
extras,
trip_total,
payment_type,
company
FROM shichen_taxi_trips;