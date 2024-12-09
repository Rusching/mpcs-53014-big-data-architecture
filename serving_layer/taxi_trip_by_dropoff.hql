CREATE EXTERNAL TABLE shichen_taxi_trips_by_dropoff_hive (
dropoff_community_area INT,
trip_id STRING,
taxi_id STRING,
trip_start_timestamp STRING,
trip_end_timestamp STRING,
trip_miles DOUBLE,
fare DOUBLE,
trip_total DOUBLE
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,trip_info:trip_id,trip_info:taxi_id,trip_info:trip_start_timestamp,trip_info:trip_end_timestamp,trip_info:trip_miles,trip_info:fare,trip_info:trip_total"
)
TBLPROPERTIES ("[hbase.table.name](http://hbase.table.name/)" = "shichen_taxi_trips_by_dropoff");

INSERT OVERWRITE TABLE shichen_taxi_trips_by_dropoff_hive
SELECT
dropoff_community_area,
trip_id,
taxi_id,
trip_start_timestamp,
trip_end_timestamp,
trip_miles,
fare,
trip_total
FROM shichen_taxi_trips
WHERE dropoff_community_area IS NOT NULL;