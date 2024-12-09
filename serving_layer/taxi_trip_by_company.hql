CREATE EXTERNAL TABLE shichen_taxi_trips_company_aggregated (
    company STRING,
    trip_count INT,
    total_fare DOUBLE,
    total_trip_seconds DOUBLE,
    top_pickup_area STRING,
    top_dropoff_area STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,trip_info:trip_count,trip_info:total_fare,trip_info:total_trip_seconds,trip_info:top_pickup_area,trip_info:top_dropoff_area"
)
TBLPROPERTIES ("hbase.table.name" = "shichen_taxi_trips_by_company");

-- Step 2: 插入聚合结果
INSERT OVERWRITE TABLE shichen_taxi_trips_company_aggregated 
SELECT
    company,
    COUNT(*) AS trip_count,
    SUM(fare) AS total_fare,
    SUM(UNIX_TIMESTAMP(trip_end_timestamp, 'MM/dd/yyyy hh:mm:ss a') - UNIX_TIMESTAMP(trip_start_timestamp, 'MM/dd/yyyy hh:mm:ss a')) AS total_trip_seconds,
    (SELECT pickup_community_area FROM shichen_taxi_trips GROUP BY pickup_community_area ORDER BY COUNT(*) DESC LIMIT 1) AS top_pickup_area,
    (SELECT dropoff_community_area FROM shichen_taxi_trips GROUP BY dropoff_community_area ORDER BY COUNT(*) DESC LIMIT 1) AS top_dropoff_area
FROM shichen_taxi_trips
GROUP BY company;
