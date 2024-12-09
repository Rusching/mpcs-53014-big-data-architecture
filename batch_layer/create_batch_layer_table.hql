CREATE TABLE IF NOT EXISTS shichen_taxi_trips (
Trip_ID STRING,
Taxi_ID STRING,
Trip_Start_Timestamp STRING,
Trip_End_Timestamp STRING,
Trip_Seconds INT,
Trip_Miles DOUBLE,
Pickup_Census_Tract STRING,
Dropoff_Census_Tract STRING,
Pickup_Community_Area INT,
Dropoff_Community_Area INT,
Fare DOUBLE,
Tips DOUBLE,
Tolls DOUBLE,
Extras DOUBLE,
Trip_Total DOUBLE,
Payment_Type STRING,
Company STRING,
Pickup_Centroid_Latitude DOUBLE,
Pickup_Centroid_Longitude DOUBLE,
Pickup_Centroid_Location STRING,
Dropoff_Centroid_Latitude DOUBLE,
Dropoff_Centroid_Longitude DOUBLE,
Dropoff_Centroid_Location STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;