#!/bin/bash

hive <<EOF

CREATE EXTERNAL TABLE bike_rides (
  duration INT,
  start_time STRING,
  end_time STRING,
  start_station_number INT,
  start_station STRING,
  end_station_number INT,
  end_station STRING,
  bike_number STRING,
  member_type STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
LOCATION '/data/bike_rides'
;

ALTER TABLE bike_rides SET TBLPROPERTIES ("skip.header.line.count"="1");

EOF
