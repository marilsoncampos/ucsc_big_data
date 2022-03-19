#!/bin/bash


hive <<EOF

SELECT 
  start_station,
  duration,
  SUM(duration) OVER (PARTITION BY start_station) AS start_station_total
  FROM bike_rides
  LIMIT 40
;
EOF

