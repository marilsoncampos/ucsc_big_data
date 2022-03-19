#!/bin/bash


hive <<EOF

WITH nmbered_rides AS (
SELECT 
  start_station,
  start_time,
  duration,
  SUM(duration) OVER
    (PARTITION BY start_station ORDER BY start_time)
     AS running_total,
  ROW_NUMBER() OVER (PARTITION BY start_station ORDER BY start_time)
     AS ride_num
FROM bike_rides
)

SELECT * FROM nmbered_rides
WHERE ride_num <= 5
LIMIT 40
;
EOF

