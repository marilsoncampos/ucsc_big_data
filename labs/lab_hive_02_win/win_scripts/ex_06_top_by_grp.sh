#!/bin/bash


hive <<EOF

SELECT 
  * 
FROM (
  SELECT 
    start_station,
    start_time,
    duration,
    ROW_NUMBER() OVER 
      (PARTITION BY start_station ORDER BY start_time) AS row_number
  FROM bike_rides 
) q1
WHERE row_number < 3
LIMIT 40  

;

EOF

