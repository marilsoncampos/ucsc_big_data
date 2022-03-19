#!/bin/bash


hive <<EOF

SELECT
  start_station,
  duration,
  SUBSTRING(start_time, 1, 13) as time_slot, 
  RANK() OVER
   (PARTITION BY start_station ORDER BY SUBSTRING(start_time, 1, 13)) AS rank
FROM bike_rides
LIMIT 40
;

EOF

