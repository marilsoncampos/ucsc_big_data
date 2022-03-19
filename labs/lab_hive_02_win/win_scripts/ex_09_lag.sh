#!/bin/bash


hive <<EOF

SELECT 
  start_station,
  duration,
  duration - LAG(duration, 1) OVER
    (PARTITION BY start_station ORDER BY duration) AS delta
FROM bike_rides
ORDER BY start_station, duration
LIMIT 40
;

EOF

