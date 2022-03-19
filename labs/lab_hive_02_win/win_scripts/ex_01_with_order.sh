#!/bin/bash


hive <<EOF

SELECT 
  start_time, duration,
  SUM(duration) OVER (ORDER BY start_time) AS running_total
FROM bike_rides
LIMIT 25
;
EOF

