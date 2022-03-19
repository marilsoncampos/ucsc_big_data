#!/bin/bash


hive <<EOF

SELECT
  start_station,
  duration,
  SUM(duration) OVER (PARTITION BY start_station) AS start_terminal_total,
  round((duration/SUM(duration) OVER (PARTITION BY start_station)) * 100, 2) AS pct_of_time
FROM bike_rides
ORDER BY start_station, pct_of_time
LIMIT 40
;

EOF

