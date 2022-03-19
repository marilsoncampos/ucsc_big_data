#!/bin/bash


hive <<EOF

SELECT 
  start_station,
  duration,
  LAG(duration) OVER window_spec AS prev,
  LEAD(duration) OVER window_spec AS next
FROM bike_rides
ORDER BY start_station, duration
WINDOW window_spec AS
         (PARTITION BY start_station ORDER BY duration)

LIMIT 40
;

EOF

