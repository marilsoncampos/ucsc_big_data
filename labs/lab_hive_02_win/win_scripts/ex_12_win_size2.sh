#!/bin/bash


hive <<EOF

SELECT 
  start_station,
  duration,
  SUM(duration) OVER window_spec AS tot
FROM bike_rides
ORDER BY start_station, duration
WINDOW window_spec AS
   (PARTITION BY start_station ORDER BY duration
    ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
LIMIT 40
;

EOF

