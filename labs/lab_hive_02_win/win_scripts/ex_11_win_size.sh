#!/bin/bash


hive <<EOF

DROP TEMPORARY MACRO IF EXISTS hour_slot;
CREATE TEMPORARY MACRO hour_slot (dt_str STRING)
  SUBSTRING(dt_str, 12, 2);

WITH rides_map_to_hour AS (
SELECT
  hour_slot(start_time) AS time_slot,
  ROUND(SUM(duration), 2) as total_duration,
  COUNT(1) AS number_of_rides
FROM bike_rides
GROUP BY hour_slot(start_time)
),
avg_3_slot_per_station AS (
SELECT 
  time_slot,
  ROUND(SUM(total_duration) OVER window_spec/SUM(number_of_rides) OVER window_spec, 2) AS avg_3h_duration
FROM rides_map_to_hour
WINDOW window_spec AS
   (ORDER BY time_slot
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
),
most_busy_hour_slots AS (
SELECT 
  time_slot, avg_3h_duration
FROM avg_3_slot_per_station
ORDER BY avg_3h_duration DESC
LIMIT 3
),
least_busy_hour_slots AS (
SELECT 
  time_slot, avg_3h_duration
FROM avg_3_slot_per_station
ORDER BY avg_3h_duration 
LIMIT 3
)


SELECT * FROM (
  SELECT * FROM most_busy_hour_slots
  UNION ALL 
  SELECT * FROM least_busy_hour_slots
) q1
;

EOF

