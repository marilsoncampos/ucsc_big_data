#!/bin/bash

hive <<EOF

DROP TABLE IF EXISTS nasa_daily;

CREATE TABLE nasa_daily (
   host STRING,   
   request_time STRING,  
   page_url STRING,
   error_cide STRING,
   page_size STRING)
 PARTITIONED BY (dt_date STRING)
 STORED AS TEXTFILE;

EOF
