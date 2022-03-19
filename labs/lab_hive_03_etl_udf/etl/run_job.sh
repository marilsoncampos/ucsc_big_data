#!/bin/bash -xe

./job_nasa.py run --cfg_file=config.json --dt_date=0702


echo  "-------------------------- xxx --------------------------" > /dev/null
echo ""

hive <<EOF

show partitions nasa_daily;

EOF
