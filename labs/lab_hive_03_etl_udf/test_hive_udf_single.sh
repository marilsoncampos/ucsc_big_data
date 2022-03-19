#!/bin/bash

BIRTH_DATE="2018-11-04"
hive  <<EOF | egrep -v "^....>" | grep -v "WARN" > /tmp/hive_udf.txt

ADD JAR /shared/lab_c2_udf/grid-udfs/dist/grid.udf-1.0.0-fat.jar;  

CREATE TEMPORARY FUNCTION astro_sign AS 'edu.ucsc.grid.udf.AstrologicalSign';

select astro_sign("${BIRTH_DATE}");

EOF

echo " "
echo -n "The astrological sign for a person born in ${BIRTH_DATE} is "
cat /tmp/hive_udf.txt


