#!/bin/bash

echo " -----------------------------------------------"
echo "Calling the function for all months..."

hive <<EOF | grep -v 'WARN:'

ADD JAR /shared/lab_c2_udf/grid-udfs/dist/grid.udf-1.0.0-fat.jar;
CREATE TEMPORARY FUNCTION astro_sign AS 'edu.ucsc.grid.udf.AstrologicalSign';

select astro_sign('1990-01-02');
select astro_sign('1990-02-02');
select astro_sign('1990-03-02');
select astro_sign('1990-04-02');
select astro_sign('1990-05-02');
select astro_sign('1990-06-02');
select astro_sign('1990-07-02');
select astro_sign('1990-08-02');
select astro_sign('1990-09-02');
select astro_sign('1990-10-02');
select astro_sign('1990-11-02');
select astro_sign('1990-12-02');

EOF
echo " -----------------------------------------------"
echo "Showing help facility..."
echo " "
echo " "
hive <<EOF | grep -v 'WARN:'

ADD JAR /shared/lab_c2_udf/grid-udfs/target/grid.udf-1.0.0-fat.jar;  
CREATE TEMPORARY FUNCTION astro_sign AS 'edu.ucsc.grid.udf.AstrologicalSign';

DESCRIBE FUNCTION EXTENDED astro_sign;

EOF
