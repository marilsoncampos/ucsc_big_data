#!/bin/bash

hdfs dfs -mkdir /data/bike_rides/
hdfs dfs -put 201801_tripdata.csv /data/bike_rides/
hdfs dfs -put 201802_tripdata.csv /data/bike_rides/


