#!/bin/bash -xe

mvn package
cp target/grid.udf-1.0.0-fat.jar dist/grid.udf-1.0.0-fat.jar
