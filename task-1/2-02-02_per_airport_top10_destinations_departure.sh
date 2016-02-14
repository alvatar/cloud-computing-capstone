#!/bin/sh

DIR=`dirname $0`

ID="per_airport_top10_destinations_departure"

echo "\nCleaning up HDFS output directory\n"
hadoop fs -rm -R /user/ubuntu/results/$ID

echo "\nSetting up Cassandra\n"
cqlsh -f $DIR/cql/setup_$ID.cql

echo "\nRunning Hadoop jobs\n"
$DIR/pig/run-pig.sh $DIR/pig/$ID.pig

echo "\n Storing in Cassandra\n"
hadoop fs -getmerge /user/ubuntu/results/$ID results/$ID.csv
cqlsh -f $DIR/cql/store_$ID.cql
