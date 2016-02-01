#!/bin/sh

DIR=`dirname $0`

ID="per_origin_destination_mean_arrival_delay"

echo "\nCleaning up HDFS output directory\n"
#hadoop fs -rm -R /user/ubuntu/results/$ID

echo "\nSetting up Cassandra\n"
#cqlsh -f $DIR/cql/setup_$ID.cql

echo "\nRunning Hadoop jobs\n"
$DIR/pig/run-pig.sh $DIR/pig/$ID.pig

echo "\nStoring in Cassandra\n"
#hadoop fs -getmerge /user/ubuntu/results/$ID $ID.csv
#cqlsh -f $DIR/cql/store_$ID.cql
