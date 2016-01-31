#!/bin/sh

DIR=`dirname $0`

ID="per_origin_destination_mean_arrival_delay"

echo "\nCleaning up HDFS output directory\n"
#hadoop fs -rm -R /user/ubuntu/results/$ID

echo "\nSetting up Cassandra\n"
#cqlsh -f $DIR/cql/setup_$ID.cql

echo "\nRunning Hadoop jobs\n"
#pig -Dpig.additional.jars=/usr/share/cassandra/apache-cassandra.jar:/usr/share/cassandra/cassandra-driver-core-2.0.9.2.jar:/usr/share/cassandra/lib/*.jar -f $DIR/pig/$ID.pig

echo "\nStoring in Cassandra\n"
hadoop fs -get /user/ubuntu/results/$ID $ID.csv
cqlsh -f $DIR/cql/store_$ID.cql
