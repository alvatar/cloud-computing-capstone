#!/bin/sh

DIR=`dirname $0`

ID="traveler"

echo "\nCleaning up HDFS output directory\n"
#hadoop fs -rm -R /user/ubuntu/results/$ID

echo "\nSetting up Cassandra\n"
cqlsh -f $DIR/cql/setup_$ID.cql

echo "\nRunning Hadoop jobs\n"
pig -x tez -f $DIR/pig/$ID.pig

echo "\nStoring in Cassandra\n"
#hadoop fs -get /user/ubuntu/results/$ID $ID.csv
#cqlsh -f $DIR/cql/store_$ID.cql
#for i in part*; do echo "Working on $i" && mv $i current && cqlsh -f ~/scripts/cql/store_traveler.cql ; mv current $i; done
