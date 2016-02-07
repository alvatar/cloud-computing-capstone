#!/bin/sh

DIR=`dirname $0`
hadoop fs -rm -R /user/ubuntu/results/days_by_on_time_arrival_performance
$DIR/pig/run-pig.sh $DIR/pig/days_by_on_time_arrival_performance.pig
