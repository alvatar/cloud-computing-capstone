#!/bin/sh

DIR=`dirname $0`
hadoop fs -rm -R /user/ubuntu/results/most_popular_airports
$DIR/pig/run-pig.sh $DIR/pig/most_popular_airports.pig
