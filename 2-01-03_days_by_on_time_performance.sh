#!/bin/sh

DIR=`dirname $0`
$DIR/pig/run-pig.sh $DIR/pig/days_by_on_time_arrival_performance.pig
