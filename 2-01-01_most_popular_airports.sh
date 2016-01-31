#!/bin/sh

DIR=`dirname $0`
$DIR/pig/run-pig.sh $DIR/pig/most_popular_airports.pig
