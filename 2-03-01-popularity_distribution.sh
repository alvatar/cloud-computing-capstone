#!/bin/sh

DIR=`dirname $0`
$DIR/pig/run-pig.sh $DIR/pig/airports_by_popularity.pig

hadoop fs -get /user/ubuntu/results/airports_by_popularity/part-r-00000 airports_by_popularity.csv

$DIR/plot_airports.py airports_by_popularity.csv
