#!/bin/sh

DIR=`dirname $0`
$DIR/pig/run-pig.sh $DIR/pig/airports_by_popularity.pig

hadoop fs -getmerge /user/ubuntu/results/airports_by_popularity airports_by_popularity.csv

$DIR/plot_airports.py airports_by_popularity.csv
