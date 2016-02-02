#!/bin/sh

DIR=`dirname $0`
hadoop fs -rm -R /user/ubuntu/results/airports_by_popularity
$DIR/pig/run-pig.sh $DIR/pig/airports_by_popularity.pig

hadoop fs -getmerge /user/ubuntu/results/airports_by_popularity results/airports_by_popularity.csv

$DIR/plot_airports.py results/airports_by_popularity.csv
