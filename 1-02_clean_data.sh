#!/bin/sh

DIR=`dirname $0`
$DIR/pig/run-pig.sh $DIR/pig/clean_data.pig
