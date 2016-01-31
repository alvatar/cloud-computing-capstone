#!/usr/bin/env sh

BASEDIR=$(dirname $0)

pig -Dpig.additional.jars=$BASEDIR/piggybank-0.15.0.jar -x tez -f $1
