#!/usr/bin/env bash

SCRIPT_DIR=python
THIS="$(basename $0)"
THIS=${THIS/.sh}
TOPIC=origin-destination

spark-submit --master yarn --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.2 --num-executors 6 --deploy-mode client $SCRIPT_DIR/${THIS}.py ip-172-30-0-47.ec2.internal:6667 $TOPIC
