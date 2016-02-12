#!/usr/bin/env bash

SCRIPT_DIR=python
THIS="$(basename $0)"
THIS=${THIS/.sh}
TOPIC=origin-destination

spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.2 \
  --master yarn \
  --deploy-mode client \
  --driver-memory 4g \
  --executor-memory 4g \
  --num-executors 24 \
  --executor-cores 4 \
  --conf spark.yarn.executor.memoryOverhead=400 \
  --conf spark.streaming.backpressure.enabled=true \
  --conf spark.streaming.kafka.maxRate=5000 \
  $SCRIPT_DIR/${THIS}.py ip-172-30-0-239.ec2.internal:2181 $TOPIC
  #$SCRIPT_DIR/${THIS}.py ip-172-30-0-47.ec2.internal:6667 $TOPIC

  # This was for Direct Streaming
