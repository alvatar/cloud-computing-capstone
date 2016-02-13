#!/usr/bin/env bash

SCRIPT_DIR=python
THIS="$(basename $0)"
THIS=${THIS/.sh}
TOPIC=ontime

spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 \
  --master yarn \
  --deploy-mode client \
  --driver-memory 4g \
  --driver-cores 4 \
  --executor-memory 4g \
  --num-executors 15 \
  --executor-cores 2 \
  --conf spark.task.cpus=1 \
  --conf spark.default.parallelism=30 \
  --conf spark.streaming.backpressure.enabled=true \
  $SCRIPT_DIR/${THIS}.py ip-172-30-0-211.ec2.internal:2181 $TOPIC

  #--conf spark.yarn.executor.memoryOverhead=1000 \

  #--conf "spark.executor.extraJavaOptions=-XX:+UseCompressedOops" \

  #--conf spark.streaming.kafka.maxRate=120000 \
  #--conf spark.streaming.receiver.maxRate=120000 \


  #$SCRIPT_DIR/${THIS}.py ip-172-30-0-239.ec2.internal:2181 $TOPIC
  #$SCRIPT_DIR/${THIS}.py ip-172-30-0-47.ec2.internal:6667 $TOPIC

  # This was for Direct Streaming
