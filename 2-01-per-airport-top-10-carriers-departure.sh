#!/usr/bin/env bash

SCRIPT_DIR=python
THIS="$(basename $0)"
THIS=${THIS/.sh}
TOPIC=ontime
KAFKA_OR_ZOOKEEPER=ip-172-30-0-211.ec2.internal:2181

source cassandra.env

cat <<HERE | cqlsh
USE spark;
DROP TABLE top_carriers;
CREATE TABLE top_carriers (
airport text,
c01 text,
c02 text,
c03 text,
c04 text,
c05 text,
c06 text,
c07 text,
c08 text,
c09 text,
c10 text,
PRIMARY KEY (airport),
);
HERE

if [[ $? != 0 ]]; then exit; fi

echo "Cassandra configured"

SCRIPT_DIR=python
THIS="$(basename $0)"
THIS=${THIS/.sh}
TOPIC=ontime
KAFKA_OR_ZOOKEEPER=ip-172-30-0-211.ec2.internal:2181
spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 \
  --master yarn \
  --deploy-mode client \
  --driver-memory 4g \
  --driver-cores 4 \
  --executor-memory 4g \
  --num-executors 15 \
  --executor-cores 2 \
  --conf spark.task.cpus=1 \
  --conf spark.yarn.executor.memoryOverhead=1000 \
  --conf spark.default.parallelism=42 \
  --conf "spark.executor.extraJavaOptions=-XX:+UseCompressedOops" \
  --conf spark.streaming.backpressure.enabled=true \
  $SCRIPT_DIR/${THIS}.py $KAFKA_OR_ZOOKEEPER $TOPIC $CASSANDRA

#--conf spark.yarn.executor.memoryOverhead=1000 \
#--conf "spark.executor.extraJavaOptions=-XX:+UseCompressedOops" \
#--conf spark.streaming.kafka.maxRate=120000 \
#--conf spark.streaming.receiver.maxRate=120000 \
