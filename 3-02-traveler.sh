#!/usr/bin/env bash

SCRIPT_DIR=python
THIS="$(basename $0)"
THIS=${THIS/.sh}
TOPIC=2008ontime
KAFKA_OR_ZOOKEEPER=ip-172-30-0-211.ec2.internal:2181

source cassandra.env

cat <<HERE | cqlsh
USE spark;
DROP TABLE trip_combinations;
CREATE TABLE trip_combinations ( origin_date timestamp,
                                 origin_date_time timestamp,
                                 origin_carrier text,
                                 origin_flight text,
                                 origin text,
                                 connection text,
                                 connection_arr_delay float,
                                 connection_date timestamp,
                                 connection_date_time timestamp,
                                 connection_carrier text,
                                 connection_flight text,
                                 destination text,
                                 destination_arr_delay float,
                                 PRIMARY KEY (origin, connection, destination, origin_date)
                                 );
HERE

#if [[ $? != 0 ]]; then exit; fi

echo "Cassandra configured"

spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0 \
  --master yarn \
  --deploy-mode client \
  --driver-memory 10g \
  --driver-cores 4 \
  --executor-memory 2g \
  --num-executors 32 \
  --executor-cores 2 \
  --conf spark.streaming.receiver.maxRate=8000 \
  --conf spark.streaming.kafka.maxRate=8000 \
  --conf spark.task.cpus=1 \
  --conf spark.shuffle.memoryFraction=0.5 \
  --conf spark.yarn.executor.memoryOverhead=1024 \
  --conf spark.default.parallelism=32 \
  --conf "spark.executor.extraJavaOptions=-XX:+UseCompressedOops" \
  $SCRIPT_DIR/${THIS}.py $KAFKA_OR_ZOOKEEPER $TOPIC $CASSANDRA

#--conf spark.streaming.backpressure.enabled=true \
