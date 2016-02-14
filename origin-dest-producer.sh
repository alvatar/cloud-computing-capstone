#!/usr/bin/env bash

source kafka-spark.env

BROKER_LIST="ip-172-30-0-47:6667"

for i in $(hadoop fs -find 'clean_origin_destination/par*'); do
    echo $i
    hadoop fs -cat $i | kafka-console-producer.sh --broker-list $BROKER_LIST --topic origin-destination --security-protocol PLAINTEXT
    sleep 1
done
