#!/usr/bin/env bash

BROKER_LIST="localhost:9092"

pv ~/clean_on_time_2008 | $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $BROKER_LIST --topic 2008ontime #--security-protocol PLAINTEXT
