#!/usr/bin/env bash

BROKER_LIST="localhost:9092"

pv ~/clean_origin_destination | $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $BROKER_LIST --topic origin-destination #--security-protocol PLAINTEXT
