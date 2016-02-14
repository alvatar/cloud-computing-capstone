#!/usr/bin/env bash

#source kafka-spark.env

#BROKER_LIST="ip-172-30-0-211:6667"
BROKER_LIST="localhost:9092"

#hadoop fs -cat 'clean_origin_destination/par*' | pv | bin/kafka-console-producer.sh --broker-list $BROKER_LIST --topic origin-destination #--security-protocol PLAINTEXT

pv ../clean_origin_destination | bin/kafka-console-producer.sh --broker-list $BROKER_LIST --topic origin-destination #--security-protocol PLAINTEXT


#for i in $(hadoop fs -find 'clean_origin_destination/par*'); do
	#echo $i
	#hadoop fs -cat $i | pv |  kafka-console-producer.sh --broker-list $BROKER_LIST --topic origin-destination --security-protocol PLAINTEXT
	#sleep 1
#done
