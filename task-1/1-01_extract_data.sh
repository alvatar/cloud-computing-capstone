#!/bin/sh

sudo mount /dev/xvdf /mnt/

mkdir /tmp/data

echo "\nDecompressing ON TIME data files and copying to HDFS...\n"

hadoop fs -mkdir /user/ubuntu/sources_on_time
cd /mnt/airline_ontime
for i in `find . -iname "*.zip"`; do
    unzip -o $i -d /tmp/data/
    hadoop fs -put /tmp/data/*.csv /user/ubuntu/sources_on_time
    rm /tmp/data/*
done

echo "\nDecompressing ORIGIN/DESTINATION data files and copying to HDFS...\n"

hadoop fs -mkdir /user/ubuntu/sources_origin_destination
cd /mnt/airline_origin_destination
for i in `find . -iname "*.zip"`; do
    unzip -o $i -d /tmp/data/
    hadoop fs -put /tmp/data/*.csv /user/ubuntu/sources_origin_destination
    rm /tmp/data/*
done

rm -Rf /tmp/data
