cat tmp.csv | kafka-console-producer.sh --broker-list ip-172-30-0-239.ec2.internal:6667 --topic test2 --security-protocol PLAINTEXT
