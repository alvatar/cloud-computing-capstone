#!/usr/bin/env bash


CREDS=$HOME/.ssh/coursera.pem

for slave in $@; do
    ssh -i $CREDS hadoop@$slave 'sudo pip install cassandra-driver'
done
