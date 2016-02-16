#!/usr/bin/env bash


CREDS=$HOME/.ssh/coursera.pem

for slave in $@; do
    ssh -o "StrictHostKeyChecking no" -i $CREDS hadoop@$slave 'sudo pip install cassandra-driver twisted eventlet gevent'
done
