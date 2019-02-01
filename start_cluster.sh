#!/bin/sh

docker run \
    --rm \
    --name redis-cluster \
    -e "IP=127.0.0.1" \
    -p 7000:7000 -p 7001:7001 -p 7002:7002 \
    grokzen/redis-cluster:latest
