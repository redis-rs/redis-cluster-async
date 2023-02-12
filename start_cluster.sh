#!/bin/sh

docker run \
    --rm \
    --init \
    --tty \
    --name redis-cluster \
    -d \
    -e "IP=127.0.0.1" \
    -p 7000:7000 -p 7001:7001 -p 7002:7002 -p 7003:7003 -p 7004:7004 -p 7005:7005 \
    grokzen/redis-cluster:6.0.0;

sleep 10;

redis-cli -h 127.0.0.1 -p 7000 -c ACL SETUSER client-user allkeys +@all ON \>redis-password;
redis-cli -h 127.0.0.1 -p 7001 -c ACL SETUSER client-user allkeys +@all ON \>redis-password;
redis-cli -h 127.0.0.1 -p 7002 -c ACL SETUSER client-user allkeys +@all ON \>redis-password;
redis-cli -h 127.0.0.1 -p 7003 -c ACL SETUSER client-user allkeys +@all ON \>redis-password;
redis-cli -h 127.0.0.1 -p 7004 -c ACL SETUSER client-user allkeys +@all ON \>redis-password;
redis-cli -h 127.0.0.1 -p 7005 -c ACL SETUSER client-user allkeys +@all ON \>redis-password;
