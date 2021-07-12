#!/bin/bash

echo $PWD
docker network rm node1
docker network create node1
IP=$(docker network inspect node1 | jq -r ".[].IPAM.Config[].Gateway")
HASH=$(./hash.sh)
IPHASH="- ip: $IP\n  hash: $HASH"
echo -e "$IPHASH" > NODE1IP
cd ..
docker build -t node1 -f ./node1/Dockerfile .
cd node1
