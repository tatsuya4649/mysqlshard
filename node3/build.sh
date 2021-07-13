#!/bin/bash

echo $PWD
docker network rm node3
docker network create node3
IP=$(docker network inspect node3 | jq -r ".[].IPAM.Config[].Gateway")
HASH=$(./hash.sh)
IPHASH="- ip: $IP\n  hash: $HASH"
echo -e "$IPHASH" > NODE1IP
cd ..
docker build -t node3 -f ./node3/Dockerfile .
cd node3
