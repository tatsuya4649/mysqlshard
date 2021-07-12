#!/bin/bash

docker network rm node2
docker network create node2
IP=$(docker network inspect node2 | jq -r ".[].IPAM.Config[].Gateway") 
HASH=$(./hash.sh)
IPHASH="- ip: $IP\n  hash: $HASH"
echo -e "$IPHASH" > NODE2IP
cd ..
docker build -t node2 -f ./node2/Dockerfile .
cd node2
