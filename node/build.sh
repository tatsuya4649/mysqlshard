#!/bin/bash

source ./var.sh

echo $PWD
docker network rm $NODE
docker network create $NODE
IP=$(docker network inspect $NODE | jq -r ".[].IPAM.Config[].Gateway")
HASH=$(./hash.sh)
IPHASH="- ip: $IP\n  hash: $HASH"
echo -e "$IPHASH" > ${NODE^^}IP
cd ..
docker build -t $NODE -f ./node/Dockerfile .
cd node
