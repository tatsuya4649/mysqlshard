#!/bin/bash

source ./var.sh

echo $PWD
docker network rm $NODE
docker network create $NODE
IP=$(docker network inspect $NODE | jq -r ".[].IPAM.Config[].Gateway")
HASH=$(./hash.sh)
IPYAML="- ip: $IP\n"
PORTYAML="  port: $PORT\n"
HASHYAML="  hash:\n  - $HASH\n"
IPHASH="$IPYAML$HASHYAML$PORTYAML"
echo -e "$IPHASH" > ${NODE^^}IP
echo -en "$IPYAML" > ${NODE^^}OPS
echo -en "$PORTYAML" >> ${NODE^^}OPS
cd ..
docker build -t $NODE -f ./node/Dockerfile .
cd node
