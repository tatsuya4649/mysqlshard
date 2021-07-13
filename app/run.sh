#!/bin/bash

source ./var.sh

NODEIP=$(docker network inspect $NODE | jq -r ".[].IPAM.Config[].Gateway")
echo "NODEIP=$NODEIP" > ENV
echo "NODEPORT=$PORT" >> ENV
echo "NODEDATABASE=$DB" >> ENV

docker run -it --name shapp --env-file ./ENV -p 48080:8080 shapp
