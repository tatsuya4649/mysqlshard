#!/bin/bash

source ./var.sh

docker container rm -f $NODE
docker rmi -f $NODE
