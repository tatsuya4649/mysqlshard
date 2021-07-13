#!/bin/bash

source ./var.sh

docker run --name $NODE --env-file ../ENV -p $PORT:3306 -d $NODE
