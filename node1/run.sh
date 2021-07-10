#!/bin/bash

docker run --name node1 --env-file ../ENV -p 13306:3306 -d shmysql
