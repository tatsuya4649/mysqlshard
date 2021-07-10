#!/bin/bash

docker run --name node1 --env-file ../ENV -v $PWD/data:/var/lib/mysql -p 13306:3306 -d shmysql
