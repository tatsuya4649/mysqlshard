#!/bin/bash

docker run --name node2 --env-file ../ENV -v $PWD/data:/var/lib/mysql -p 23306:3306 -d shmysql
