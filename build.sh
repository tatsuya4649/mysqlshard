#!/bin/bash

docker build -t shmysql .
docker network create shmysql
