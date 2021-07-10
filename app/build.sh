#!/bin/bash


NODE1IP=$(docker network inspect node1 | jq -r ".[].IPAM.Config[].Gateway")

echo "$NODE1IP" > NODE1IP

cd ..
docker build -t shapp -f app/Dockerfile .
cd app
