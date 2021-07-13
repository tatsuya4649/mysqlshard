#!/bin/bash

source ./var.sh

cd ..
docker build -t shapp -f app/Dockerfile .
cd app
