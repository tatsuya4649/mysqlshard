#!/bin/bash

docker run --name node3 --env-file ../ENV -p 13306:3306 -d node3
