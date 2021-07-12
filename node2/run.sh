#!/bin/bash

docker run --name node2 --env-file ../ENV -p 23306:3306 -d node2
