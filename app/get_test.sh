#!/bin/bash

curl -X GET http://172.17.0.1:48080/user/hello -w '\n' -v
