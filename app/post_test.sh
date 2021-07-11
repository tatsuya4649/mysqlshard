#!/bin/bash

curl -X POST http://172.17.0.1:48080/user -F "username=hello" -F "comment=there" -w '\n'
