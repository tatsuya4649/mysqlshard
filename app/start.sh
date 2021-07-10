#!/bin/bash

export NODEIP=$(cat ./NODE1IP)
export NODEPORT=$(cat ./NODE1PORT)
export NODEDATABASE=$(cat ./NODE1DATABASE)

python3 main.py
