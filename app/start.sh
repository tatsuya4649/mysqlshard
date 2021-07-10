#!/bin/bash

export NODEIP=$(cat ./NODE1IP)
export NODEPORT=$(cat ./NODE1PORT)

python3 main.py
