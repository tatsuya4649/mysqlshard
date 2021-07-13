#!/bin/bash

source ./var.sh

echo $(cat ${NODE^^}IP) | md5sum | cut -d' ' -f1
