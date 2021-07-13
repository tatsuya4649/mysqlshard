#!/bin/bash

if [ -z "$NODE" ];then
	echo "NODE var is empty..."
	exit 1
fi

if [ -z "$PORT" ]; then
	echo "PORT var is empty..."
	exit 1
fi
