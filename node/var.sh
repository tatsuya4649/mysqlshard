#!/bin/bash

if [ -z "$NODE" ];then
	echo "NODE var is empty..."
	exit 1
fi

if [ -z "$PORT" ]; then
	echo "PORT var is empty..."
	exit 1
fi

#if [ -z "$USER" ]; then
#	echo "USER var is empty..."
#	exit 1
#fi
#
#if [ -z "$PASSWORD" ]; then
#	echo "PASSWORD var is empty..."
#	exit 1
#fi
