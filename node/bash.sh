#!/bin/bash

if [ -z "$NODE" ]; then
	echo "NODE var is empty"
	exit 1
fi
docker exec -it $NODE /bin/bash
