#!/bin/bash

echo $(cat NODE2IP) | md5sum | cut -d' ' -f1
