#!/bin/bash

echo $(cat NODE1IP) | md5sum | cut -d' ' -f1
