#!/bin/bash

echo $(cat NODE3IP) | md5sum | cut -d' ' -f1
