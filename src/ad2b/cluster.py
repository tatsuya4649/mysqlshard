#!/bin/env python3

from cluster import *
import sys

cluinfo,ops = yaml_to_ops("./ops.yaml")
print(ops)

print("=============== Operate Cluster ==============")

#operations = MySQLOperation(
#		ip="127.0.0.1",
#		port=3306,
#		mode=NodeMode.ADD
#		)
cluster = MySQLCluster(cluinfo,ops)
cluster.operate()
