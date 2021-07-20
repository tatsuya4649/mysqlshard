import yaml
import sys
from algo import con

def parse_yaml(path):
	with open(path,"r") as yf:
		obj = yaml.safe_load(yf)
	if "cluster" not in obj.keys():
		raise KeyError("Cluster YAML File must have \"cluster\" keyword.")
	# {'172.25.0.1': 'a0dffb129fffa47f1dd582d481c28717', '172.24.0.1': 'a799bfa76e49bb0e43190454ff941710'}
	cluster = obj["cluster"]
	# have one or more node in cluster
	if cluster is not None:
		# Sort by NodeID
		lists = sorted(cluster,key=lambda x:x["hash"])
	# initial node of cluster
	else:
		lists = list()
	return lists

#def sort(lists):
#	return sorted(lists,key=lambda x:x["hash"])

def update_yaml(path,lists):
	newips = dict()
	newips["cluster"] = lists
	with open(path,"w") as yf:
		yaml.dump(newips,yf,default_flow_style=False)