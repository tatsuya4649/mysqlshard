import hashlib
import sys
import test
import copy

def consistent_hasing(nodes,data):
	nodes_id = dict()
	for node in nodes:
		if type(node) is not str:
			raise TypeError
		nodes_id[node] = hashlib.md5(node.encode('utf-8')).hexdigest()
	# Hash data
	data_id = hashlib.md5(data.encode('utf-8')).hexdigest()
	sorted_nodes = sorted(nodes_id.items(),key=lambda x:x[1])
	for j in range(len(sorted_nodes)):
		if sorted_nodes[j][1] >= data_id:
			return sorted_nodes[j][0]
	return sorted_nodes[0][0]

def distributed(test,nodes_dict):
	average = float(test.datas_count)/float(test.nodes_count)
	total = 0.0
	for node in nodes_dict.keys():
		total += ((float(len(nodes_dict[node])) - average)) ** 2
	distributed = total/float(test.nodes_count)
	std = pow(distributed,0.5)
	print(f"Distributed: {distributed}")
	print(f"Standard Deviation: {std}")

if __name__ == "__main__":
	test = test.TestNode(node_count=100,data_count=10000,str_len=2)
	nodes_dict = dict()
	for node in test.nodes:
		nodes_dict[node] = list()
	print("=========== cons ===========")
	print(f"Nodes Count: {test.nodes_count}")
	for data in test.datas:
		node = consistent_hasing(test.nodes,data)
		nodes_dict[node].append(data)
	for node in nodes_dict.keys():
		print(f"{node} => {len(nodes_dict[node])}")
	cons_nodes_dict = copy.deepcopy(nodes_dict)
	distributed(test,nodes_dict)
	print("=========== add node ===========")
	test.append(count=1)
	print(f"Nodes Count: {test.nodes_count}")
	nodes_dict = dict()
	for node in test.nodes:
		nodes_dict[node] = list()
	for data in test.datas:
		node = consistent_hasing(test.nodes,data)
		nodes_dict[node].append(data)
	for node in nodes_dict.keys():
		print(f"{node} => {len(nodes_dict[node])}")
	add_nodes_dict = copy.deepcopy(nodes_dict)
	distributed(test,nodes_dict)
	print("=========== delete node ===========")
	test.remove(count=1)
	print(f"Nodes Count: {test.nodes_count}")
	nodes_dict = dict()
	for node in test.nodes:
		nodes_dict[node] = list()
	for data in test.datas:
		node = consistent_hasing(test.nodes,data)
		nodes_dict[node].append(data)
	for node in nodes_dict.keys():
		print(f"{node} => {len(nodes_dict[node])}")
	del_nodes_dict = copy.deepcopy(nodes_dict)
	distributed(test,nodes_dict)

	print("Changed: consistent vs add")
	count = 0
	for key in cons_nodes_dict.keys():
		if set(cons_nodes_dict[key]) != set(add_nodes_dict[key]):
			count += 1
			print(f"send to add: {set(cons_nodes_dict[key]) ^ set(add_nodes_dict[key])}")
	print(f"Changed count => {count}")

	print("Changed: add vs del")
	count = 0
	for key in del_nodes_dict.keys():
		if set(del_nodes_dict[key]) != set(add_nodes_dict[key]):
			count += 1
			print(f"send to anynode: {set(del_nodes_dict[key]) ^ set(add_nodes_dict[key])}")
	print(f"Changed count => {count}")
