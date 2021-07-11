import hashlib
import sys
import test
import copy

def hash(data):
	return hashlib.md5(data).hexdigest()

def virtual_hash(nodes_id,node,virtual):
	for i in range(1,virtual+1):
		nodes_id[hash(f"{node}_{i}".encode('utf-8'))] = node

# Receive IP Address of Nodes,Data(non Hash)
# Return IP Address to be saved
def consistent_hasing(nodes,data,virtual=100):
	nodes_id = dict()
	for node in nodes:
		if type(node) is not str:
			raise TypeError
		nodes_id[hash(node.encode('utf-8'))] = node
		virtual_hash(nodes_id,node,virtual)
	# Hash data
	data_id = hash(data.encode('utf-8'))
	sorted_nodes = sorted(nodes_id.items(),key=lambda x:x[0])
	for j in range(len(sorted_nodes)):
		if sorted_nodes[j][0] >= data_id:
			return sorted_nodes[j][1]
	return sorted_nodes[0][1]

def distributed(test,nodes_dict):
	average = float(test.datas_count)/float(test.nodes_count)
	total = 0.0
	for node in nodes_dict.keys():
		total += ((float(len(nodes_dict[node])) - average)) ** 2
	distributed = total/float(test.nodes_count)
	std = pow(distributed,0.5)
	print(f"Data Count: {test.datas_count}")
	print(f"Distributed: {distributed}")
	print(f"Standard Deviation: {std}")

if __name__ == "__main__":
	test = test.TestNode(node_count=10,data_count=1000,str_len=10)
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

	print("Consistency Test")
	print("Check the All Datas Set in All Nodes is same.")
	cons_set = set()
	add_set = set()
	del_set = set()
	for key in cons_nodes_dict.keys():
		cons_set |= set(cons_nodes_dict[key])
	for key in add_nodes_dict.keys():
		add_set |= set(add_nodes_dict[key])
	for key in del_nodes_dict.keys():
		del_set |= set(del_nodes_dict[key])
	if cons_set == add_set:
		print("Data:consistent == add")
	if cons_set == del_set:
		print("Data:consistent == del")
	if add_set == del_set:
		print("Data:del == add")
