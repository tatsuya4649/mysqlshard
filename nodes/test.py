import random
import string

class TestNode:
	def __init__(self,node_count=10,data_count=100,str_len=10):
		self._node_count = node_count
		self._data_count = data_count
		self._nodes = list()
		self._datas = list()
		for _ in range(self._node_count):
			self._nodes.append(self.random_ip())
		for _ in range(self._data_count):
			random_str = ""
			for _ in range(str_len):
				random_str += random.choice(string.ascii_letters + string.digits)
			self._datas.append(random_str)

	@classmethod	
	def random_ip(self):
		ip = ""
		for i in range(4):
			dot = "." if i != 3 else ""
			ip += f"{random.randint(0,255)}{dot}"
		return ip

	@property
	def nodes(self):
		return self._nodes
	@property
	def nodes_count(self):
		return len(self._nodes)
	@property
	def datas(self):
		return self._datas
	@property
	def datas_count(self):
		return len(self._datas)

	def now_nodes(self):
		print(f"Nodes => {self.nodes}")
	def now_datas(self):
		print(f"Datas => {self.datas}")

	# insert new IP Address
	def append(self,count=1):
		for _ in range(count):
			self._nodes.append(self.random_ip())
	# remove IP Address
	def remove(self,count=1):
		for _ in range(count):
			index = random.randint(0,len(self._nodes)-1)
			self._nodes.pop(index)
	
	# delete IP Address with Index
	def delete(self,index=None):
		if index is None:
			index = random.randint(0,len(self._nodes)-1)
		self._nodes.pop(index)

	# function (nodes,data)
	def _test(self,func,nodes=None,datas=None):
		nodes_datas = dict()
		for node in nodes:
			nodes_datas[node] = list()
		if nodes is None:
			nodes = self.nodes
		if datas is None:
			datas = self.datas
		for data in datas:
			res_node = func(nodes,data)
			nodes_datas[res_node].append(data)
		return nodes_datas
	def test(self,func,nodes=None,datas=None):
		nodes_datas = _test(func,nodes,datas)
		for key in nodes_datas.keys():
			print(f"{key}: {nodes_datas[key]}")
	def test_count(self,func,nodes=None,datas=None):
		nodes_datas = _test(func,nodes,datas)
		for key in nodes_datas.keys():
			print(f"{key}: {len(nodes_datas[key])}")
		

if __name__ == "__main__":
	test = TestNode()
	print(" ======== test node ========")
	print(test.nodes)
	print(" ======== test node count =========")
	print(test.nodes_count)
	print(" ======== test data ========")
	print(test.datas)
	print(" ======== test data count =========")
	print(test.datas_count)
