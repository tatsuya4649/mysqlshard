import argparse
import parse
import re
import sys
import copy
import pymysql
from algo import con

class AddNode:
	def __init__(self,ip,yaml_path="ip.yaml",virtual_count=100):
		if not self.ip_check(ip):
			print(f"not IP Address {args.ip}",file=sys.stderr)
			raise Exception
		try:
			self._exists_iphashs = self._parse_yaml(yaml_path)
		except FileNotFoundError as e:
			raise FileNotFoundError
		print(f"Add Nodes:\n{ip}")
		print(f"Exists Nodes:\n{self._exists_iphashs}")
		print("================================")
		add_node_ip = ip
		add_node_hash = con.hash(add_node_ip.encode("utf-8"))
		self._add_node_dict = dict()
		self._add_node_dict["ip"] = add_node_ip
		self._add_node_dict["hash"] = add_node_hash
		new_iphashs = copy.deepcopy(self._exists_iphashs)
		new_iphashs.append(self._add_node_dict)
		self._new_iphashs = parse.sort(new_iphashs)

		self._add_node_index = new_iphashs.index(self._add_node_dict)
		self._previous_dict = None
		self._next_dict = None
		# Get Previous IP of add_node_ip From HashID
		if self._add_node_index == 0:
			self._previous_dict = None
			print("previous_dict is None...")
		else:
			self._previous_dict = dict()
			self._previous_dict = new_iphashs[self._add_node_index-1]
			self._previous_ip = self._previous_dict["ip"]
			self._previous_hash = self._previous_dict["hash"]
			print(f"Previous IP => {self._previous_ip}")
			print(f"Previous Hash => {self._previous_hash}")

		# Get Next IP of add_node_ip From HashID
		if self._add_node_index == len(new_iphashs)-1:
			self._next_dict = None
			print("next_dict is None...")
		else:
			self._next_dict = new_iphashs[self._add_node_index+1]
			self._next_ip = self._next_dict["ip"]
			self._next_hash = self_next_dict["hash"]
			print(f"Next IP => {self._next_ip}")
			print(f"Next Hash => {self._next_hash}")
		
		self._conn = None
		self._steal_data = None
		self._virtual_node(virtual_count)

	@classmethod	
	def ip_check(self,ip):
		if re.match(r'^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$',ip):
			return True
		else:
			return False
	def _parse_yaml(self,path):
		return parse.parse_yaml(path)

	# count: total node count(real node + virtual node)
	def _virtual_node(self,count):
		total = len(self._new_iphashs)
		virtual_iphashs = copy.deepcopy(self._new_iphashs)
		while total < count:
			for obj in self._new_iphashs:
				ip = obj["ip"]
				virtual_ip = f"{ip}{total}"
				virtual_hash = con.hash(virtual_ip.encode("utf-8"))
				virtual_dict = dict()
				virtual_dict["ip"] = ip
				virtual_dict["hash"] = virtual_hash
				virtual_iphashs.append(virtual_dict)
				total += 1
		self._virtual_iphashs = virtual_iphashs
	
	# return array of dict(All IP and virtual hashs list)
	def _virtual_data(self):
		if self._virtual_iphashs is None:
			raise ValueError
		virtuals = list()
		for obj in self._new_iphashs:
			virtual_dict = dict()
			ip = obj["ip"]
			virtual_dict["ip"] = ip
			virtual_dict["hashs"] = list()
			for vobj in self._virtual_iphashs:
				if ip == vobj["ip"]:
					virtual_dict["hashs"].append(vobj["hash"])
			virtuals.append(virtual_dict)
		# Order by hashID
		for i in range(len(virtuals)):
			virtuals[i]["hashs"]  = sorted(virtuals[i]["hashs"])
		return virtuals

	@property
	def add_ip(self):
		return self._add_node_dict["ip"]
	@property
	def add_hash(self):
		return self._add_node_dict["hash"]
	@property
	def add_index(self):
		return self._add_node_index
	@property
	def pre_ip(self):
		if self._previous_dict is not None:
			return self._previous_dict["ip"]
		else:
			return None
	@property
	def pre_hash(self):
		if self._previous_dict is not None:
			return self._previous_dict["hash"]
		else:
			return None
	@property
	def pre_index(self):
		if self._add_node_index==0:
			return None
		else:
			return self._add_node_index-1
	@property
	def next_ip(self):
		if self._next_dict is not None:	
			return self._next_dict["ip"]
		else:
			return None
	@property
	def next_hash(self):
		if self._next_dict is not None:	
			return self._next_dict["hash"]
		else:
			return None
	@property
	def next_index(self):
		if self._add_node_index==len(self._new_iphashs)-1:
			return None
		else:
			return self._add_node_index+1
	
	# Get Previous HashID
	@property
	def move_data_hashid_small(self):
		return self.pre_hash
	# Get Add HashID
	@property
	def move_data_hashid_big(self):
		return self.add_hash

	@property
	def steal_ip(self):
		return self.next_ip
	# Steal Data From Next
	def steal_data(
		self,
		database,
		table,
		hashcolmn,
		port=3306,
		user='root',
		password='mysql',
	):
		# Steal Next Host
		host = self.next_ip
		self._conn = pymysql.connect(
			host=host,
			port=port,
			user=user,
			password=password,
			database=database,
			cursor=pymysql.cursors.DictCursor
		)
		try:
			with self._conn.cursor() as cursor:
				sql = f"SELECT * FROM {table} WHERE {hashcolmn} > {self.move_data_hashid_small} AND {hashcolumn} <= {self.move_data_hashid_big}"
				print(f"{sql}")
				cursor.execute(sql)
				results = cursor.fetchall()
				self._steal_data = results
		except Exception as e:
			self._steal_data = None
			pass
		finally:
			self._conn = None
	@property
	def delete_ip(self):
		return self.next_ip
	# Delete Steal Data
	def delete_data(
		self,
		database,
		table,
		scheme,
		port=3306,
		user='root',
		password='mysql',
	):
		# Delete Next Host
		host = self.next_ip
		self._conn = pymysql.connect(
			host=host,
			port=port,
			user=user,
			password=password,
			database=database,
			cursor=pymysql.cursors.DictCursor
		)
		try:
			with self._conn.cursor() as cursor:
				sql = f"DELETE FROM {table} WHERE {hashcolmn} > {self.move_data_hashid_small} AND {hashcolumn} <= {self.move_data_hashid_big}"
				print(f"{sql}")
				self._conn.begin()
				cursor.execute(sql)
				self._conn.commit()
		except Exception as e:
			print(e)
			pass
		finally:
			self._conn = None

	@property
	def insert_ip(self):
		return self.add_ip
	# Insert New Data
	def insert_data(
		self,
		database,
		table,
		scheme,
		port=3306,
		user='root',
		password='mysql',
	):
		# Delete Add Host
		host = self.add_ip
		self._conn = pymysql.connect(
			host=host,
			port=port,
			user=user,
			password=password,
			database=database,
			cursor=pymysql.cursors.DictCursor
		)
		for insert_data in self._steal_data:
			try:
				with self._conn.cursor() as cursor:
					sql = f"INSERT INTO {scheme} VALUES ({insert_data})"
					self._conn.begin()
					curosr.execute(sql)
					self._conn.commit()
			except Exception as e:
				pass
		self._conn = None


	

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Add node from IP Address")
	parser.add_argument("ip",help="IP Address of Node")
	parser.add_argument("yaml_path",help="Exists IP Addresses file path")

	args = parser.parse_args()

	addnode = AddNode(args.ip,args.yaml_path)
	print("-----add")
	print(addnode.add_ip)
	print(addnode.add_hash)
	print(addnode.add_index)
	print("-----prev")
	print(addnode.pre_ip)
	print(addnode.pre_hash)
	print(addnode.pre_index)
	print("-----next")
	print(addnode.next_ip)
	print(addnode.next_hash)
	print(addnode.next_index)
	print("-----move data hash")
	print(f"{addnode.move_data_hashid_small}~{addnode.move_data_hashid_big}")
	print(f"steal IP: {addnode.steal_ip}")
	print(f"delete IP: {addnode.delete_ip}")
	print(f"insert IP: {addnode.insert_ip}")
	print(f"add IP: {addnode.add_ip}")
	virtuals = addnode._virtual_data()
	steels = list()
	
	add_virtual = None
	virtual_index = dict()
	for virtual in virtuals:
		virtual_index[virtual["ip"]] = 0
	for virtual in virtuals:
		if virtual["ip"] == addnode.add_ip:
			add_virtual = virtual
	del virtual_index[addnode.add_ip]

	nonaddvirs = copy.deepcopy(virtuals)
	for j in range(len(nonaddvirs)):
		if nonaddvirs[j]["ip"] == addnode.add_ip:
			nonaddvirs.pop(j)
			break
	
	count = 0
	query = ""
	for j in range(len(add_virtual["hashs"])):
		hash = add_virtual["hashs"][j]
		if j+1 == len(add_virtual["hashs"]):
			break
		else:
			smallest_hash = add_virtual["hashs"][j+1]
		smallest_ip = None
		for virtual in nonaddvirs:
			ip = virtual["ip"]
			if hash < virtual["hashs"][virtual_index[ip]]:
				if smallest_hash > virtual["hashs"][virtual_index[ip]]:
					smallest_hash = virtual["hashs"][virtual_index[ip]]
					smallest_ip = ip
				else:
					virtual_index[ip]+=1
			else:
				virtual_index[ip]+=1
		if hash != smallest_hash:
			count += 1
			print("-----------")
			print(hash)
			print(smallest_hash)
			query += f" {hash} <= {smallest_hash} OR"
	query = query.rstrip("OR")
	print(count)
	print(query)
