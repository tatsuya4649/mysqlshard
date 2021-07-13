import argparse
import parse
import re
import sys
import copy
import pymysql
import test
import choice
import notice
import columns as clms
from algo import con
import time

class MySQLAddNode(test.MySQLConsistency):
	def __init__(self,ip,hash_column,database,table,columns,yaml_path="ip.yaml",virtual_count=100,_DEBUG=False,funcpath=None,notice_args=[],notice_kwargs={}):
		super().__init__(ip,database,table)
		if not isinstance(columns,clms.Columns):
			raise TypeError("column argument's type  must be Columns")
		if funcpath is not None:
			self._notice = notice.Notice(funcpath,*notice_args,**notice_kwargs)
		else:
			self._notice = None
		self._notice_args = notice_args
		self._notice_kwargs = notice_kwargs
		self._columns = columns
		self._database = database
		self._table = table
		if not self.ip_check(ip):
			print(f"not IP Address {args.ip}",file=sys.stderr)
			raise Exception
		self._yaml_path = yaml_path
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
			self._next_hash = self._next_dict["hash"]
			print(f"Next IP => {self._next_ip}")
			print(f"Next Hash => {self._next_hash}") 
		self._conn = None
		self._steal_data = None
		self._virtual_node(virtual_count)
		self._hash_column = hash_column
		self._DEBUG = _DEBUG
		self._steal_ip = set()
		self._steal_query = dict()
		self._insert_ip = set()
		self._delete_ip = set()

		self._ip_query = self._get_steal_query()

	@property
	def exists_iphashs(self):
		return self._exists_iphashs

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
			raise ValueError("must have _virtual_iphashs. hints: call _virtual_node function.")
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

	"""
		add_virtual => Added virtual node dict("ip","hashs")
		virtual_index => Index dict("ip",index) for getting the index with the smallest hash value greater than the hash value of add_virtual
		virtualb_index => Index dict("ip",index) for getting the index with the biggest hash value smaller than the hash value of add_virtual
		nonaddvirs => virtuals excluding add_virtual
	"""
	def _check_change(self,booldict):
		for key in booldict.keys():
			if booldict[key]:
				return True
		return False

	def _debug_red(self,str):
		if str is None:
			return None
		return '\033[31m' + str + '\033[0m'
	def _debug_blue(self,str):
		if str is None:
			return None
		return '\033[34m' + str + '\033[0m'
	def _debug_green(self,str):
		if str is None:
			return None
		return '\033[32m' + str + '\033[0m'

	def _debug_steal_query(self,hdict):
		print(f"{self._debug_red('========================== DEBUG =========================')}")
		print(f"biggest change: {hdict['bchange']}")
		print(f"smallest change: {hdict['schange']}")
		addv = hdict["add"]
		for i in range(len(addv["hashs"])):
			if addv["hashs"][i] == hdict["hash"]:
				print(f"{self._debug_red(addv['hashs'][i])}",end=",")
			else:
				print(f"{addv['hashs'][i]}",end=",")
		print("")
		if "bindex" in hdict.keys():
			print(f"virtualb index => {self._debug_blue(str(hdict['bindex']))}")
			print(f"{self._debug_blue(hdict['vir']['hashs'][hdict['bindex']])}")
		if "index" in hdict.keys():
			print(f"virtual index => {self._debug_green(str(hdict['index']))}")
			print(f"{self._debug_green(hdict['vir']['hashs'][hdict['index']])}")
		print(f"{self._debug_red('==========================================================')}")
	def _hash_smallest(self,hashs,smallest,threshold):
		results = list()
		for h in hashs:
			if smallest is None:
				if threshold < h:
					results.append(h)
			else:
				if h < smallest and threshold < h:
					results.append(h)
		if len(results) == 0:
			return None
		else:
			return min(results)
	def _hash_biggest(self,hashs,biggest,threshold):
		results = list()
		for h in hashs:
			if biggest is None:
				if threshold > h:
					results.append(h)
			else:
				if h > biggest and threshold > h:
					results.append(h)
		if len(results) == 0:
			return None
		else:
			return max(results)

	def virtualhash(self,add_virtual,nonaddvirs):
		rem_lists = list()
		# res_dict["ip"],res_dict["query"]
		res_dict = dict()
		for j in range(len(add_virtual["hashs"])):
			# now hash value
			addhash = add_virtual["hashs"][j]
			# if last hash in added virtual
			if j+1 == len(add_virtual["hashs"]):
				smallest_hash = None
			else:
				smallest_hash = add_virtual["hashs"][j+1]
			if j == 0:
				biggest_hash = None
			else:
				biggest_hash = add_virtual["hashs"][j-1]
			# have smallest ip larger than hash(added node virtual hash)
			smallest_ip = None
			# have biggest ip smaller than hash(added node virtual hash)
			biggest_ip = None

			# loop of non adding node
			for nonadd in nonaddvirs:
				res = self._hash_smallest(nonadd["hashs"],smallest_hash,addhash)
				if res is not None:
					if smallest_hash is None or smallest_hash > res:
						smallest_hash = res
						smallest_ip = nonadd["ip"]
				res = self._hash_biggest(nonadd["hashs"],biggest_hash,addhash)
				if res is not None:
					if biggest_hash is None or res > biggest_hash:
						biggest_hash = res
						biggest_ip = nonadd["ip"]
			if smallest_ip is None or smallest_ip == self.add_ip:
				rem_lists.append(j)
				continue
			if smallest_ip not in res_dict.keys():
				res_dict[smallest_ip] = list()
			more = ""
			if biggest_hash is not None:
				more = f"{self._hash_column} > \"{biggest_hash}\""
			less = f"\"{smallest_hash}\" > {self._hash_column}"
			query = f" {more} AND {less} "
			res_dict[smallest_ip].append(query)
			if self._DEBUG:
				from_str = ""
				to_str = ""
				if biggest_hash is not None:
					from_str = f"from {self._debug_red(biggest_hash)} "
				if smallest_hash is not None:
					to_str = f" to {self._debug_red(smallest_hash)}"
				print(f"{from_str}{to_str}")
		return rem_lists,res_dict

	# Get dict (key IP Address,value Query)
	def _get_steal_query(self):
		virtuals = self._virtual_data()
		
		add_virtual = None
		
		# initial index => 0
		virtual_index = dict()
		virtualb_index = dict()
		for virtual in virtuals:
			virtual_index[virtual["ip"]] = 0
			virtualb_index[virtual["ip"]] = 0
		# delete addnode index
		for virtual in virtuals:
			if virtual["ip"] == self.add_ip:
				add_virtual = virtual
		del virtual_index[self.add_ip]
		del virtualb_index[self.add_ip]

		nonaddvirs = copy.deepcopy(virtuals)
		for j in range(len(nonaddvirs)):
			if nonaddvirs[j]["ip"] == self.add_ip:
				nonaddvirs.pop(j)
				break

		# raise Exception
		if add_virtual is None:
			raise ValueError("not found added node data from addnode._virtual_data")
		if len(virtual_index.keys()) == 0:
			raise ValueError("virtual_index must have one or more elements")
		if len(virtualb_index.keys()) == 0:
			raise ValueError("virtualb_index must have one or more elements")
		if len(nonaddvirs) == 0:
			raise ValueError("nonaddvirs must have one or more elements")
		if self._hash_column is None:
			raise ValueError("not found hash_column used in query")

		# query count
		scount = 0
		bcount = 0
		# total query
		query = ""
		hashcolumn = self._hash_column
		# per add_virtual hash value
		rem_lists = self.virtualhash(add_virtual,nonaddvirs)
		while len(rem_lists) != 0:
			rem_lists,query_dict = self.virtualhash(add_virtual,nonaddvirs)
			rem_lists.reverse()
			for i in rem_lists:
				add_virtual["hashs"].pop(i)
		if not 'query_dict' in locals():
			raise AttributeError("not found query_dict")

		ip_query = dict()
		for ip in query_dict.keys():
			query = ""
			for i in range(len(query_dict[ip])):
				query += query_dict[ip][i]
				if i < len(query_dict[ip])-1:
					query += "OR"
			ip_query[ip] = query
		return ip_query

	@property
	def steal_ip(self):
		return list(self._ip_query.keys())
	def _steal_dataset_init(self):
		if len(self._steal_data) != 0:
			for column in self._steal_data[0]:
				self._steal_dataset[column] = set()
	def _steal_dataset_set(self):
		if len(self._steal_data) != 0:
			for data in self._steal_data:
				for column in data.keys():
					self._steal_dataset[column].add(data[column])
	# Steal Data From Next
	def steal_data(
		self,
		database,
		table,
		port=3306,
		user='root',
		password='mysql',
	):
		for ip in self.steal_ip:
			host = ip
			query = self._ip_query[ip]
			#print("----------------------------------------")
			#print(f"Select: From Host \"{host}\", Port \"{port}\",Database \"{database}\", Table \"{table}\", Execute Query \"{query}\"")
			# Steal Next Host
			host = ip
			self._conn = pymysql.connect(
				host=host,
				port=port,
				user=user,
				password=password,
				database=database,
				cursorclass=pymysql.cursors.DictCursor
			)
			try:
				with self._conn.cursor() as cursor:
					sql = f"SELECT * FROM {table} WHERE {query}"
					#print(f"{sql}")
					cursor.execute(sql)
					results = cursor.fetchall()
					self._steal_data = results
			except Exception as e:
				self._steal_data = None
			finally:
				self._conn = None
		self.steal()
		self._steal_dataset_init()
		self._steal_dataset_set()
			
		return self._steal_data
	@property
	def delete_ip(self):
		return list(self._ip_query.keys())
	def _delete_dataset_init(self):
		if len(self._steal_data) != 0:
			for column in self._steal_data[0]:
				self._delete_dataset[column] = set()
	# Delete Steal Data
	def delete_data(
		self,
		port=3306,
		user='root',
		password='mysql',
		wait_printtime=10,
	):
		res_list = list()
		self._delete_dataset_init()
		for ip in self.steal_ip:
			self._conn = pymysql.connect(
				host=ip,
				port=port,
				user=user,
				password=password,
				db=self._database,
				cursorclass=pymysql.cursors.DictCursor
			)
			query = self._ip_query[ip]
			try:
				with self._conn.cursor() as cursor:
					sql = f"DELETE FROM {self._table} WHERE {query}"
					print(f"{sql}")
					self._conn.begin()
					res = cursor.execute(sql)
					self._conn.commit()
					res_list.append(res)
			except Exception as e:
				print(e)
				time.sleep(wait_printtime)
			finally:
				self._conn = None
		self._delete_res = res_list
		self.delete()
		return res_list
	@property
	def insert_ip(self):
		return self.add_ip
	def contest_delete(self):
		try:
			self._contest_delete()
		except test.ConsistencyDeleteError as e:
			print(e)
		except test.ConsistencyUnmatchError as e:
			print(e)
			
	def _contest_delete(self):
		if self.steal_len != self.delete_len:
			raise test.ConsistencyDeleteError("Detect Insert Error",(self.delete_len - self.steal_len))
	def contest_insert(self):
		try:
			self._contest_insert()
		except test.ConsistencyInsertError as e:
			if choice.insert_retry(self.insert_ip,e.err_count):
				pass
			return False
		except test.ConsistencyUnmatchError as e:
			if choice.insert_redo():
				self.insert_redo()
			return False
		else:
			return True
	def _contest_insert(self):
		if self.steal_len != self.insert_len:
			raise test.ConsistencyInsertError("Detect Insert Error",len([x for x in self._insert_res if x == 0]))
			
		for column in self._steal_dataset.keys():
			if self._insert_dataset[column] != self._steal_dataset[column]:
				raise test.ConsistencyUnmatchError("insert dataset unmatch to steal dataset")
	def insert_redo(
		self,
		user='root',
		password='mysql',
	):
		self._conn = pymysql.connect(
			host=self.insert_ip,
			port=self._insert_port,
			user=user,
			password=password,
			db=self._database,
			cursorclass=pymysql.cursors.DictCursor
		)
		res_list = list()
		for ip in self.steal_ip:
			query = self._ip_query[ip]
			try:
				with self._conn.cursor() as cursor:
					sql = f"DELETE FROM {self._table} WHERE {query}"
					print(f"{sql}")
					self._conn.begin()
					res = cursor.execute(sql)
					res_list.append(res)
					self._conn.commit()
			except Exception as e:
				print(e)
				pass
			finally:
				self._conn = None
	def _insert_dataset_init(self):
		if len(self._steal_data) != 0:
			for column in self._steal_data[0]:
				self._insert_dataset[column] = set()
		
	# Insert New Data
	def insert_data(
		self,
		port=3306,
		user='root',
		password='mysql',
		wait_printtime=10, # waiting for confirming Error 
	):
		# Delete Add Host
		host = self.add_ip
		self._conn = pymysql.connect(
			host=host,
			port=port,
			user=user,
			password=password,
			database=self._database,
			cursorclass=pymysql.cursors.DictCursor
		)
		self._insert_port = port
		res_list = list()
		self._insert_dataset_init()
		for insert_data in self._steal_data:
			try:
				with self._conn.cursor() as cursor:
					sql = f"INSERT INTO {self._table} {str(self._columns)} VALUES {self._columns.convert(insert_data)}"
					print(sql)
					self._conn.begin()
					res = cursor.execute(sql)
					self._conn.commit()
					for column in insert_data.keys():
						self._insert_dataset[column].add(insert_data[column])
					res_list.append(res)
			except Exception as e:
				res_list.append(0)
				print(e)
				time.sleep(wait_printtime)
		self._conn = None
		self._insert_res = res_list

		print("============== Insert Dataset Counter =================")
		print(len(self._insert_res))
		for key in self._insert_dataset.keys():
			print(f"{key} => {len(self._insert_dataset[key])}")

		self.insert()
		return res_list

	@property
	def steal_len(self):
		return len(self._steal_data)
	@property
	def insert_len(self):
		if len(self._insert_res) == 0:
			return 0
		total = 0
		for i in self._insert_res:
			total += i
		return total
	@property
	def delete_len(self):
		if len(self._delete_res) == 0:
			return 0
		total = 0
		for i in self._delete_res:
			total += i
		return total
	def part_insert_date(
		self,
		index_list,
		port=3306,
		user='root',
		password='mysql'
	):
		# Delete Add Host
		host = self.add_ip
		self._conn = pymysql.connect(
			host=host,
			port=port,
			user=user,
			password=password,
			database=self._database,
			cursorclass=pymysql.cursors.DictCursor
		)
		res_list = list()
		for idx in index_list:
			try:
				with self._conn.cursor() as cursor:
					sql = f"INSERT INTO {self._table} {str(self._columns)} VALUES {self._columns.convert(self._steal_date[idx])}"
					print(sql)
					self._conn.begin()
					res = cursor.execute(sql)
					res_list.append(res)
					self._conn.commit()
			except Exception as e:
				res_list.append(0)
				print(e)
				time.sleep(wait_printtime)
		self._conn = None
		return res_list
	@property
	def error_insert(self):
		self._check_i()
		return [ i for i,x in enumerate(self._insert_res) if x == 0]
	@property
	def error_delete(self,res_list):
		pass

	@property
	def columns(self):
		return self._columns

	# Steal,Insert,Delete
	def sid(self,steal_port=3306,insert_port=3306,script=True,update=True):
		steal_data = addnode.steal_data("sharding","user",steal_port)
		print("============== Steal Dataset Counter =================")
		for key in addnode._steal_dataset.keys():
			print(f"{key} => {len(addnode._steal_dataset[key])}")

		res = self.insert_data(port=insert_port)
		if self.contest_insert():
			if choice.delete_data(self.delete_ip):
				res = self.delete_data(port=steal_port)
				self.contest_delete()

		print(f"Success: Steal from {self.steal_ip}")
		print(f"Success: Insert into {self.insert_ip}")
		print(f"Success: Delete from {self.steal_ip}")
		if script:
			# Notification script for increment node
			if self._notice is not None:
				self._notice()
		if update:
			self._update_yaml()
	def anotice(self):
		if self._notice is not None:
			self._notice()

	def _update_yaml(self):
		addnode_dict = dict()
		addnode_dict["ip"] = self.add_ip
		addnode_dict["hash"] = self.add_hash
		new_iphashs = self._exists_iphashs
		new_iphashs.append(addnode_dict)
		parse.update_yaml(self._yaml_path,new_iphashs)


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Add node from IP Address")
	parser.add_argument("ip",help="IP Address of Node")
	parser.add_argument("yaml_path",help="Exists IP Addresses file path")

	args = parser.parse_args()

	columns = clms.Columns("id","username","hash_username","comment","start")
	addnode = MySQLAddNode(args.ip,"hash_username","sharding","user",columns,args.yaml_path,_DEBUG=True,funcpath="ls",notice_args=["-l"])

	print(f"steal IP: {addnode.steal_ip}")
	print(f"delete IP: {addnode.delete_ip}")
	print(f"insert IP: {addnode.insert_ip}")
	print(f"add IP: {addnode.add_ip}")

#	addnode.sid(steal_port=13306,insert_port=23306)
	addnode.anotice()

