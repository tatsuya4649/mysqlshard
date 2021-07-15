import argparse
from hashlib import new
import parse
import re
import sys
import copy
import pymysql
import test
import choice
import userpass
import notice
import ping
import columns as clms
from algo import con
import time
import shutil

class MySQLAddNode(test.MySQLConsistency):
	def __init__(
		self,
		ip,
		port,
		hash_column,
		database,
		table,
		yaml_path="ip.yaml",
		virtual_count=100,
		_DEBUG=False,
		funcpath=None,
		notice_args=[],
		notice_kwargs={},
		user=None,
		password=None,
		secret=False,
		secret_once=False,
		ping_interval=1,
		virtual_nodecount=100
	):
		super().__init__(ip,database,table)
		if not isinstance(port,int):
			raise ValueError("PORT number must be int")
		if funcpath is not None:
			self._notice = notice.Notice(funcpath)
		else:
			self._notice = None
		self._notice_args = notice_args
		self._notice_kwargs = notice_kwargs
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

		add_node_ip = ip
		add_node_hash = con.hash(add_node_ip.encode("utf-8"))
		self._add_node_dict = dict()
		self._add_node_dict["ip"] = add_node_ip
		self._add_node_dict["port"] = port
		self._add_node_dict["hash"] = add_node_hash
		if user is None or password is None or secret:
			res = userpass.secret_userpass(add_node_ip,port,self._database,"\033[31mPlease input add node database user and database password.\033[0m")
			user = res["user"]
			password = res["password"]
		self._add_node_dict["user"] = user
		self._add_node_dict["password"] = password

		ping.ping(ip,port,user,password,self._database)

		# get scheme
		self._columns = clms.MySQLColumns(
			ip=add_node_ip,
			port=port,
			database=self._database,
			table=self._table,
			user=user,
			password=password,
			show_column=True
		)

		new_iphashs = copy.deepcopy(self._exists_iphashs)
		for node in new_iphashs:
			if node["ip"] == self._add_node_dict["ip"]:
				raise ValueError("already addkd node in exists node")
		new_iphashs.append(self._add_node_dict)
		self._new_iphashs = parse.sort(new_iphashs)
		self._ipport = dict()
		self._ipuser = dict()
		self._ippass = dict()
		for node in self._new_iphashs:
			self._ipport[node["ip"]] = node["port"]
		userpassstr = f"\033[31mPlease input exists node database user and password\033[0m\nNOTICE: if trouble to input,\"secret=True\" in init function and insert user and password into yaml"
		for node in self._new_iphashs:
			if node["ip"] != add_node_ip:
				time.sleep(ping_interval)
				if ("user" not in node.keys() or "password" not in node.keys() or node["user"] is None or node["password"] is None or secret) and (secret_once is False):
					res = userpass.secret_userpass(node["ip"],node["port"],self._database,userpassstr)
					node["user"] = res["user"]
					node["password"] = res["password"]
				elif secret_once:
					node["user"] = user
					node["password"] = password
				self._ipuser[node["ip"]] = node["user"]
				self._ippass[node["ip"]] = node["password"]
				ping.ping(node["ip"],node["port"],node["user"],node["password"],self._database)
			else:
				self._ipuser[add_node_ip] = user 
				self._ippass[add_node_ip] = password 

		self._add_node_index = self._new_iphashs.index(self._add_node_dict)
		self._conn = None
		self._steal_data = list()
		self._hash_column = hash_column
		self._DEBUG = _DEBUG
		self._steal_ip = set()
		self._steal_query = dict()
		self._insert_ip = set()
		self._delete_ip = set()
		self._total_data_count = dict()

		self._virtual_nodecount=virtual_nodecount
		self._virtual_node(self._virtual_nodecount)
		self._ip_query = dict()
		self._total_transaction = self._real_steal_query()
		self._steal_dataset = dict()

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
		virtual_haship = dict()
		while total < count:
			for obj in self._new_iphashs:
				ip = obj["ip"]
				virtual_ip = f"{ip}{total}"
				virtual_hash = con.hash(virtual_ip.encode("utf-8"))
				virtual_dict = dict()
				virtual_dict["ip"] = ip
				virtual_dict["hash"] = virtual_hash
				virtual_haship[virtual_hash] = ip
				virtual_iphashs.append(virtual_dict)
				total += 1
		self._virtual_haship = sorted(virtual_haship.items())
		self._virtual_iphashs = virtual_iphashs
	
	# return array of dict(All IP and virtual hashs list)
	def _virtual_org(self):
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
	def add_port(self):
		return self._add_node_dict["port"]
	@property
	def add_hash(self):
		return self._add_node_dict["hash"]
	@property
	def add_index(self):
		return self._add_node_index
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
				if h < smallest and threshold <= h:
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
				if h > biggest and threshold >= h:
					results.append(h)
		if len(results) == 0:
			return None
		else:
			return max(results)

	"""
		add_virtual => Added virtual node dict("ip","hashs")
		virtual_index => Index dict("ip",index) for getting the index with the smallest hash value greater than the hash value of add_virtual
		virtualb_index => Index dict("ip",index) for getting the index with the biggest hash value smaller than the hash value of add_virtual
		nonaddvirs => virtuals excluding add_virtual
	"""
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

	def _have_real_node(self,hash,exih_order):
		for j in range(len(exih_order)):
			if exih_order[j]["hash"] > hash:
				return exih_order[j],exih_order[j-1]
		return exih_order[0],exih_order[-1]

	def _real_steal_query(self):
		HASH_INDEX=0
		IP_INDEX=1
		virtuals = self._virtual_org()
		# Real node IP(key),HASH(value) list sorted in reverse
		exih_order = sorted(self._exists_iphashs,key=lambda x:x["hash"],reverse=False)
		ip_query = dict()
		for haship in self._virtual_haship:
			ip_query[haship[IP_INDEX]] = list()

		# Get From Exists Node
		vhip_len = len(self._virtual_haship)
		for i in range(vhip_len):
			haship = self._virtual_haship[i]
		
		exists_hashs = list()
		for node in self._exists_iphashs:
			exists_hashs.append(node["hash"])

		total_transaction = list()
		pre_trans = dict()
		for i in range(vhip_len):
			haship = self._virtual_haship[i]
			if haship[HASH_INDEX] in exists_hashs:
				continue
			nowhaveih,previh = self._have_real_node(haship[HASH_INDEX],exih_order)
			trans = dict()
			if i == 0 or previh is None:
				trans["minhash"] = previh["hash"]
				trans["maxhash"] = haship[HASH_INDEX]
			else:
				trans["minhash"] = pretrans["maxhash"]
				trans["maxhash"] = haship[HASH_INDEX]
			trans["steal_ip"] = nowhaveih["ip"]
			trans["insert_ip"] = haship[IP_INDEX]
			if trans["steal_ip"] != trans["insert_ip"]:
				total_transaction.append(trans)
			pretrans = trans
		return total_transaction

	@property
	def steal_ip(self):
		steal_ips = list()
		for trans in self._total_transaction:
			if trans["steal_ip"] not in steal_ips:
				steal_ips.append(trans["steal_ip"])
		return steal_ips
#		return list(self._ip_query.keys())
	def _steal_dataset_init(self):
		for column in self._columns:
			self._steal_dataset[column] = set()
	def _steal_dataset_set(self,results):
			for data in results: 
				for column in data.keys():
					if column in self._steal_dataset.keys():
						self._steal_dataset[column].add(data[column])
					else:
						raise KeyError(f"_steal_dataset must have column({column})")
	# Steal Data From Next
	def steal_data(
		self
	):
		self._steal_ip_count = dict()
		for ip in self.steal_ip:
			self._steal_ip_count[ip] = 0
		self._steal_dataset_init()
		for trans in self._total_transaction:
			ip = trans["steal_ip"]
			query = f"\"{trans['minhash']}\" < {self._hash_column} AND \"{trans['maxhash']}\" >= {self._hash_column}"
			print(query)
			# Steal Next Host
			self._conn = pymysql.connect(
				host=ip,
				port=self._ipport[ip],
				user=self._ipuser[ip],
				password=self._ippass[ip],
				database=self._database,
				cursorclass=pymysql.cursors.DictCursor
			)
			try:
				with self._conn.cursor() as cursor:
					sql = f"SELECT * FROM {self._table} WHERE {query}"
					cursor.execute(sql)
					results = cursor.fetchall()
					trans["steal_data"] = results
					self._steal_ip_count[ip] += len(results)
					self._steal_dataset_set(results)
			except Exception as e:
				print(e)
			finally:
				self._conn = None
		self.steal()

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
		wait_printtime=10
	):
		res_list = list()
		self._delete_dataset_init()
		for ip in self.steal_ip:
			self._conn = pymysql.connect(
				host=ip,
				port=self._ipport[ip],
				user=self._ipuser[ip],
				password=self._ippass[ip],
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
	):
		self._conn = pymysql.connect(
			host=self.insert_ip,
			port=self._insert_port,
			user=self._ipuser[self.insert_ip],
			password=self._ippass[self._insert_ip],
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
		if hasattr(self,"_steal_data") and self._steal_data is not None and len(self._steal_data) != 0:
			for column in self._steal_data[0]:
				self._insert_dataset[column] = set()
		
	# Insert New Data
	def insert_data(
		self,
		wait_printtime=10, # waiting for confirming Error 
	):
		# Delete Add Host
		host = self.add_ip
		self._conn = pymysql.connect(
			host=host,
			port=self._ipport[host],
			user=self._ipuser[host],
			password=self._ippass[host],
			database=self._database,
			cursorclass=pymysql.cursors.DictCursor
		)
		self._insert_port = self.add_port
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

#		print("============== Insert Dataset Counter =================")
#		print(len(self._insert_res))
#		for key in self._insert_dataset.keys():
#			print(f"{key} => {len(self._insert_dataset[key])}")
#
		self.insert()
		return res_list

	@property
	def steal_len(self):
		total_steal = 0
		for node in self._new_iphashs:
			ip = node["ip"]
			try:
				total_steal += self._steal_ip_count[ip]
			except KeyError as e:
				total_steal += 0
		return total_steal
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
		wait_printtime=10,
	):
		# Delete Add Host
		host = self.add_ip
		self._conn = pymysql.connect(
			host=host,
			port=self._ipport[host],
			user=self._ipuser[host],
			password=self._ippass[host],
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
	
	def _get_allnode_data_count(self):
		for node in self._new_iphashs:
			ip = node["ip"]
			port = node["port"]
			try:
				conn = pymysql.connect(
					host=ip,
					port=port,
					user=node["user"],
					password=node["password"],
					db=self._database,
					cursorclass=pymysql.cursors.DictCursor
				)
				with conn:
					with conn.cursor() as cursor:
						sql = f"SELECT COUNT(*) from {self._table}"
						cursor.execute(sql)
						result = cursor.fetchone()
				count = re.match(r'^COUNT.*',list(result.keys())[0],flags=re.IGNORECASE)
				if count is not None:
					self._total_data_count[ip] = result[count.group()]
				else:
					raise Exception
			except Exception as e:
				print(e)
				sys.exit(1)
	def _dot_count(self,column_size,length):
		count = int((column_size - length)/2)
		remainder = (column_size - length)%2
		return count,remainder
	def _draw(self,string,count):
		for _ in range(count):
			print(string,end="")

	# Show amount of data ratio before and after
	def _before_after(self):
		self._get_allnode_data_count()

		allnode_ip = list()
		allnode_updown = dict()
		total_datacount = 0
		for node in self._new_iphashs:
			allnode_ip.append(node["ip"])
			total_datacount += self._total_data_count[node["ip"]]

		# Before
		termcolumn = shutil.get_terminal_size().columns
		before_len = len("before")
		after_len = len("after")
		count,remainder = self._dot_count(termcolumn,before_len)
		self._draw("-",count)
		print("before",end="")
		self._draw("-",count)
		if remainder:
			print("-")

		for ip in allnode_ip:
			print("|",end="")
			before = self._total_data_count[ip]
			allnode_updown[ip] = before
			string = f"{ip}: {before}"
			count,remainder = self._dot_count(termcolumn-2,len(string))
			self._draw(" ",count)
			print(string,end="")
			self._draw(" ",count)
			if remainder:
				print("",end="")
			print("|")
			
		self._draw("-",termcolumn)

		# After
		count,remainder = self._dot_count(termcolumn,after_len)
		self._draw("-",count)
		print("after",end="")
		self._draw("-",count)
		if remainder:
			print("-")

		aftertotal_datacount = 0
		for ip in allnode_ip:
			print("|",end="")
			if ip == self.insert_ip:
				after = self._total_data_count[ip] + self.steal_len
			else:
				try:
					after = self._total_data_count[ip] - self._steal_ip_count[ip]
				except KeyError:
					after = self._total_data_count[ip]
			aftertotal_datacount += after
			gap = allnode_updown[ip] - after
			string = f"{ip}: {after}"
			stringlen = len(string)
			if gap > 0:
				string = f"{ip}: \033[31m{after}\033[0m"
			elif gap == 0:
				string = f"{ip}: {after}"
			else:
				string = f"{ip}: \033[34m{after}\033[0m"
			count,remainder = self._dot_count(termcolumn-2,stringlen)
			self._draw(" ",count)
			print(string,end="")
			self._draw(" ",count)
			if remainder:
				print(" ",end="")
			print("|")

		self._draw("-",termcolumn)

		print("")

		if total_datacount != aftertotal_datacount:
			raise test.ConsistencyError("difference total data and after total data.")
		# If choise not to send, end.
		if not choice.insert_ok():
			print("Data was not moved from Existed node to Added node.")
			sys.exit(1)

	@property
	def columns(self):
		return self._columns

	# Steal,Insert,Delete
	def sid(self,script=True,update=True):
		steal_data = self.steal_data()
		if len(self._steal_dataset.keys()) == 0:
			print(f"No Data should be sent to new added node. Already, has already been sent.")
			sys.exit(1)

		self._before_after()

		res = self.insert_data()
		if self.contest_insert():
			if choice.delete_data(self.delete_ip):
				res = self.delete_data()
				self.contest_delete()

		print(f"Success: Steal from {self.steal_ip}")
		print(f"Success: Insert into {self.insert_ip}")
		print(f"Success: Delete from {self.steal_ip}")
		if script:
			# Notification script for increment node
			if self._notice is not None:
				self._notice(*self._notice_args,**self._notice_kwargs)
		if update:
			self._update_yaml()

	def _update_yaml(self):
		addnode_dict = dict()
		addnode_dict["ip"] = self.add_ip
		addnode_dict["port"] = self.add_port
		addnode_dict["hash"] = self.add_hash
		new_iphashs = self._exists_iphashs
		new_iphashs.append(addnode_dict)
		parse.update_yaml(self._yaml_path,new_iphashs)

	def _test_conshash(self):
		pass

