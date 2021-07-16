import argparse
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
		_DEBUG=False,
		funcpath=None,
		notice_args=[],
		notice_kwargs={},
		user=None,
		password=None,
		secret=False,
		secret_once=False,
		ping_interval=1,
		virtual_nodecount=100,
		require_reshard=True,
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
		self._secret = secret
		# If require resharding, steal data from all real node
		self._require_reshard = require_reshard
		if not self.ip_check(ip):
			print(f"not IP Address {ip}",file=sys.stderr)
			raise Exception
		self._yaml_path = yaml_path
		try:
			self._exists_iphashs = self._parse_yaml(yaml_path)
		except FileNotFoundError as e:
			raise FileNotFoundError

		# check YAML format
		if not isinstance(self._exists_iphashs,list):
			raise TypeError("this YAML File is invalid in this program.")
		for node in self._exists_iphashs:
			if "ip" not in node.keys() or \
					"port" not in node.keys():
					raise TypeError("this This is invalid in this program.(no ip,port)")
			elif "hash" not in node.keys():
				node["hash"] = [con.hash(node["ip"].encode("utf-8"))]
			if not isinstance(node["hash"],list):
				raise TypeError("this YAML File is invalid in this program.(hash must be list)")
			
		add_node_ip = ip
		add_node_hash = con.hash(add_node_ip.encode("utf-8"))
		self._add_node_dict = dict()
		self._add_node_dict["ip"] = add_node_ip
		self._add_node_dict["port"] = port
		self._add_node_dict["hash"] = [add_node_hash]
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
				raise ValueError("already added node in exists node")
		new_iphashs.append(self._add_node_dict)
		# If resharding, delete virtual node
		if require_reshard:
			for node in self._exists_iphashs:
				node_hash = con.hash(node["ip"].encode("utf-8"))
				node["hash"] = [node_hash]
			for node in new_iphashs:
				node_hash = con.hash(node["ip"].encode("utf-8"))
				node["hash"] = [node_hash]

		self._new_iphashs = new_iphashs
		self._ipport = dict()
		self._ipuser = dict()
		self._ippass = dict()
		for node in self._new_iphashs:
			self._ipport[node["ip"]] = node["port"]
		userpassstr = f"\033[31mPlease input exists node database user and password\033[0m\nNOTICE: if trouble to input,\"secret=True\" in init function and insert user and password into yaml"
		iphashs_set = set()
		for node in self._new_iphashs:
			if node["ip"] in iphashs_set: continue
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
				iphashs_set.add(node["ip"])
			else:
				self._ipuser[add_node_ip] = user 
				self._ippass[add_node_ip] = password 
				iphashs_set.add(node["ip"])
		
		self._conn = None
		self._steal_data = list()
		self._hash_column = hash_column
		self._DEBUG = _DEBUG
		self._steal_ip = set()
		self._steal_query = dict()
		self._insert_ip = set()
		self._delete_ip = set()
		self._total_data_count = dict()
		self._deplica_insert = dict()

		self._virtual_node(virtual_nodecount)
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
		total = 0
		for node in self._new_iphashs:
			total += len(node["hash"])
		virtual_iphashs = copy.deepcopy(self._new_iphashs)
		virtual_haship = dict()

		for obj in self._new_iphashs:
			ip = obj["ip"]
			for hash in obj["hash"]:
				virtual_haship[hash] = ip

		if total < count:
			while total < count:
				for obj in virtual_iphashs:
					ip = obj["ip"]
					virtual_ip = f"{ip}{total}"
					virtual_hash = con.hash(virtual_ip.encode("utf-8"))
					virtual_haship[virtual_hash] = obj["ip"]
					obj["hash"].append(virtual_hash)
					total += 1

		self._virtual_haship = sorted(virtual_haship.items())
		self._virtual_iphashs = virtual_iphashs
	
	# return array of dict(All IP and virtual hashs list)
	def _virtual_org(self):
		if self._virtual_iphashs is None:
			raise ValueError("must have _virtual_iphashs. hints: call _virtual_node function.")
		for obj in self._virtual_iphashs:
			obj["hash"].sort()
	@property
	def add_ip(self):
		return self._add_node_dict["ip"]
	@property
	def add_port(self):
		return self._add_node_dict["port"]
	@property
	def add_hash(self):
		return self._add_node_dict["hash"]

	def _have_real_node(self,hash,exih_order):
		for j in range(len(exih_order)):
			if exih_order[j]["hash"] >= hash:
				return exih_order[j],exih_order[j-1]
		return exih_order[0],exih_order[-1]

	def _real_steal_query(self):
		HASH_INDEX=0
		IP_INDEX=1
		self._virtual_org()
		# Real node IP(key),HASH(value) list sorted in reverse
		exih_order = list()
		for node in self._exists_iphashs:
			print(node)
			for hash in node["hash"]:
				exih_order.append(
					{
						"ip": node["ip"],
						"hash": hash
					}
				)
		exih_order = sorted(exih_order,key=lambda x:x["hash"])

		# Get From Exists Node
		vhip_len = len(self._virtual_haship)
		for i in range(vhip_len):
			haship = self._virtual_haship[i]
			print(haship)

		total_transaction = list()
		pre_trans = dict()
		for i in range(vhip_len):
			haship = self._virtual_haship[i]
			nowhaveih,previh = self._have_real_node(haship[HASH_INDEX],exih_order)
			trans = dict()
			if i == 0:
				trans["minhash"] = self._virtual_haship[-1][HASH_INDEX]
				trans["maxhash"] = haship[HASH_INDEX]
			else:
				trans["minhash"] = pretrans["maxhash"]
				trans["maxhash"] = haship[HASH_INDEX]
			trans["steal_ip"] = nowhaveih["ip"]
			trans["insert_ip"] = haship[IP_INDEX]
			if trans["steal_ip"] != trans["insert_ip"]:
				total_transaction.append(trans)
			pretrans = trans
		
#		for trans in total_transaction:
#			print(trans)
		return total_transaction

	@property
	def steal_ip(self):
		steal_ips = list()
		for trans in self._total_transaction:
			if trans["steal_ip"] not in steal_ips:
				steal_ips.append(trans["steal_ip"])
		return steal_ips
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
		self,
		trans
	):
		self._steal_dataset_init()
		ips = set()
		ips.add(trans["steal_ip"])
		if self._require_reshard:
			for node in self._new_iphashs:
				ips.add(node["ip"])

		trans["steal_data"] = list()
		trans["steal_fake"] = dict()
		for ip in ips:
			query = f"\"{trans['minhash']}\" < {self._hash_column} AND \"{trans['maxhash']}\" >= {self._hash_column}"
			sql = f"SELECT * FROM {self._table} WHERE {query}"
			print(sql)
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
					cursor.execute(sql)
					results = cursor.fetchall()
					if len(results) > 0 and ip != trans["steal_ip"]:
						trans["steal_fake"][ip] = len(results)
					for result in results:
						if result not in trans["steal_data"]:
							trans["steal_data"].append(result)
					trans["steal_len"]  = len(trans["steal_data"])
					self._steal_ip_count[ip] += len(results)
					self._steal_dataset_set(results)
			except Exception as e:
				print(e)
			finally:
				self._conn = None

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
		trans,
		wait_printtime=1,
	):
		res_list = list()
		self._delete_dataset_init()
		steal_ip = trans["steal_ip"]
		ips = list()
		ips += list(trans["steal_fake"].keys())
		ips.append(steal_ip)
		if "steal_fake" in trans.keys():
			for ip in ips:
				if ip != trans["insert_ip"]:
					self._conn = pymysql.connect(
						host=ip,
						port=self._ipport[ip],
						user=self._ipuser[ip],
						password=self._ippass[ip],
						db=self._database,
						cursorclass=pymysql.cursors.DictCursor
					)
					query = f"\"{trans['minhash']}\" < {self._hash_column} AND \"{trans['maxhash']}\" >= {self._hash_column}"
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
						raise
					finally:
						self._conn = None

		return res_list
	@property
	def insert_ip(self):
		insert_ips = list()
		for trans in self._total_transaction:
			if trans["insert_ip"] not in insert_ips:
				insert_ips.append(trans["insert_ip"])
		return insert_ips
	def _insert_dataset_init(self):
			for column in self._columns:
				self._insert_dataset[column] = set()
	
	def _init_contest_data(self):
		self._insert_res = list()
		self._steal_ip_count = dict()
		for ip in self.steal_ip:
			self._steal_ip_count[ip] = 0
		self._get_allnode_data_count()
	# Insert New Data
	def insert_data(
		self,
		trans,
		wait_printtime=1, # waiting for confirming Error 
	):
		res_list = list()
		self._insert_dataset_init()
		host = trans["insert_ip"]
		self._conn = pymysql.connect(
			host=host,
			port=self._ipport[host],
			user=self._ipuser[host],
			password=self._ippass[host],
			database=self._database,
			cursorclass=pymysql.cursors.DictCursor
		)
		# Get from data from not steal_ip
		# So, may duplicate data, delete insert_data from insert_ip
		if "steal_fake" in trans.keys() or self._require_reshard:
			for steal_ip in trans["steal_fake"].keys():
				query = f"\"{trans['minhash']}\" < {self._hash_column} AND \"{trans['maxhash']}\" >= {self._hash_column}"
				try:
					with self._conn.cursor() as cursor:
						sql = f"DELETE FROM {self._table} WHERE {query}"
						self._conn.begin()
						cursor.execute(sql)
						self._conn.commit()
				except Exception as e:
					res_list.append(0)
					print(e)
					time.sleep(wait_printtime)
		complete_list = list()
		for insert_data in trans["steal_data"]:
			if insert_data not in complete_list:
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
						complete_list.append(insert_data)
				except Exception as e:
					print(e)
					res_list.append(0)
					time.sleep(wait_printtime)
					raise
		self._conn = None
	
		self._insert_res += res_list

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
	def insert_len(self,ip):
		total_len = 0
		for trans in self._total_transaction:
			if trans["insert_ip"] == ip:
				try:
					total_len += trans["steal_len"]
				except KeyError:
					total_len += 0
		return total_len

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

	def _check_trans(self):
		if self._require_reshard:
			termcolumn = shutil.get_terminal_size().columns
			reshard = "Resharding Mode"
			for _ in range(int((termcolumn-len(reshard))/2)):
				print("-",end='')
			print(reshard,end='')
			for _ in range(int((termcolumn-len(reshard))/2)):
				print("-",end='')
			print("")
		print(f'DB: {self._database} TABLE: {self._table} HASH_COLUMN: {self._hash_column}')
		for trans in self._total_transaction:
			ipinfo = f'{trans["steal_ip"]}=>{trans["insert_ip"]}'
			hashinfo = f'(\"{trans["minhash"]}\"~\"{trans["maxhash"]}\")'
			print("%-33s%s"%(ipinfo,hashinfo))
		if not choice.trans_ok():
			print("End program without moving data.")
			sys.exit(1)

	@property
	def columns(self):
		return self._columns
	
	def _redo_errhandle(self,redo_trans):
		res = choice.redo_errhandle()
		if res is choice.RedoErrorHandle.CANCEL:
			sys.exit(1)
		elif res is choice.RedoErrorHandle.FILE:
			with open("err.data","a") as f:
				writestr = f'{redo_trans["steal_ip"],redo_trans["insert_ip"],redo_trans["minhash"],redo_trans["maxhash"]},'
				if "steal_len" in redo_trans.keys():
					writestr += f'{redo_trans["steal_len"]},'
				if "steal_data" in redo_trans.keys():
					writestr += f'{redo_trans["steal_data"]}'
				writestr += f'\n'
				f.write(writestr)
			sys.exit(1)
		else:
			self._redo_errhandle(redo_trans)


	def _redo(self):
		for i in range(len(self._total_transaction)):
			trans = self._total_transaction[-1-i]
			if trans["complete"] is True:
				ipinfo = f'{trans["insert_ip"]}=>{trans["steal_ip"]}'
				hashinfo = f'(\"{trans["minhash"]}\"~\"{trans["maxhash"]}\")'
				redo_trans = copy.deepcopy(trans)
				redo_trans["steal_ip"] = trans["insert_ip"]
				redo_trans["insert_ip"] = trans["steal_ip"]
				redo_trans["complete"] = False

				try:
					self.steal_data(redo_trans)
					if len(self._steal_dataset.keys()) == 0:
						continue
					self.insert_data(redo_trans)
					self.delete_data(redo_trans)
					del redo_trans["steal_data"]
					redo_trans["complete"] = True
					print("%-33s%s"%(ipinfo,hashinfo))
				except Exception as e:
					print(e)
					print("\033[31mOccur Error in Redo\033[0m")
					self._redo_errhandle(redo_trans)
				finally:
					self._steal_dataset_init()
		print("Back to be state of before of sharding.")
		sys.exit(1)

	def _error_selection(self,trans):
		error_handle = choice.error_handle()
		if error_handle == choice.ErrorHandle.REDO:
			self._redo()
		elif error_handle == choice.ErrorHandle.CANCEL:
			if choice.really_cancel(trans["steal_ip"],trans["insert_ip"]):
				sys.exit(1)
			else:
				self._error_selection(trans)
		elif error_handle == choice.ErrorHandle.CONTINUE:
			if choice.really_continue(trans["steal_ip"],trans["insert_ip"]):
				return 
			else:
				self._error_selection(trans)
		else:
			raise choice.UnknownErrorHandle


	# Steal,Insert,Delete
	def sid(self,script=True,update=True):
		self._check_trans()
		for trans in self._total_transaction:
			trans["complete"] = False
		
		self._init_contest_data()
		after_totalcount = copy.deepcopy(self._total_data_count)

		for trans in self._total_transaction:
			try:
				self.steal_data(trans)
				if len(self._steal_dataset.keys()) == 0:
					continue
				self.insert_data(trans)
				self.delete_data(trans)
				del trans["steal_data"]
				trans["complete"] = True
				sip_count = trans["steal_len"]
				for sip in trans["steal_fake"].keys():
					after_totalcount[sip] -=  trans["steal_fake"][sip]
					sip_count -= trans["steal_fake"][sip]
				after_totalcount[trans["steal_ip"]] -= sip_count
				after_totalcount[trans["insert_ip"]] += trans["steal_len"]

				# DEBUG
				if trans is self._total_transaction[-1]:
					raise Exception

			except Exception as e:
				print(f"Occurred error !!!")
				print(f'\033[31mDB: {self._database} TABLE: {self._table} HASH_COLUMN: {self._hash_column}\033[0m')
				ipinfo = f'\033[31m{trans["steal_ip"]}=>{trans["insert_ip"]}\033[0m'
				hashinfo = f'\033[31m(\"{trans["minhash"]}\"~\"{trans["maxhash"]}\")\033[0m'
				print("%-33s%s"%(ipinfo,hashinfo))
				self._error_selection(trans)

		print("Before :")
		for node in self._new_iphashs:
			print(f'\t{node["ip"]} => {self._total_data_count[node["ip"]]}')
		print("After :")
		for node in self._new_iphashs:
			print(f'\t{node["ip"]} => {after_totalcount[node["ip"]]}')

		beforetotal = 0
		aftertotal = 0
		for countip in self._total_data_count.keys():
			beforetotal += self._total_data_count[countip]
		for countip in after_totalcount.keys():
			aftertotal += after_totalcount[countip]

		if beforetotal != aftertotal:
			raise test.ConsistencyUnmatchError("Unmatch total before and after data count.")
		else:
			print(f'\rTotal Data Count: {aftertotal}')

		if script:
			# Notification script for increment node
			if self._notice is not None:
				self._notice(*self._notice_args,**self._notice_kwargs)
		if update:
			self._update_yaml()

	def _update_yaml(self):
		new_iphashs = copy.deepcopy(self._new_iphashs)
		vnode_dict = dict()
		vnode_iphashs = list()
		for node in new_iphashs:
			vnode_dict[node["ip"]] = list()
			for vnode in self._virtual_iphashs:
				if node["ip"] == vnode["ip"]:
					vnode_dict[node["ip"]].append(vnode["hash"])
			vnode_dict[node["ip"]].sort()

			vnode = dict()
			vnode["ip"] = node["ip"]
			vnode["hash"] = vnode_dict[node["ip"]]
			vnode["port"] = self._ipport[node["ip"]]
			if not self._secret:
				vnode["user"] = self._ipuser[node["ip"]]
				vnode["password"] = self._ippass[node["ip"]]
			vnode_iphashs.append(vnode)

		parse.update_yaml(self._yaml_path,vnode_iphashs)

	def _test_conshash(self):
		pass

