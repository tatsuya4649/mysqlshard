import argparse
from unittest import result
from yaml import resolver

from yaml.nodes import Node
import re
import sys
import copy
import pymysql
from . import parse
from . import test
from . import choice
from . import userpass
from . import notice
from . import ping
from . import columns as clms
from .ip import ip_check
from .algo import con
from utils.parse import parse_bool 
import time
import shutil
from enum import Enum
from .err import *

class NodeMode(Enum):
	ADD			= "add"			# New node add cluster
	DELETE		= "delete"			# Exists node delete from cluster
class Logical(Enum):
	AND			= "and"
	OR			= "or"

class NodeWorker:
	"""
	Parameter required by the NodeWorker: cluster_info,params
	---
	
	Required key of cluster_info:
		database,table,hash_column,cluster_yaml
	Required key of params:
		ip,port,mode
	"""
	def work(self):
		raise NotImplementedError("work must be implemented.")
	def _cluster_check(self,cluster_info):
		if "database" not in cluster_info.keys():
			raise NodeWorkerValueError("cluster_info dict must have databse key.")
		if "table" not in cluster_info.keys():
			raise NodeWorkerValueError("cluster_info dict must have table key.")
		if "hash_column" not in cluster_info.keys():
			raise NodeWorkerValueError("cluster_info dict must have hash_column key.")
		if "cluster_yaml" not in cluster_info.keys():
			raise NodeWorkerValueError("params dict must have yaml_path key.")

		self._database = cluster_info["database"]
		self._table = cluster_info["table"]
		self._hash_column = cluster_info["hash_column"]
		self._yaml_path = cluster_info["cluster_yaml"]

		if not isinstance(self._database,str):
			raise NodeWorkerTypeError("Database mast be string.")
		if not isinstance(self._table,str):
			raise NodeWorkerTypeError("Table mast be string.")
		if not isinstance(self._hash_column,str):
			raise NodeWorkerTypeError("Hash Column mast be string.")
		if not isinstance(self._yaml_path,str):
			raise NodeWorkerTypeError("yaml_path must be str")
	def _params_required(self,params):
		if "ip" not in params.keys():
			raise NodeWorkerValueError("params dict must have ip key.")
		if "port" not in params.keys():
			raise NodeWorkerValueError("params dict must have port key.")
		if "mode" not in params.keys():
			raise NodeWorkerValueError("params dict must have mode key.")
		self._ip = params["ip"]
		self._port = params["port"]
		self._mode = [v for _,v in NodeMode.__members__.items() if v.value == params["mode"]]
		if len(self._mode) != 1:
			raise NodeWorkerTypeError("mode must be NodeMode(Enum)")
		self._mode = self._mode[0]

		if not ip_check(self._ip):
			raise NodeWorkerValueError("IP is invalid.")
		if not isinstance(self._port,int):
			raise NodeWorkerTypeError("Port number mast be integer.")
		if not isinstance(self._mode,NodeMode):
			raise NodeWorkerTypeError("mode must be NodeMode(Enum)")


class MySQLWorker(NodeWorker):
	"""
	Optional parameter of MySQLWorker: params["option"]
	---

	params["option"]: dict type. these parameters control operate.
	"""
	doclists = [NodeWorker.__doc__ if NodeWorker.__doc__ is not None else "",__doc__]
	__doc__ = "".join(doclists)
	_VIRTUAL_NODECOUNT_DEFAULT=100
	_REQUIRE_RESHARD_DEFAULT=True
	_FUNCPATH_DEFAULT=None
	_PING_INTERVAL_DEFAULT=0
	_NOTICE_ARGS_DEFAULT=None
	_NOTICE_KWARGS_DEFAULT=None
	_SECRET_DEFAULT=False
	_SECRET_ONCE_DEFAULT=False
	_UPDATE_DEFAULT=True
	_USER_DEFAULT=None
	_PASSWORD_DEFAULT=None
	def __init__(
		self,
		cluster_info,
		params,
	):
		super().__init__()
		option = params["option"] if "option" in params.keys() else None
		self._cluster_check(cluster_info)
		self._params_required(params)

		funcpath = option["funcpath"] if option is not None and "funcpath" in option.keys() else self._FUNCPATH_DEFAULT
		if funcpath is not None:
			self._notice = notice.Notice(funcpath)
		else:
			self._notice = None
		self._notice_args = option["notice_args"] if option is not None and "notice_args" in option.keys() else self._NOTICE_ARGS_DEFAULT
		self._notice_kwargs = option["notice_kwargs"] if option is not None and "notice_kwargs" in option.keys() else self._NOTICE_KWARGS_DEFAULT

		self._secret = parse_bool(option["secret"]) if option is not None and "secret" in option.keys() else self._SECRET_DEFAULT
		self._secret_once = parse_bool(option["secret_once"]) if option is not None and  "secret_once" in params.keys() else self._SECRET_ONCE_DEFAULT
		self._update = parse_bool(option["update"]) if option is not None and "update" in option.keys() else self._UPDATE_DEFAULT
		self._virtual_nodecount = option["virtual_nodecount"] if option is not None and  "virtual_nodecount" in params.keys() else self._VIRTUAL_NODECOUNT_DEFAULT

		# If require resharding, steal data from all real node
		self._require_reshard = parse_bool(option["require_reshard"]) if option is not None and  "require_reshard" in params.keys() else self._REQUIRE_RESHARD_DEFAULT

		try:
			self._exists_iphashs = self._parse_yaml(self._yaml_path)
			if self._mode == NodeMode.DELETE:
				self._exists_topology = copy.deepcopy(self._exists_iphashs)
		except FileNotFoundError as e:
			raise FileNotFoundError(f"not found cluster yaml file.({self._yaml_path})")

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
		
		if self._mode is NodeMode.ADD:
			# if target node in yaml format, error
			if len([x for x in self._exists_iphashs if x["ip"] == self._ip]) != 0:
				raise ValueError("There are already target node in YAML file. target must be non exists node ip.")
		elif self._mode is NodeMode.DELETE:
			# if no target node in yaml format, error
			if len([x for x in self._exists_iphashs if x["ip"] == self._ip]) == 0:
				raise ValueError("There is no target node in YAML file. Because target node is delete from existed node, it must be in YAML file.")
		
		target_node_ip = self._ip
		target_node_hash = con.hash(target_node_ip.encode("utf-8"))
		self._target_node_dict = dict()
		self._target_node_dict["ip"] = target_node_ip
		self._target_node_dict["port"] = self._port
		if self._mode is NodeMode.ADD:
			self._target_node_dict["hash"] = [target_node_hash]

		user = option["user"] if option is not None and "user" in option.keys() else self._USER_DEFAULT
		password = option["password"] if option is not None and "password" in option.keys() else self._PASSWORD_DEFAULT
		if (self._mode is NodeMode.ADD and (user is None or password is None or self._secret)) or \
			(self._mode is NodeMode.DELETE and ((len([x for x in self._exists_iphashs if x["ip"] == target_node_ip and x["user"] is not None and x["password"] is not None]) == 0) or \
				(len([x for x in self._exists_iphashs if x["ip"] == target_node_ip and x["user"] is not None and x ["password"] is not None]) == 0))):
			res = userpass.secret_userpass(target_node_ip,self._port,self._database,"\033[31mPlease input add node database user and database password.\033[0m")
			user = res["user"]
			password = res["password"]
		self._target_node_dict["user"] = user
		self._target_node_dict["password"] = password

		ping.ping(self._ip,self._port,user,password,self._database)

		# get scheme
		self._columns = clms.MySQLColumns(
			ip=target_node_ip,
			port=self._port,
			database=self._database,
			table=self._table,
			user=user,
			password=password,
			show_column=True
		)

		new_iphashs = copy.deepcopy(self._exists_iphashs)
		for node in new_iphashs:
			if self._mode is NodeMode.ADD and node["ip"] == self._target_node_dict["ip"]:
				raise ValueError("already target node in exists node")
		
		if self._mode is NodeMode.ADD:
			new_iphashs.append(self._target_node_dict)
		elif self._mode is NodeMode.DELETE:
			new_iphashs = [ x for x in new_iphashs if x["ip"] != self._target_node_dict["ip"]]
			if len(new_iphashs) == 0:
				raise ValueError("Ther is only one node.If delete this node, all data will be gone.")

		# If resharding, delete virtual node
		if self._require_reshard:
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

		ping_interval = option["ping_interval"] if option is not None and "ping_interval" in option.keys() else self._PING_INTERVAL_DEFAULT
		_iphashs = self._mode_iphashs
		for node in _iphashs:
			self._ipport[node["ip"]] = node["port"]
		userpassstr = f"\033[31mPlease input exists node database user and password\033[0m\nNOTICE: if trouble to input,\"secret=True\" in init function and insert user and password into yaml"
		iphashs_set = set()
		for node in _iphashs:
			if node["ip"] in iphashs_set: continue
			if node["ip"] != target_node_ip:
				time.sleep(ping_interval)
				if ("user" not in node.keys() or \
					"password" not in node.keys() or \
						node["user"] is None or node["password"] is None \
							or self._secret) \
					and (self._secret_once is False):
					res = userpass.secret_userpass(node["ip"],node["port"],self._database,userpassstr)
					node["user"] = res["user"]
					node["password"] = res["password"]
				elif self._secret_once:
					node["user"] = user
					node["password"] = password
				self._set_another_dict(node)
				self._ipuser[node["ip"]] = node["user"]
				self._ippass[node["ip"]] = node["password"]
				ping.ping(node["ip"],node["port"],node["user"],node["password"],self._database)
				iphashs_set.add(node["ip"])
			else:
				self._ipuser[target_node_ip] = user 
				self._ippass[target_node_ip] = password 
				iphashs_set.add(node["ip"])
		
		self._conn = None
		self._steal_ip = set()
		self._steal_query = dict()
		self._insert_ip = set()
		self._delete_ip = set()
		self._total_data_count = dict()
		self._deplica_insert = dict()

		self._virtual_node(self._virtual_nodecount)
		self._total_transaction = self._real_steal_query()
		
	@property
	def _mode_iphashs(self):
		if self._mode is NodeMode.ADD:
			_iphashs = self._new_iphashs
		elif self._mode is NodeMode.DELETE:
			_iphashs = self._exists_iphashs
		return _iphashs
	@property
	def _mode_having_iphashs(self):
		if self._mode is NodeMode.ADD:
			_iphashs = self._exists_iphashs
		elif self._mode is NodeMode.DELETE:
			_iphashs = self._exists_topology
		return _iphashs
	@property
	def _mode_target_haship(self):
		if self._mode is NodeMode.ADD:
			_haship = self._virtual_haship
		elif self._mode is NodeMode.DELETE:
			_haship = list()
			for node in self._virtual_haship:
				_haship.append(
					{
						"ip": node[1],
						"hash": node[0]
					}
				)
			_haship = sorted(_haship,key=lambda x:x["hash"])
			return _haship
		return _haship
	# node Having data now sorted list
	@property
	def _mode_having_haship(self):
		exih_order = list()
		for node in self._mode_having_iphashs:
			for hash in node["hash"]:
				exih_order.append(
					{
						"ip": node["ip"],
						"hash": hash
					}
				)
		exih_order = sorted(exih_order,key=lambda x:x["hash"])
		return exih_order

	def _set_another_dict(self,node):
		if self._mode is NodeMode.ADD:
			for iphash in self._exists_iphashs:
				if iphash["ip"] == node["ip"]:
					for key in node.keys():
						iphash[key] = node[key]
		elif self._mode is NodeMode.DELETE:
			for iphash in self._new_iphashs:
				if iphash["ip"] == node["ip"]:
					for key in node.keys():
						iphash[key] = node[key]
	@property
	def exists_iphashs(self):
		return self._exists_iphashs

	def _parse_yaml(self,path):
		return parse.parse_yaml(path)
	
	def _virtual_node_hash(self,ip,total):
		virtual_ip = f'{ip}{total}'
		virtual_hash = con.hash(virtual_ip.encode("utf-8"))
		return virtual_hash

	# count: total node count(real node + virtual node)
	def _virtual_node(self,count):
		total = 0
		virtual_iphashs = copy.deepcopy(self._new_iphashs)
		virtual_haship = list()
		for obj in virtual_iphashs:
			total += len(obj["hash"])
			for hash in obj["hash"]:
				virtual_haship.append({
					"hash": hash,
					"ip": obj["ip"]
				})
		while total < count:
			for obj in virtual_iphashs:
				virtual_hash = self._virtual_node_hash(obj["ip"],total)
				virtual_haship.append({
					"hash": virtual_hash,
					"ip": obj["ip"]
				})
				obj["hash"].append(virtual_hash)
				print(f'{obj["ip"]}: {virtual_hash}')
				total += 1
		# This is the node info that is distination of moving data
		self._virtual_haship = sorted(virtual_haship,key=lambda x:x["hash"])
		self._virtual_iphashs = virtual_iphashs
	
	# return array of dict(All IP and virtual hashs list)
	def _virtual_org(self):
		if self._virtual_iphashs is None:
			raise ValueError("must have _virtual_iphashs. hints: call _virtual_node function.")
		for obj in self._virtual_iphashs:
			obj["hash"].sort()
	@property
	def target_ip(self):
		return self._target_node_dict["ip"]
	@property
	def target_port(self):
		return self._target_node_dict["port"]
	@property
	def target_hash(self):
		return self._target_node_dict["hash"]
	
	# receive node having data now, and hash id of after moving node
	# returns a dictionary at the index that represents the node to which data should be passed 
	def _have_real_node(self,target_hash,having_order):
		for j in range(len(having_order)):
			if self._mode == NodeMode.ADD:
				if having_order[j]["hash"] >= target_hash:
					return [j]
			elif self._mode == NodeMode.DELETE:
				results = list()
				if having_order[j]["hash"] > target_hash:
					results.append(j)
					nondelete_lists = [ i for i in having_order if i["hash"] <= target_hash and "checked" not in i.keys()]
					for ele in nondelete_lists:
						if having_order.index(ele) not in results:
							results.append(having_order.index(ele))
						ele["checked"] = True
					results.sort()
					return results
		return [0]
	# received target hash, return IP of node having data greater than or equal to target hash
	def _next_having(self,hash,having_order):
		for hav in having_order:
			if hash <= hav["hash"]:
				return hav["ip"]
	def _real_steal_query(self):
		self._virtual_org()
		# get nodes that having a data now (before moving.).Sorted by hashid.
		# [{'hash': `HASH`,ip: `IP`}]
		having_haship = self._mode_having_haship
		# get nodes length that will be haved data (after moving.)
		target_len = len(self._mode_target_haship)
		total_transaction = list()
		pre_trans = dict()

#		for haship in having_haship:
#			print(haship)
#		for iphash in self._virtual_iphashs:
#			print(iphash)
		# loog node that after moving.
		for i in range(target_len):
			# target node Hash and IP in after moving
			haship = self._mode_target_haship[i]
			matchindex = self._have_real_node(haship["hash"],having_haship)
			for index in matchindex:
#				print("'''''''''''''")
				matchih = self._mode_having_haship[index]
				trans = dict()
				if self._mode is NodeMode.DELETE:
					prevhash = self._mode_target_haship[i-1]
					if i == 0:
						nodelete_lists = [ x for x in having_haship if x["hash"] > prevhash["hash"] and "checked" not in x.keys()]
						for ele in nodelete_lists:
							if having_haship.index(ele) not in nodelete_lists:
								matchindex.append(having_haship.index(ele))
							ele["checked"] = True
							ele["minus"] = True
						matchindex.sort()
						
					previndex = matchindex.index(index) - 1 if matchindex.index(index) > 0 \
						else -1
					prevmatchindex = matchindex[previndex]
					prevhaving = self._mode_having_haship[prevmatchindex]
					if i != 0:
						lasthash = prevhash \
							if (matchindex.index(index) == 0 or len(matchindex) == 1 or previndex < 0) \
								else prevhaving if prevhaving["hash"] > prevhash["hash"] \
									else prevhash
					else:
						lasthash = prevhash \
							if (matchindex.index(index) == 0 or len(matchindex) == 1 or previndex < 0) \
								else prevhaving if prevhaving["hash"] > prevhash["hash"] \
									else prevhash
						if len([ele for ele in nodelete_lists if "minus" in ele.keys()]) != 0:
							lasthash = prevhaving \
								if prevhaving["hash"] > lasthash["hash"] else lasthash
							eles = [ele for ele in nodelete_lists if "minus" in ele.keys()]
							for ele in eles:
								del ele["minus"]
						else:
							lasthash = prevhaving \
								if prevhaving["hash"] < lasthash["hash"] else lasthash
				if i == 0:
#							if self._mode_having_haship[-1]["hash"] >= self._mode_target_haship[-1][HASH_INDEX] \
#								else self._mode_target_haship[-1]["hash"]
					maxhash = matchih if matchih["hash"] < haship["hash"] else haship
					trans["minhash"] = self._mode_target_haship[-1]["hash"] if self._mode is NodeMode.ADD else lasthash["hash"]
					trans["maxhash"] = haship["hash"] if self._mode is NodeMode.ADD else maxhash["hash"]
					trans["logical"] = Logical.OR

				else:
#					if self._mode is NodeMode.DELETE:
#						prevhash = self._mode_target_haship[i-1][HASH_INDEX]
#						previndex = matchindex.index(index) - 1 if matchindex.index(index) > 0 \
#							else -1
#						prevmatchindex = matchindex[previndex]
#						print(prevmatchindex)
#						lasthash = prevhash if (index == 0 or len(matchindex) == 1 or previndex < 0) \
#							else self._mode_having_haship[prevmatchindex]["hash"] if self._mode_having_haship[prevmatchindex]["hash"] >= prevhash \
#								else prevhash
					maxhash = matchih if matchih["hash"] < haship["hash"] else haship
					trans["minhash"] = pre_trans["maxhash"] if self._mode is NodeMode.ADD else lasthash["hash"]
					trans["maxhash"] = haship["hash"] if self._mode is NodeMode.ADD else maxhash["hash"]
					trans["logical"] = Logical.AND
				if i != 0 and trans["minhash"] >= trans["maxhash"]:
					continue
				trans["steal_ip"] = matchih["ip"] if self._mode is NodeMode.ADD else self._next_having(trans["maxhash"],having_haship)
				trans["insert_ip"] = haship["ip"] if self._mode is NodeMode.ADD else haship["ip"]
#				print(trans["minhash"])
#				print(trans["maxhash"])
#				print(trans["steal_ip"])
#				print(trans["insert_ip"])
				if trans["steal_ip"] != trans["insert_ip"]:
					total_transaction.append(trans)
				pre_trans = trans
		if self._mode is NodeMode.DELETE:
			extralists = [x for x in having_haship if x["hash"] > self._mode_target_haship[-1]["hash"]]
			extralists = sorted(extralists,key=lambda x:x["hash"])
			pre_trans = dict()
			if len(extralists) != 0:
				for extra in extralists:
					trans = dict()
#					print("'''''''''''''")
					trans["minhash"] = self._mode_target_haship[-1]["hash"] if "maxhash" not in pre_trans.keys() else pre_trans["maxhash"]
					trans["maxhash"] = extra["hash"]
					trans["logical"] = Logical.AND
					trans["steal_ip"] = extra["ip"]
					trans["insert_ip"] = self._mode_target_haship[0]["ip"]
					if trans["steal_ip"] != trans["insert_ip"]:
						total_transaction.append(trans)
#					print(trans["minhash"])
#					print(trans["maxhash"])
		
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
	# Steal Data From Next
	def steal_data(
		self,
		trans
	):
		ips = set()
		ips.add(trans["steal_ip"])
		if self._require_reshard:
			for node in self._new_iphashs:
				ips.add(node["ip"])

		trans["steal_data"] = list()
		trans["steal_fake"] = dict()
		for ip in ips:
			query = f"\"{trans['minhash']}\" < {self._hash_column} {trans['logical'].value} \"{trans['maxhash']}\" >= {self._hash_column}"
			sql = f"SELECT * FROM {self._table} WHERE {query}"
			print(sql)
			# Steal Next Host
			try:
				self._conn = pymysql.connect(
					host=ip,
					port=self._ipport[ip],
					user=self._ipuser[ip],
					password=self._ippass[ip],
					database=self._database,
					cursorclass=pymysql.cursors.DictCursor
				)
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
			except Exception as e:
				print(e)
			finally:
				self._conn = None
		return len(trans["steal_data"])

	# Delete Steal Data
	def delete_data(
		self,
		trans,
		wait_printtime=1,
	):
		res_list = list()
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
					query = f"\"{trans['minhash']}\" < {self._hash_column} {trans['logical'].value} \"{trans['maxhash']}\" >= {self._hash_column}"
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
		host = trans["insert_ip"]
		self._conn = pymysql.connect(
			host=host,
			port=self._ipport[host],
			user=self._ipuser[host],
			password=self._ippass[host],
			database=self._database,
			cursorclass=pymysql.cursors.DictCursor
		)
		# Get fromgg data from not steal_ip
		# So, may duplicate data, delete insert_data from insert_ip
		if "steal_fake" in trans.keys() or self._require_reshard:
			for steal_ip in trans["steal_fake"].keys():
				query = f"\"{trans['minhash']}\" < {self._hash_column} {trans['logical'].value} \"{trans['maxhash']}\" >= {self._hash_column}"
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
		for node in self._mode_iphashs:
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
		print(f'MODE: {self._mode.value} DB: {self._database} TABLE: {self._table} HASH_COLUMN: {self._hash_column}')
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
					steal_len = self.steal_data(redo_trans)
					if steal_len == 0:
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
					pass
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
	def sid(self,update=True):
		self._check_trans()
		for trans in self._total_transaction:
			trans["complete"] = False
		
		self._init_contest_data()
		after_totalcount = copy.deepcopy(self._total_data_count)
		
		for trans in self._total_transaction:
			try:
				steal_len = self.steal_data(trans)
				if steal_len == 0:
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
#				if trans is self._total_transaction[-1]:
#					raise Exception

			except Exception as e:
				print(e)
				print(sys.exc_info())
				print(f"Occurred error !!!")
				print(f'\033[31mDB: {self._database} TABLE: {self._table} HASH_COLUMN: {self._hash_column}\033[0m')
				ipinfo = f'\033[31m{trans["steal_ip"]}=>{trans["insert_ip"]}\033[0m'
				hashinfo = f'\033[31m(\"{trans["minhash"]}\"~\"{trans["maxhash"]}\")\033[0m'
				print("%-33s%s"%(ipinfo,hashinfo))
				self._error_selection(trans)

		print("Before :")
		for node in self._mode_iphashs:
			print(f'\t{node["ip"]} => {self._total_data_count[node["ip"]]}')
		print("After :")
		for node in self._mode_iphashs:
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

		# Notification script for increment node
		if self._notice is not None:
			self._notice(*self._notice_args,**self._notice_kwargs)
		if update:
			self._update_yaml()

	def _update_yaml(self):
		new_iphashs = copy.deepcopy(self._new_iphashs)
		vnode_hash = dict()
		vnode_iphashs = list()
		for node in new_iphashs:
			vnode_hash[node["ip"]] = list()
			for vnode in self._virtual_iphashs:
				if node["ip"] == vnode["ip"]:
					vnode_hash[node["ip"]] += vnode["hash"]
			vnode_hash[node["ip"]].sort()

			vnode = dict()
			vnode["ip"] = node["ip"]
			vnode["hash"] = vnode_hash[node["ip"]]
			vnode["port"] = self._ipport[node["ip"]]
			if not self._secret:
				vnode["user"] = self._ipuser[node["ip"]]
				vnode["password"] = self._ippass[node["ip"]]
			vnode_iphashs.append(vnode)

		parse.update_yaml(self._yaml_path,vnode_iphashs)

	def work(self):
		print("MySQL Sharding Now.")
		if not hasattr(self,"_update"):
			raise ValueError("not found variable of bool whether update yaml after sharding.")
		self.sid(update=self._update)
