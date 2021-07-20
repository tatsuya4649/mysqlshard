from node import NodeMode,NodeWorker
from .err import *
from .nodeope import *
from .parse import *
from node import *
import inspect
import sys
import yaml
import copy
from utils.parse import parse_bool

class Cluster:
	"""
	required:
		_operation_lists: for node operations
		[NodeOperation,...]
		cluster_info: "database","table","hash_column", etc...
	"""
	_CLUSTER_DATA_PATH="numdata.yaml"
	def __init__(self,cluster_info,ops_path=None):
		self._opelen = 0
		self._operate = None
		self._ops_path = ops_path
		self._cluster_info = cluster_info
		self._database = cluster_info["database"]
		self._table = cluster_info["table"]
		self._hash_column = cluster_info["hash_column"]
		self._cluster_yaml = cluster_info["cluster_yaml"]
		self._virtual_nodecount = cluster_info["virtual_nodecount"]
		self._cluster_update = cluster_info["cluster_update"]
		self._create_data_counter = parse_bool(cluster_info["create_counter_yaml"] if "create_counter_yaml" in cluster_info.keys() else False)
		self._create_data_path = cluster_info["create_data_path"] if "create_data_path" in cluster_info.keys() else self._CLUSTER_DATA_PATH
	# check have a operation lists
	def _require_operation_lists(func):
		def _check(self,*args,**kwargs):
			if not hasattr(self,"_operation_lists"):
				raise ClusterAttributeError("must have _operation_lists")
			if not isinstance(self._operation_lists,list):
				raise ClusterAttributeError("_operation_lists type must be list")
			for ele in self._operation_lists:
				if not isinstance(ele,NodeOperation):
					raise ClusterAttributeError("_operation_lists'element type must be list")
			func(self,*args,**kwargs)
		return _check
	def _require_worker(func):
		def _check(self,*args,**kwargs):
			if not hasattr(self,"_worker"):
				raise ClusterAttributeError("must have _worker")
			if len([x for x in inspect.getmro(self._worker) if x is NodeWorker]) == 0:
				raise ClusterTypeError("worker must be NodeWorker type.")
			func(self,*args,**kwargs)
		return _check
	@_require_worker
	def _work(self,operate):
		self._operate = operate

		params = self._operate()
		work_ins = self._worker(self._cluster_info,params)
		# get number of data by node
		total_node_counter = work_ins.work()
		if self._create_data_counter:
			with open(self._create_data_path,"w") as yf:
				yaml.dump(total_node_counter,yf,default_flow_style=False)

		self._operate = None
	
	@_require_worker
	def _only_update_cluster(self,operate):
		self._operate = operate

		params = self._operate()
		work_ins = self._worker(self._cluster_info,params)
		update_cluster = work_ins.update_cluster()
		if self._cluster_update:
			update_cluster_yaml(self._cluster_yaml,update_cluster)
		self._operate = None

	# required: 
	#   * _operation_lists: this is cluster operation dict lists
	@_require_operation_lists
	def operate(self):
		ops_yaml = copy.deepcopy(self._cluster_info)
		now_operations = copy.deepcopy(self._operation_lists)
		ops_yaml["ops"] = [ i() for i in now_operations ]
		for operate in self._operation_lists:
			index = self._operation_lists.index(operate)
			# moving data only last
			if len(self._operation_lists) > 1 and self._operation_lists[index+1] != self._operation_lists[-1]:
				self._only_update_cluster(operate)
			else:
				self._work(operate)

			ops_yaml["ops"].remove(operate())
			if self._ops_path is not None:
				with open(self._ops_path,"w") as yf:
					yaml.dump(ops_yaml,yf,default_flow_style=False)
	@property
	@_require_operation_lists
	def operation_count(self):
		return self._opelen
	@property
	def database(self):
		return self._database
	@property
	def table(self):
		return self._table
	@property
	def hash_column(self):
		return self._hash_column

class MySQLClusterTypeError(ClusterTypeError):
	pass

class MySQLCluster(Cluster):
	def __init__(
		self,
		cluster_info,
		operations,
		ops_path,
	):
		operations = self._operations_check(operations)
		if len(operations) != 0:
			operations[-1].option["require_reshard"] = True
		super().__init__(cluster_info,ops_path)
		self._operation_lists = operations
		self._worker = MySQLWorker

	def _operations_check(self,operations):
		if isinstance(operations,MySQLOperation):
			operations = [operations]
		if not isinstance(operations,list):
			raise MySQLClusterTypeError("operations must be list.")
		for ope in operations:
			if not isinstance(ope,MySQLOperation):
				raise MySQLClusterTypeError("operations element must be MySQLOperation.")
		return operations

__all__ = [
	MySQLCluster.__name__,
	NodeMode.__name__,
	NodeWorker.__name__,
	MySQLWorker.__name__,
]
