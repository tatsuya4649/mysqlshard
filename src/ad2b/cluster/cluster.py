from node import NodeMode,NodeWorker
from .err import *
from .nodeope import *
import inspect

class Cluster:
	"""
		required:
			_operation_lists: for node operations
			[NodeOperation,...]
			cluster_info: "database","table","hash_column", etc...
	"""
	def __init__(self,cluster_info):
		self._opelen = 0
		self._operate = None
		self._cluster_info = cluster_info
		self._database = cluster_info["database"]
		self._table = cluster_info["table"]
		self._hash_column = cluster_info["hash_column"]
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
		work_ins.work()

		self._operate = None

	# required: 
	#   * _operation_lists: this is cluster operation dict lists
	@_require_operation_lists
	def operate(self):
		for operate in self._operation_lists:
			self._work(operate)
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
	):
		operations = self._operations_check(operations)
		super().__init__(cluster_info)
		self._operation_lists = operations
		self._worker = NodeWorker

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
]
