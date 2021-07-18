import yaml
from .nodeope import NodeOperation,MySQLOperation
from .err import ClusterOpsFileError,ClusterOpsTypeError,ClusterOperationError
from .type import OperationType

def yaml_to_ops(path):
	"""
	yaml_to_ops function is YAML => NodeOperations List.

	format
	---

	type: mysql
	database: example
	table: user
	hash_column: hash_username
	ops:
	- ip: "127.0.0.1"
	  port: 3306
	  mode: add
	- ip: "23.123.32.1"
	  port: 3306
	  mode: add
	"""
	print(path)
	with open(path,"r") as f:
		obj = yaml.safe_load(f)
	if obj is None:
		raise ClusterOpsFileError("YAML file is empty.")
	_check_type(obj)

	_check_database(obj)
	_check_table(obj)
	_check_hash_column(obj)

	type = obj["type"]
	if type == OperationType.MYSQL.value:
		type = OperationType.MYSQL
	else:
		raise ClusterTypeError("invalid operation type.")

	_check_ops(obj)

	lists = list()
	for ops in obj["ops"]:
		_check_operation(ops)
		ip,port,mode = _get_operation(ops)
		if type is OperationType.MYSQL:
			operation = MySQLOperation(ip=ip,port=port,mode=mode)
		else:
			raise ClusterTypeError("invalid operation type.")
		lists.append(operation)
	
	if len(lists) == 0:
		raise ClusterOperationError("no operation.")
	cluster_info = {
		"database": obj["database"],
		"table": obj["table"],
		"hash_column": obj["hash_column"],
	}
	return cluster_info,lists



def _check_type(obj):
	if "type" not in obj.keys() and obj["type"] not in [v.value for _,v in OperationType.__members__.items()]:
		raise ClusterOpsTypeError(f'\"type\" is invalid value.')
def _check_database(obj):
	if "database" not in obj.keys() or obj["database"] is None:
		raise ClusterOpsTypeError(f'\"database\" is empty.')
def _check_hash_column(obj):
	if "hash_column" not in obj.keys() or obj["hash_column"] is None:
		raise ClusterOpsTypeError(f'\"hash_column\" is empty.')
	
def _check_table(obj):
	if "table" not in obj.keys() or obj["table"] is None:
		raise ClusterOpsTypeError(f'\"table\" is empty.')
def _check_ops(obj):
	if "ops" not in obj.keys():
		raise ClusterOpsYAMLError('must have \"ops\" list in YAML')
def _check_operation(ops):
	if "ip" not in ops.keys():
		raise ClusterOpsYAMLError('must have \"ip\" in \"ops\"')
	if "port" not in ops.keys():
		raise ClusterOpsYAMLError('must have \"port\" in \"ops\"')
	if "mode" not in ops.keys():
		raise ClusterOpsYAMLError('must have \"mode\" in \"ops\"')

def _get_operation(ops):
	return ops["ip"],ops["port"],ops["mode"]

__all__ = [
	yaml_to_ops.__name__
]

