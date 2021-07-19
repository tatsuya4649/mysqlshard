import inspect
import sys
from node import NodeMode

class NodeOperation:
	"""
	NodeOperation is a superclass of to treat target node.
	"""
	def __init__(self,ip,port,mode=NodeMode.ADD):
		self._ip = ip
		self._port = port
		self._mode = mode
	@property
	def ip(self):
		return self._ip
	@property
	def port(self):
		return self._port
	@property
	def mode(self):
		return self._mode
	def _delete_underline(self,string):
		return "".join(string.split("_")[1:])
	def __call__(self):
		_METHOD_INDEX=0
		allelems = dict()
		params = [ m for m in inspect.getmembers(self) \
			if hasattr(self,self._delete_underline(m[0]))
		]
		for param in params:
			allelems[self._delete_underline(param[0])] = param[1]
		return allelems


class MySQLOperation(NodeOperation):
	"""
	Optional parameters: option
		option => dict type.
	"""
	doclists = [NodeOperation.__doc__ if NodeOperation.__doc__ is not None else "",__doc__]
	__doc__ = "".join(doclists)
	def __init__(
		self,
		ip,
		port,
		mode,
		option,
	):
		super().__init__(ip,port,mode)
		self._option = option
	@property
	def option(self):
		return self._option

__all__ = [
	MySQLOperation.__name__,
	NodeOperation.__name__,
]
