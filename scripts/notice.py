from enum import Enum
import subprocess

class Way(Enum):
	FUNC = "function"
	SCRIPT = "script"

class NoticeArgsError(Exception):
	pass

class Notice:
	def __init__(self,notice,*notice_args,**notice_kwargs):
		# Check scripts or function
		if callable(notice):
			self._way = Way.FUNC
		if isinstance(notice,str):
			self._way = Way.SCRIPT
			if len(notice_kwargs.keys()) != 0:
				raise NoticeArgsError("kwargs is invalid in script.")
		self._notice = notice
		self._notice_args = notice_args
		self._notice_kwargs = notice_kwargs
	# execute
	def __call__(self):
		if self._way == Way.FUNC:
			self._notice(*self._notice_args,**self._notice_kwargs)
		elif self._way == Way.SCRIPT:
			command = list()
			command.append(self._notice)
			if len(self._notice_args) != 0:
				print(self._notice_args)
				command.append(*self._notice_args)
			print(command)
			subprocess.run(command)
			print(f"{self._notice}")
	@property
	def way(self):
		return self._way.value

if __name__ == "__main__":
	def hello(*test):
		print(*test)
	
	class A:
		def __init__(self,hello_args):
			self._hello_args = hello_args
		def notice(self):
			notice = Notice(hello,*self._hello_args)
			notice()
	
	args = ["hello","world"]
	a = A(args)
	a.notice()
