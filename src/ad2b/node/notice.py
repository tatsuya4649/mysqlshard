from enum import Enum
import subprocess

class Way(Enum):
	FUNC = "function"
	SCRIPT = "script"

class NoticeArgsError(Exception):
	pass

class Notice:
	def __init__(self,notice):
		# Check scripts or function
		if callable(notice):
			self._way = Way.FUNC
		if isinstance(notice,str):
			self._way = Way.SCRIPT
		self._notice = notice
	# execute
	def __call__(self,*notice_args,**notice_kwargs):
		if self._way == Way.FUNC:
			self._notice(*notice_args,**notice_kwargs)
		elif self._way == Way.SCRIPT:
			if len(notice_kwargs.keys()) != 0:
				raise NoticeArgsError("kwargs is invalid in script.")
			command = list()
			command.append(self._notice)
			if len(notice_args) != 0:
				print(notice_args)
				command.append(*notice_args)
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
			notice = Notice(hello)
			notice(*self._hello_args)
	
	args = ["hello","world"]
	a = A(args)
	a.notice()
