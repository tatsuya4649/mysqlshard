import pymysql
import sys
import shutil

def pinger(f):
	terminal_size = shutil.get_terminal_size()
	terminal_columns = terminal_size.columns
	ping=" PING "
	length=len(ping)

	def _wrapper(*args,**kwargs):	
		for _ in range(int((terminal_columns-length)/2)):
			print("+",end='')
		print(ping,end='')
		for _ in range(int((terminal_columns-length)/2)):
			print("+",end='')
		if (terminal_columns-length)%2:
			print("+",end='')
		print("")
		f(*args,**kwargs)
		for _ in range(terminal_columns):
			print("+",end='')
		print("")
	return _wrapper

@pinger
def ping(
	ip,
	port,
	user,
	password,
	database
):
	print(f"PING TEST \"{database}\" of {ip}:{port}...")
	try:
		connection = pymysql.connect(
			host=ip,
			user=user,
			port=port,
			password=password,
			database=database,
			cursorclass=pymysql.cursors.DictCursor)
		with connection:
			connection.ping()
	except pymysql.Error as e:
		print(f"PING:({e.__class__.__name__}) {e}",file=sys.stderr)
		sys.exit(1)
	except pymysql.InterfaceError as e:
		print(f"PING:({e.__class__.__name__}) {e}",file=sys.stderr)
		sys.exit(1)
	except pymysql.OperationalError as e:
		print(f"PING:({e.__class__.__name__}) {e}",file=sys.stderr)
		sys.exit(1)
	except ValueError as e:
		print(f"PING:({e.__class__.__name__}) {e}",file=sys.stderr)
		sys.exit(1)
	except Exception as e:
		print(f"PING:({e.__class__.__name__}) {e}",file=sys.stderr)
		sys.exit(1)
	else:
		print(f"SUCCESS!!! \"{database}\" of {ip}:{port}")
		return
	
	
