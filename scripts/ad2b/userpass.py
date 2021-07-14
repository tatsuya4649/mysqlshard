
import sys
import pymysql
from getpass import getpass

def secret_userpass(
	ip,
	port,
	database,
	str=None
):
	if str is not None:
		print(str,file=sys.stderr)
	print(f"IP: {ip},PORT: {port}",file=sys.stderr)
	user = input("user: ")
	password = getpass("password: ")

	return {"user":user,"password":password}

