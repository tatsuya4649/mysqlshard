import sys
import add_node
import argparse

def main():
	parser = argparse.ArgumentParser(description="Sharding Database")
	parser.add_argument("--ip",help="IP address of added node",type=str,required=True)
	parser.add_argument("--port",help="Port number of added node",type=int,required=True)
	parser.add_argument("-u","--user",help="Database user of added node",type=str)
	parser.add_argument("-p","--password",help="Database password of added node",type=str)
	parser.add_argument("-c","--hash_column",help="Column name of having HashID",type=str,required=True)
	parser.add_argument("-y","--yaml_path",help="YAML file path of existing node info",type=str,required=True)
	parser.add_argument("-d","--db",help="Sharding Database name",type=str,required=True)
	parser.add_argument("-t","--table",help="Sharding Table name",type=str,required=True)
	parser.add_argument("-s","--notice_script",help="When Success,execute script file")
	parser.add_argument("-a","--notice_args",help="When Success,execute script file arguments",nargs="*")
	parser.add_argument("--secret",help="No use of yaml user,password, use interactive input",action="store_true")
	parser.add_argument("--secret_once",help="If Once input user/password, it use at all database user/password ",action="store_true")
	parser.add_argument("--ping_interval",help="PING test interval time",type=int,default=1)
	parser.add_argument("--non_update",help="YAML non update",action="store_false")
	parser.add_argument("--non_notice",help="No execute notice script",action="store_false")

	args = parser.parse_args()
	
	if args.notice_args is None:
		args.notice_args = []

	a2node = add_node.MySQLAddNode(
		ip = args.ip,
		port = args.port,
		hash_column = args.hash_column,
		database = args.db,
		table = args.table,
		yaml_path = args.yaml_path,
		funcpath = args.notice_script,
		notice_args = args.notice_args,
		user = args.user,
		password = args.password,
		secret = args.secret,
		secret_once = args.secret_once,
		ping_interval = args.ping_interval,
	)
	a2node.sid(script=args.non_notice,update=args.non_update)
