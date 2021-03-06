import sys
import os
import re
from cluster import *
import argparse

def _type(ope):
	if isinstance(ope,MySQLOperation):
		return MySQLCluster
	else:
		print(f"Operation type is invalid.",file=sys.stderr)

def _call_cluster(cluster_info,ops,ops_yaml):
	if len(ops) == 0:
		print(f"Operation must be one or more",file=sys.stderr)
	first_ope = ops[0]
	cluster_type = _type(first_ope)
	cluster = cluster_type(cluster_info,ops,ops_yaml)
	cluster.operate()

def with_ops_yaml(ops_yaml):
	if not os.path.exists(ops_yaml):
		print(f"Operations File(\"{ops_yaml}\") not found...",file=sys.stderr)
		sys.exit(1)
	if re.match(r'^{YML|YAML}$',ops_yaml.split(".")[-1],flags=re.IGNORECASE) is not None:
		print(f"Operations File(\"{ops_yaml}\") not YAML File.(Hints: should verify extensions)",file=sys.stderr)
		sys.exit(1)
	cluster_info,ops = yaml_to_ops(ops_yaml)
	_call_cluster(cluster_info,ops,ops_yaml)
def without_ops_yaml(parser,args):
	if args.ip is None:
		parser.print_help()
		print("\nThere is no IP address of target node.",file=sys.stderr)
		sys.exit(1)
	if args.port is None:
		parser.print_help()
		print("\nThere is no PORT number of target node.",file=sys.stderr)
		sys.exit(1)
	if args.mode is None:
		parser.print_help()
		print("\nThere is no MODE (how to treat target node).",file=sys.stderr)
		sys.exit(1)

	mode = [v for n,v in NodeMode.__members__.items() if v.value == args.mode]
	if len(mode) != 1:
		print("\nMODE parameter is invalid.",file=sys.stderr)
		sys.exit(1)
	mode = mode[0]
	
	operate = {
		"ip":args.ip,
		"port":args.port,
		"mode": mode,
	}

	cluster_info = dict()
	if args.database is None:
		paeser.print_help()
		print("\nThere is no database name.",file=sys.stderr)
		sys.exit()
	cluster_info["database"] = args.database
	if args.table is None:
		paeser.print_help()
		print("\nThere is no table name.",file=sys.stderr)
		sys.exit()
	cluster_info["table"] = args.table
	if args.hash_column is None:
		paeser.print_help()
		print("\nThere is no hash column name.",file=sys.stderr)
		sys.exit()
	cluster_info["hash_column"] = args.hash_column
	if args.cluster_yaml is None:
		paeser.print_help()
		print("\nThere is no cluster yame path.",file=sys.stderr)
		sys.exit()
	cluster_info["cluster_yaml"] = args.cluster_yaml
	if args.virtual_nodecount is None:
		paeser.print_help()
		print("\nThere is no virtual node count.",file=sys.stderr)
		sys.exit()
	cluster_info["virtual_nodecount"] = args.virtual_nodecount
	_call_cluster(cluster_info,[operate],None)

def main():
	parser = argparse.ArgumentParser(description="Sharding Database")
	parser.add_argument("--ip",help="IP address of added node",type=str,required=False)
	parser.add_argument("--port",help="Port number of added node",type=int,required=False)
	parser.add_argument("-u","--user",help="Database user of added node",type=str)
	parser.add_argument("-p","--password",help="Database password of added node",type=str)
	parser.add_argument("-c","--hash_column",help="Column name of having HashID",type=str,required=False)
	parser.add_argument("-y","--cluster_yaml",help="YAML file path of existing node info",type=str,required=False)
	parser.add_argument("-o","--ops_yaml",help="YAML file path of cluster operations",type=str)
	parser.add_argument("-d","--db",help="Sharding Database name",type=str,required=False)
	parser.add_argument("-t","--table",help="Sharding Table name",type=str,required=False)
	parser.add_argument("-m","--mode",help="How target node is treated?",choices=["add","delete"],required=False)
	parser.add_argument("-s","--notice_script",help="When Success,execute script file")
	parser.add_argument("-a","--notice_args",help="When Success,execute script file arguments",nargs="*")
	parser.add_argument("-v","--virtual_nodecount",help="If you want the data to be more even,set it to a higher value(default: 100)",type=int,default=100)
	parser.add_argument("--secret",help="No use of yaml user,password, use interactive input",action="store_true")
	parser.add_argument("--secret_once",help="If Once input user/password, it use at all database user/password ",action="store_true")
	parser.add_argument("--ping_interval",help="PING test interval time",type=int,default=0)
	parser.add_argument("--non_update",help="YAML non update",action="store_false")
	parser.add_argument("--non_notice",help="No execute notice script",action="store_false")
	parser.add_argument("-n","--non_reshard",help="If required to resharding,this flag should be on(ex.change virtual node count)",action="store_false")

	args = parser.parse_args()
	
	if args.notice_args is None:
		args.notice_args = []

	if args.ops_yaml:
		with_ops_yaml(args.ops_yaml)
	else:
		without_ops_yaml(parser,args)
