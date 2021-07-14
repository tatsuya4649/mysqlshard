import add_node
import argparse
import columns as clms

def main():
	parser = argparse.ArgumentParser(description="Sharding Database")
	parser.add_argument("ip",help="IP address of added node")
	parser.add_argument("port",help="Port number of added node",type=int)
	parser.add_argument("yaml_path",help="YAML file path of existing node info")
	parser.add_argument("database",help="Sharding Database name")
	parser.add_argument("table",help="Sharding Table name")
	parser.add_argument("notice_script",help="When Success,execute script file")
	parser.add_argument("notice_args",help="When Success,execute script file arguments",nargs="*")

	args = parser.parse_args()

