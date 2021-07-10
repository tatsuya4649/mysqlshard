import pymysql
import sys
import os

def connection():
	node = os.getenv("NODEIP")
	port = int(os.getenv("NODEPORT"))
	if node is None or port is None:
		sys.exit(1)
	conn = pymysql.connect(
		host=node,
		port=port,
		user='root',
		password='mysql',
		cursorclass=pymysql.cursors.DictCursor
	)
	return conn


def insert(conn,table,scheme,data_array):
	try:
		with conn.cursor() as cursor:
			sql = f"INSERT INTO {table} ({scheme}) VALUES ({data_array})"
			conn.begin()
			cursor.execute(sql)
			conn.commit()
	except Exception as e:
		raise Exception
