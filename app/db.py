import pymysql
import sys
import os

def connection():
	database = os.getenv("NODEDATABASE")
	node = os.getenv("NODEIP")
	port = int(os.getenv("NODEPORT"))
	if node is None or port is None:
		sys.exit(1)
	conn = pymysql.connect(
		host=node,
		port=port,
		user='root',
		password='mysql',
		database=database,
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
		print(e)
		raise e

def select_username(conn,table,username):
	try:
		with conn.cursor() as cursor:
			sql = f"SELECT * FROM {table} WHERE username=\"{username}\""
			print(sql)
			cursor.execute(sql)

			results = cursor.fetchall()
			if (len(results) != 1):
				raise Exception
			for r in results:
				return r
	except Exception as e:
		print(e)
		raise e

