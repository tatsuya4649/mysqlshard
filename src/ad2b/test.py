import hashlib
import ip as ipm
import pymysql

class ConsistencyError(Exception):
	pass
class ConsistencyInsertError(Exception):
	def __init__(self,str,err_count):
		super().__init__(str)
		self.err_count = err_count
class ConsistencyDeleteError(Exception):
	def __init__(self,str,err_count):
		super().__init__(str)
		self.err_count = err_count
class ConsistencyUnmatchError(Exception):
	pass
class ConsistencyYetError(Exception):
	pass
class IPRegularExpressionError(ValueError):
	pass

class Consistency:
	"""
	 Consistency Test
	 	Check before and after data consistency
		
		# database: test database name
		# table: test table name
	"""
	def __init__(self,addip,database,table):
		self._addip = addip
		self._database = database
		self._table = table

		self._steal = False
		self._insert = False
		self._delete = False

		self._insert_dataset = dict()
		self._steal_dataset = dict()
		self._delete_dataset = dict()

#	@property
#	def ipquery(self):
#		return self._ipquery
	@property
	def steal_len(self):
		raise NotImplementedError("test")
	@property
	def insert_len(self):
		raise NotImplementedError("test")
	@property
	def delete_len(self):
		raise NotImplementedError("test")

#	def query(self,ip):
#		if ipm.ip_check(ip):
#			return self._ipquery[ip]
#		else:
#			raise IPRegularExpressionError(f"invalid \"{ip}\"")
	def contest(self):
		raise NotImplementedError("test")
	def contest_insert(self):
		raise NotImplementedError("test")
	def connect(self):
		raise NotImplementedError("connect to database")
	def select(self,query):
		raise NotImplementedError("select data from table of database")
	def steal(self):
		self._steal = True
	def insert(self):
		self._insert = True
	def delete(self):
		self._delete = True
	def _check_i(self):
		if self._insert is False:
			raise consistencyyeterror("yes called insert data function")
	def _check_si(self):
		if self._steal is False:
			raise ConsistencyYetError("yet called steal data function")
		if self._insert is False:
			raise consistencyyeterror("yes called insert data function")

class MySQLConsistency(Consistency):
	def __init__(self,addip,database,table,port=3306,user="root",password="mysql"):
		super().__init__(addip,database,table)
		self._port = port
		self._user = user
		self._password = password

	def connect(self,ip,port,user,password,charset="utf8"):
		connection = pymysql.connect(
			host=ip,
			user=user,
			password=password,
			db=self._database,
			cursorclass=pymysql.cursors.DictCursor
		)
		return connection
	def select(self,connection,query):
		with connection:
			with connection.cursor() as cursor:
				sql = f"{query}"
				cursor.execute(sql)
				results = cursor.fetchall()
				return results
	"""
		consistency test 3.step
		1. Get query from add node
		2. Insert all columns into set()
		3. Get query from stolen node
		4. Check Consistency step.2 set() and step.3 set()
		5. return True or False
	"""
	def contest(self,ipquery):
		self._check_si()
		connect = self.connect(self._addip,self._port,self._user,self._password)
		column_setdict = dict()

		for key in self.ipquery.keys():
			results = self.select(connect,ipquery[key])
			for reskey in results[0].keys():
				column_setdict[reskey] = set()
			for res in results:
				for column in res.keys():
					column_setdict[column].add(res[column])
		
		exists_column_setdict = dict()
		for column in column_setdict.keys():
			exists_column_setdict[column] = set()
		for key in self.ipquery.keys():
			connect = self.connect(key,self._port,self._user,self._password)
			results = self.select(connect,ipquery[key])
			for res in results:
				for column in res.keys():
					exists_column_setdict[column].add(res[column])

		for column in column_setdict.keys():
			if column_setdict[column] == exists_column_setdict[column]:
				continue
			else:
				return False
		return True

if __name__ == "__main__":
	ipquery = dict()
	testid = hashlib.md5("test".encode("utf-8")).hexdigest()
	addip = "172.17.0.2"
	ip = "43.23.4.2"
	ipquery[ip] = "select * from user where hash_username > \"d169243a5f9258169e6368a4fba769c8\" AND \"d2825baf19871782307154233574114f\" > hash_username" 
	con = MySQLConsistency(addip,ipquery,"sharding","user",13306)
	result = con.contest()
