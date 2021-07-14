import datetime 
from enum import Enum

class ColumnsError(ValueError):
	pass
class ColumnsNotValue(ValueError):
	pass

class ColumnType(Enum):
	pass

def _column_deco(func):
	def _wrapper(*args,**kwargs):
		func(*args,**kwargs)
	return _wrapper

class Columns:
	def __init__(self,**kwargs):
		self._port = kwargs["port"]
		self._ip = kwargs["ip"]
		self._database = kwargs["database"]
		self._table = kwargs["table"]
		self._user = kwargs["user"]
		self._password = kwargs["password"]

		self._columns = list()
		self._columns_type = dict()
		
		if "show_column" in kwargs.keys():
			self._show_column = True
		else:
			self._show_column = False

		self._get_columns()
	def __iter__(self):
		self._counter = 0
		return self
	def __next__(self):
		if len(self._columns) == self._counter:
			raise StopIteration
		column = self._columns[self._counter]
		self._counter += 1
		return column
	def __str__(self):
		string = "("
		for j in range(len(self._columns)):
			string += self._columns[j]
			if j == len(self._columns)-1:
				string += ")"
			else:
				string += ", "
		return string
	def _get_columns(self):
		raise NotImplementedError("must connect database,and get describe of columns")
	# receive dict (key: column,value: column value)
	def convert(self,values):
		raise NotImplemetendError("query value by column")


import pymysql
import re
class MySQLNUMERIC(Enum):
	# Numeric Type
	TINYINT		= 1
	SMALLINT	= 2
	MEDIUMINT	= 3
	INT		= 4
	BIGINT		= 5
	FLOAT		= 6
	DOUBLE		= 7
	DECIMAL		= 8
class MySQLSTRING(Enum):
	# String Type
	CHAR		= 1
	VARCHAR		= 2
	TINYBLOB	= 3
	BLOB		= 4
	MEDIUMBLOB	= 5
	LONGBLOB	= 6
	TINYTEXT	= 7
	TEXT	 	= 8
	MEDIUMTEXT	= 9
	LONGTEXT 	= 10
	ENUM		= 11
	SET		= 12
class MySQLDATE(Enum):
	# Date Type
	DATE		= 1
	TIME		= 2
	DATETIME 	= 3
	TIMESTAMP	= 4
	YEAR		= 5

class MySQLColumnType(ColumnType):
	NUMERIC = MySQLNUMERIC
	STRING = MySQLSTRING
	DATE = MySQLDATE

class MySQLUnknownColumnType(ValueError):
	pass

class MySQLColumns(Columns):
	def __init__(self,**kwargs):
		super().__init__(**kwargs)
		self._desc = list()
	def _get_columns(self):
		try:
			conn = pymysql.connect(
				host=self._ip,
				port=self._port,
				user=self._user,
				password=self._password,
				database=self._database,
				cursorclass=pymysql.cursors.DictCursor
			)
			with conn:
				with conn.cursor() as cursor:
					sql = f"DESCRIBE {self._table}"
					cursor.execute(sql)

					self._desc = cursor.fetchall()
					self._add_columns()

					self._display_columns()
		except Exception as e:
			raise
	@_column_deco
	def _display_columns(self):
		if self._show_column:
			print(self)	
	def _add_columns(self):
		for column in self._describe:
			field = column["Field"]
			type = column["Type"]
			null = column["Null"]
			key = column["Key"]
			default = column["Default"]
			extra = column["Extra"]
			self._columns.append(field)
			self._columns_type[field] = self._specify_type(type)
	def _specify_type(self,type_string):
		# NUMERIC
		if re.match(r'^tinyint',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.NUMERIC.value.TINYINT
		elif re.match(r'^smallint',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.NUMERIC.value.SMALLINT
		elif re.match(r'^mediumint',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.NUMERIC.value.MEDIUMINT
		elif re.match(r'^int',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.NUMERIC.value.INT
		elif re.match(r'^bigint',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.NUMERIC.value.BIGINT
		elif re.match(r'^float',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.NUMERIC.value.FLOAT
		elif re.match(r'^double',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.NUMERIC.value.DOUBLE
		elif re.match(r'^decimal',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.NUMERIC.value.DECIMAL
		# STRING
		elif re.match(r'^char',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.STRING.value.CHAR
		elif re.match(r'^varchar',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.STRING.value.VARCHAR
		elif re.match(r'^tinyblob',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.STRING.value.TINYBLOB
		elif re.match(r'^blob',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.STRING.value.BLOB
		elif re.match(r'^mediumblob',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.STRING.value.MEDIUMBLOB
		elif re.match(r'^longblob',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.STRING.value.LONGBLOB
		elif re.match(r'^tinytext',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.STRING.value.TINYTEXT
		elif re.match(r'^text',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.STRING.value.TEXT
		elif re.match(r'^mediumtext',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.STRING.value.MEDIUMTEXT
		elif re.match(r'^longtext',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.STRING.value.LONGTEXT
		elif re.match(r'^enum',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.STRING.value.ENUM
		elif re.match(r'^set',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.STRING.value.SET
		# DATE
		elif re.match(r'^date',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.DATE.value.DATE
		elif re.match(r'^time',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.DATE.value.TIME
		elif re.match(r'^datetime',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.DATE.value.DATETIME
		elif re.match(r'^timestamp',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.DATE.value.TIMESTAMP
		elif re.match(r'^year',type_string,flags=re.IGNORECASE) is not None:
			return MySQLColumnType.DATE.value.YEAR
		else:
			raise MySQLUnknownColumnType("must be valid type.")

	# receive dict (key: column,value: column value)
	def convert(self,values):
		for key in values.keys():
			if not key in self._columns:
				raise ColumnsNotValue(f"{key} not found in {self._columns}")
		res_value = "("
		for i in range(len(self._columns)):
			value = values[self._columns[i]]
			type = self._column_type[self._columns[i]]

			if value is None:
				value = "NULL"
			elif type not in MySQLColumnType.MySQLNUMERIC.value:
				value = f"\"{value}\""
			res_value += f"{value}"
			if i < len(self._columns)-1:
				res_value += ", "
			else:
				res_value += ")"
		return res_value

	@property
	def _describe(self):
		return self._desc

if __name__ == "__main__":
	columns = MySQLColumns(
		ip="172.27.0.1",
		port=13306,
		user="root",
		password="mysql",
		database="sharding",
		table="user",
		show_column=True
	)
