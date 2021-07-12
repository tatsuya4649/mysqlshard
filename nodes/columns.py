import datetime 

class ColumnsError(ValueError):
	pass
class ColumnsNotValue(ValueError):
	pass

class Columns:
	def __init__(self,*args):
		self._columns = [*args]
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
	# receive dict (key: column,value: column value)
	def convert(self,values):
		for key in values.keys():
			if not key in self._columns:
				raise ColumnsNotValue(f"{key} not found in {self._columns}")
		res_value = "("
		for i in range(len(self._columns)):
			value = values[self._columns[i]]
			if isinstance(value,str):
				value = f"\"{value}\""
			if isinstance(value,datetime.datetime):
				value = f"\"{value}\""
			if isinstance(value,datetime.time):
				value = f"\"{value}\""
			if isinstance(value,datetime.date):
				value = f"\"{value}\""
				
			res_value += f"{value}"
			if i < len(self._columns)-1:
				res_value += ", "
			else:
				res_value += ")"
		return res_value

		

if __name__ == "__main__":
	columns = Columns("id","username","hash_username","comment")
	print(str(columns))
