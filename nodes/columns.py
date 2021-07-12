

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
		

if __name__ == "__main__":
	columns = Columns("id","username","hash_username","comment")
	for i in columns:
		print(i)
