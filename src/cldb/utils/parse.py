import re

def parse_bool(string):
	if isinstance(string,bool):
		return string
	elif not isinstance(string,string):
		return False
	if re.match(r'^(TRUE|YES)$',string,flags=re.IGNORECASE):
		return True
	elif re.match(r'^(FALSE|NO)$',string,flags=re.IGNORECASE):
		return False
	else:
		return False
