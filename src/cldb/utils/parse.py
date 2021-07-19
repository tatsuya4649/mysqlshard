import re

def parse_bool(string):
	if isinstance(string,bool):
		return string
	if re.match(r'^(TRUE|YES)$',string,flags=re.IGNORECASE):
		return True
	elif re.match(r'^(FALSE|NO)$',string,flags=re.IGNORECASE):
		return True
	else:
		return False
