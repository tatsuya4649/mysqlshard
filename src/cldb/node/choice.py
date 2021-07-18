import inquirer
from enum import Enum

def insert_ok():
	questions = [
		inquirer.List(
			"insert_ok",
			message=f"this results of sending sharding data to add new node. insert now? (ok?)",
			choices=["yes","no"],
			carousel=False,
		)
	]
	answer = inquirer.prompt(questions)
	if answer["insert_ok"] == "yes":
		return True
	else:
		print("warning: no data has been sent yet!!!")
		return False

def trans_ok():
	questions = [
		inquirer.List(
			"trans_ok",
			message=f"The data will be moved now,ok?",
			choices=["yes","no"],
			carousel=False,
		)
	]
	answer = inquirer.prompt(questions)
	if answer["trans_ok"] == "yes":
		return True
	else:
		print("warning: no data has been sent yet!!!")
		return False

class ErrorHandle(Enum):
	REDO = "Redo (Recommended)"
	CANCEL = "Cancel"
	CONTINUE = "Continue"

class UnknownErrorHandle(ValueError):
	pass 

def error_handle():
	questions = [
		inquirer.List(
			"error_handle",
			message=f"What should I do?",
			choices=[ErrorHandle.REDO.value,ErrorHandle.CANCEL.value,ErrorHandle.CONTINUE.value],
			carousel=False,
		)
	]
	answer = inquirer.prompt(questions)
	handle = answer["error_handle"]
	if handle == ErrorHandle.REDO.value:
		return ErrorHandle.REDO
	elif handle == ErrorHandle.CANCEL.value:
		return ErrorHandle.CANCEL
	elif handle == ErrorHandle.CONTINUE.value:
		return ErrorHandle.CONTINUE

class RedoErrorHandle(Enum):
	CANCEL = "Cancel"
	FILE = "Write data to file(err.data)"

def redo_errhandle():
	questions = [
		inquirer.List(
			"redo_errhandle",
			message=f"What should I do?",
			choices=[RedoErrorHandle.CANCEL.value,RedoErrorHandle.FILE.value],
			carousel=False,
		)
	]
	answer = inquirer.prompt(questions)
	handle = answer["redo_errhandle"]
	if handle == RedoErrorHandle.CANCEL.value:
		return RedoErrorHandle.CANCEL
	elif handle == RedoErrorHandle.FILE.value:
		return RedoErrorHandle.FILE


def really_cancel(steal,insert):
	questions = [
		inquirer.List(
			"really_cancel",
			message=f"Really? some data will be gone",
			choices=["Yes","No"],
			carousel=False,
		)
	]
	answer = inquirer.prompt(questions)
	if answer["really_cancel"] == "Yes":
		return True
	else:
		print(f"WARNING: There may be Inconsistency data in {steal},{insert}!!!")
		return False
	
def really_continue(steal,insert):
	questions = [
		inquirer.List(
			"really_continue",
			message=f"Really? some data will be gone",
			choices=["Yes","No"],
			carousel=False,
		)
	]
	answer = inquirer.prompt(questions)
	if answer["really_continue"] == "Yes":
		return True
	else:
		print(f"WARNING: There are Inconsistency data in {steal},{insert}!!!")
		return False

def insert_retry(ip,err_count):
	questions = [
		inquirer.List(
			"insert_retry",
			message=f"Detect {err_count}Times Failure in inserting data to new added node {ip}(Retry?)",
			choices=["Yes","No"],
			carousel=False,
		)
	]
	answer = inquirer.prompt(questions)
	if answer["insert_retry"] == "Yes":
		return True
	else:
		print("WARNING: There are Inconsistency data in added node!!!")
		return False
def insert_redo():
	questions = [
		inquirer.List(
			"insert_redo",
			message="Detect Unmatch in insert data and steal data. Delete insert data from new add node?(Redo?)",
			choices=["Yes","No"],
			carousel=True,
		)
	]
	answer = inquirer.prompt(questions)
	if answer["insert_redo"] == "Yes":
		return True
	else:
		print("WARNING: There are Inconsistency data in added node!!!")
		return False

def delete_data(ip):
	questions = [
		inquirer.List(
			"delete",
			message="Insert Complete. Delete from existing node.(Delete?)",
			choices=["Yes","No"],
			carousel=True,
		)
	]
	answer = inquirer.prompt(questions)
	if answer["delete"] == "Yes":
		return True
	else:
		print("NOTICE: There are Consistency data in existed node!!!")
		return False

if __name__ == "__main__":
	res = error_handle()
	print(res)