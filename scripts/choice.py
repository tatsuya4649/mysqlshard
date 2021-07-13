import inquirer

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
