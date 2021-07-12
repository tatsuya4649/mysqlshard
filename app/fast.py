from fastapi import FastAPI,Form
from typing import Optional
import db
import hashlib

app = FastAPI()

_TABLE="user"

@app.get("/")
def get():
	return {"hello":"world"}

@app.get("/user/{username}")
def get_user(username:str):
	conn = db.connection()
	try:
		with conn:
			user=db.select_username(conn,"user",username)
		return {"result":"success","user":user}
	except Exception as e:
		print(e)
		return {"result":"failure"}


@app.post("/user")
def post_user(username:str=Form(...),comment:Optional[str]=Form(None)):
	hash_username = hashlib.md5(username.encode("utf-8")).hexdigest()
	if comment is None:
		scheme = "username,hash_username"
		data_array = f"\"{username}\",\"{hash_username}\""
	else:
		scheme = "username,hash_username,comment"
		data_array = f"\"{username}\",\"{hash_username}\",\"{comment}\""
	conn = db.connection()
	try:
		with conn:
			db.insert(conn,_TABLE,scheme,data_array)
		return {"result":"success","username":username,"hash_username":hash_username,"comment":comment}
	except Exception as e:
		print(e)
		return {"result":"failure","username":username,"hash_username":hash_username,"comment":comment}
