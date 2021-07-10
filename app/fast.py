from fastapi import FastAPI,Form
from typing import Optional
import db

app = FastAPI()

_TABLE="user"

@app.get("/")
def get():
	return {"hello":"world"}

@app.post("/user")
def post_user(username:str=Form(...),comment:Optional[str]=Form(None)):
	if comment is None:
		scheme = "username"
		data_array = f"\"{username}\""
	else:
		scheme = "username,comment"
		data_array = f"\"{username}\",\"{comment}\""
	conn = db.connection()
	try:
		with conn:
			db.insert(conn,_TABLE,scheme,data_array)
		return {"result":"success","username":username,"comment":comment}
	except:
		return {"result":"failure","username":username,"comment":comment}
