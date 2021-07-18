import string
import random
import requests
import hashlib

def post_request(url,username,comment):
	hash_username = hashlib.md5(username.encode("utf-8")).hexdigest()
	response = requests.post(
		url,
		data={"username":username,"hash_username": hash_username,"comment":comment}
	)
	print("request:")
	print(f"username:{username},hash_username:{hash_username},comment:{comment}")
	print("response header:")
	print(response.headers)
	print("response content:")
	print(response.content)

# Generate random comment(0~count)
def random_comment(count=10):
	result = ""
	for _ in range(random.randint(0,count)):
                result += random.choice(string.ascii_letters + string.digits)
	return result

# Generate random string
def random_string(count=10):
        result = ""
        for _ in range(count):
                result += random.choice(string.ascii_letters + string.digits)
        return result


if __name__ == "__main__":
	_URL="http://172.17.0.1:48080/user"
	for _ in range(100):
		post_request(_URL,random_string(),random_comment(count=100))
