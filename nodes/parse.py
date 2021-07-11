import yaml
from algo import con

if __name__ == "__main__":
	with open("ip.yaml","r") as f:
		obj = yaml.safe_load(f)
	ips = obj["ip"]
	print(ips)
	data = "iwaikanami"
	ip = con.consistent_hasing(ips,data)
	print(ip)
