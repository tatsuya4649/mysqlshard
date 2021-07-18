import binascii
import test

def mod(nodes,data):
	crc32 = binascii.crc32(data.encode('utf-8'))
	index = crc32 % len(nodes)
	return nodes[index]

if __name__ == "__main__":
	test = test.TestNode(node_count=10)
	data = "124"
	print("========= mod ===========")
	test.now_nodes()
	test.now_datas()
	test.test(mod,test.nodes,test.datas)
	print("========= add node ===========")
	test.append()
	test.test(mod,test.nodes,test.datas)
	print("========= delete node ===========")
	test.delete()
	test.test(mod,test.nodes,test.datas)
