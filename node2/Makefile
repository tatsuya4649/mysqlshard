NODE:=node2

Dockerfile: Dockerfile.in ../*.docker
	cpp -P -o Dockerfile Dockerfile.in

clean:
	rm Dockerfile
