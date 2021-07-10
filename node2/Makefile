Dockerfile: Dockerfile.in ../*.docker
	cpp -P -o Dockerfile Dockerfile.in

build:	Dockerfile
	docker build build --no-cache -t node2 .

clean:
	rm -rf Dockerfile
