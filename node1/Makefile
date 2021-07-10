Dockerfile: Dockerfile.in ../*.docker
	cpp -P -o Dockerfile Dockerfile.in

build: Dockerfile
	docker build --no-cache -t node1 .

clean:
	rm -rf Dockerfile
