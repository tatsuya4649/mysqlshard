.PHONY: all clean

all: cluster.yaml ops.yaml

cluster.yaml:	cluster.yaml.in
	cpp -P -o $@ $<
ops.yaml:	ops.yaml.in
	cpp -P -o $@ $<
clean:
	rm cluster.yaml
	rm ops.yaml
