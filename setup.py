from setuptools import setup

with open("./requirements.txt") as f:
	install_requires = f.read().splitlines()
	print(install_requires)

setup(
	install_requires=install_requires
)
