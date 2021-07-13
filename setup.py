from setuptools import setup

with open("./requirements.txt") as f:
	install_requires = f.read().splitlines()
<<<<<<< HEAD
=======
	print(install_requires)
>>>>>>> 21ef7e4d4f2a8982f8337cebf4c1e630531b9366

setup(
	install_requires=install_requires
)
