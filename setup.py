from setuptools import setup


setup(
    name="Light-Pipe",
    version="0.1",
    long_description="""
    A open source library that facilitates the development of highly-efficient,
    massively-scalable geospatial data pipelines for use in machine-learning 
    applications.
    """,
    url="https://github.com/rcorrero/light-pipe",
    author="Richard Correro",
    author_email="rcorrero@stanford.edu",
    license="BSD 3-Clause",
    packages=["light_pipe"],
    install_requires=["gdal>=3.5.1"]
)