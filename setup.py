from setuptools import setup


setup(
    name="Light-Pipe",
    version="0.1",
    long_description="""
    Light-Pipe is an open-source Python package that efficiently and losslessly creates analysis-ready samples from georeferenced data to facilitate the deployment of computer vision models at scale.
    """,
    url="https://github.com/rcorrero/light-pipe",
    author="Richard Correro",
    author_email="rcorrero@stanford.edu",
    license="BSD 3-Clause",
    packages=["light_pipe"],
    install_requires=["gdal>=3.5.1"]
)