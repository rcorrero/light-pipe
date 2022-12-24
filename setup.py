from setuptools import setup


setup(
    name="Light-Pipe",
    version="0.2.1",
    long_description="""
    Light-Pipe is an extensible, light-weight Python framework for data pipelines that scale. It provides a set of intuitive abstractions designed to decouple pipeline implementation from the operations performed within the pipeline. It is designed to scale effortlessly, being built from the ground-up to support concurrency in all its forms, and it has zero non-standard dependencies. Light-Pipe is released under a BSD-3-Clause License.
    """,
    url="https://github.com/rcorrero/light-pipe",
    author="Richard Correro",
    author_email="rcorrero@gmail.com",
    license="BSD 3-Clause",
    packages=["light_pipe"]
)