from setuptools import setup


setup(
    name="light-pipe",
    version="0.3.0",
    long_description="""
        Light-Pipe makes data processing pipeline development quick and painless. It is an extensible, light-weight Python framework with zero non-standard dependencies for data pipelines that scale effortlessly. It abstracts away the implementation details of the pipeline itself, meaning that the developer only has to define the transformations performed within the pipeline on individual units of data.

        Pipelines defined using Light-Pipe scale effortlessly, with native support for all forms of concurrency, allowing for the mixing and matching of asynchronous, multi-threaded, and multi-process operations all within a single pipeline. It's also super fast and efficient, having been used to perform critical geospatial data processing tasks at least an order of magnitude faster than existing systems.

        Light-Pipe is released under a BSD-3-Clause License.
    """,
    url="https://github.com/rcorrero/light-pipe",
    author="Richard Correro",
    author_email="richard@richardcorrero.com",
    license="BSD 3-Clause",
    packages=["light_pipe"]
)