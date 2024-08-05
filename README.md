# [Light-Pipe](https://github.com/rcorrero/light-pipe)

---

## Overview

[Light-Pipe](https://www.light-pipe.io/) is a high-level syntax for data pipelines, designed to make pipeline development quick and painless. It is an extensible, light-weight Python framework with zero non-standard dependencies for data pipelines that scale effortlessly. It abstracts away the implementation details of the pipeline itself, meaning that the developer only has to define the transformations performed within the pipeline on individual units of data.

Pipelines defined using Light-Pipe scale effortlessly, with native support for all forms of concurrency, allowing for the mixing and matching of asynchronous, multi-threaded, and multi-process operations all within a single pipeline. It's easily extensible for use with distributed processing services such as [Celery](https://docs.celeryq.dev/en/stable/). It's also super fast and efficient, having been used to perform critical geospatial data processing tasks [at least an order of magnitude faster than existing systems](https://github.com/rcorrero/light-pipe/blob/depth_first/data/plots/test_geo_tiling.png).

Light-Pipe is released under a [BSD-3-Clause License](https://opensource.org/licenses/BSD-3-Clause).

## Installing Light-Pipe

```console
$ pip install light-pipe
```

## A Basic Example

```python
>>> from light_pipe import make_data, make_transformer
>>> 
>>> 
>>> @make_data
>>> def gen_dicts(x: int):
>>>     for i in range(x):
>>>         yield {
>>>             "one": 3 * i, 
>>>             "two": 3 * i + 1, 
>>>             "three": 3 * i + 2
>>>         }
>>> 
>>> @make_transformer
>>> def get_third(one: int, two: int, three: int):
>>>     print(f"Third: {three}")
>>>     return three
>>> 
>>> 
>>> data = gen_dicts(x=3, store_results=True)
>>> data >> get_third()
>>> 
>>> print(data(block=True))
Third: 2
Third: 5
Third: 8
[2, 5, 8]
>>>
>>> print(data(block=True))
[2, 5, 8]
```

## A (Slightly) More Interesting Example

```python
>>> import asyncio
>>> import time
>>> 
>>> from light_pipe import AsyncGatherer, make_data, make_transformer
>>> 
>>> 
>>> @make_data
>>> def gen(x: int):
>>>     yield from range(x)
>>> 
>>> 
>>> @make_transformer
>>> async def add_one(x: int):
>>>     await asyncio.sleep(1)
>>>     return x + 1
>>> 
>>> 
>>> data = gen(x=8)
>>> 
>>> t = add_one(parallelizer=AsyncGatherer())
>>> 
>>> 
>>> for _ in range(10):
>>>     data >> t
>>> 
>>> start = time.time()
>>> print(data(block=True))
[12, 10, 14, 11, 16, 15, 13, 17]
>>> 
>>> end = time.time()
>>> diff = end - start
>>> print(f"Total time to execute tasks: {diff:.1f} seconds.")
Total time to execute tasks: 10.0 seconds.
```

## More Information

- [GitHub](https://github.com/rcorrero/light-pipe)

- [Documentation](https://www.light-pipe.io/)

---

Copyright 2020-Present [Richard Correro](https://www.richardcorrero.com/).