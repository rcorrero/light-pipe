__author__ = "Richard Correro (richard@richardcorrero.com)"


from .data import *
from .parallelizer import *
from .transformer import *

__doc__ = """
# [Light-Pipe](https://github.com/rcorrero/light-pipe)

---

## Overview

[Light-Pipe](https://www.light-pipe.io/) makes data processing pipeline development quick and painless. It is an extensible, light-weight Python framework with zero non-standard dependencies for data pipelines that scale effortlessly. It abstracts away the implementation details of the pipeline itself, meaning that the developer only has to define the transformations performed within the pipeline on individual units of data.

Pipelines defined using Light-Pipe scale effortlessly, with native support for all forms of concurrency, allowing for the mixing and matching of asynchronous, multi-threaded, and multi-process operations all within a single pipeline. It's also super fast and efficient, having been used to perform critical geospatial data processing tasks [at least an order of magnitude faster than existing systems](https://github.com/rcorrero/light-pipe/blob/depth_first/data/plots/test_geo_tiling.png).

Light-Pipe is released under a [BSD-3-Clause License](https://opensource.org/licenses/BSD-3-Clause).

## Installing Light-Pipe

```
pip install light-pipe

```

## Basic Example

```
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

## More Information

- [GitHub](https://github.com/rcorrero/light-pipe)

- [Documentation](https://www.light-pipe.io/)

---

Copyright 2020-2023 [Richard Correro](https://www.richardcorrero.com/).
"""
