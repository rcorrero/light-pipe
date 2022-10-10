import functools
import random
from typing import Any, Callable, Optional

import numpy as np
from light_pipe import abstractions, concurrency


class Transformer:
    def __init__(
        self, 
        concurrency: Optional[concurrency.ConcurrencyHandler] = concurrency.ConcurrencyHandler,
        *args, **kwargs
    ):
        self.concurrency = concurrency
        self.args = args
        self.kwargs = kwargs


    @staticmethod
    def _transformation_fn(n: int, *args, **kwargs):
        return n, n


    def _make_decorator(self, *args, **kwargs):
        def decorator(fn: Callable):
            @functools.wraps(fn)
            def wrapper(*wargs, **wkwargs):
                return self.concurrency.join(
                    self.concurrency.fork(
                        self._transformation_fn, fn(*wargs, **wkwargs), *args, 
                        **kwargs,
                    )
                )
            return wrapper
        return decorator


    def transform(self, data: abstractions.Data, *args, **kwargs):
        decorator = self._make_decorator(*args, *self.args, **kwargs, **self.kwargs)
        data.wrap_generator(decorator)
        return data


    def __call__(self, data: abstractions.Data, *args, **kwargs):
        return self.transform(data, *args, **kwargs)


    def __ror__(self, data: abstractions.Data):
        return self(data)


class AnyArrayFilter(Transformer):
    @staticmethod
    def _transformation_fn(arr: np.array, *args, **kwargs):
        return np.any(arr), arr


class RandomPartitioner(Transformer):
    @staticmethod
    def _transformation_fn(
        datum: Any, num_partitions: Optional[int] = 2, *args, **kwargs
    ):
        partition_id = random.randint(0, num_partitions - 1)
        return partition_id, datum


class Rasterizer(Transformer):
    @staticmethod
    def _transformation_fn(n: int, *args, **kwargs):
        return n, n
