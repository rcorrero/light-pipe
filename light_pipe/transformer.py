import functools
from typing import Callable, Generator, Iterable, Optional

from light_pipe import data, parallelizer


class Transformer:
    __name__: str = "Transformer"


    def __init__(
        self, 
        parallelizer: Optional[parallelizer.Parallelizer] = parallelizer.Parallelizer,
        *args, **kwargs
    ):
        self.parallelizer = parallelizer
        self.args = args
        self.kwargs = kwargs


    @staticmethod
    def _transformation_fn(input, *args, **kwargs):
        return input


    def _make_decorator(self, *args, **kwargs):
        def decorator(fn: Callable):
            @functools.wraps(fn)
            def wrapper(*wargs, **wkwargs):
                return self.join(
                    self.parallelizer.fork(
                        self._transformation_fn, fn(*wargs, **wkwargs), *args, 
                        **kwargs,
                    )
                )
            return wrapper
        return decorator


    @classmethod
    def join(cls, iterable: Iterable, recurse: Optional[bool] = True) -> Generator:
        for item in iterable:
            if recurse and isinstance(item, data.Data):
                yield from cls.join(item, recurse=recurse)
            else:
                yield item


    def transform(
        self, data: data.Data, *args, return_copy: Optional[bool] = True, **kwargs
    ) -> data.Data:
        decorator = self._make_decorator(*args, *self.args, **kwargs, **self.kwargs)
        if return_copy:
            data = data.copy(*args, **kwargs)
        data.wrap_generator(decorator, *args, **kwargs)
        return data


    def __call__(self, data: data.Data, *args, **kwargs):
        return self.transform(data, *args, **kwargs)


    def __ror__(self, data: data.Data):
        return self(data)
