__author__ = "Richard Correro (richard@richardcorrero.com)"


import functools
from typing import Callable, Generator, Iterable, Iterator, Optional

from .data import Data
from .parallelizer import Parallelizer


class Transformer:
    __name__: str = "Transformer"


    def __init__(
        self, transform_item: Optional[Callable] = None,
        join_fn: Optional[Callable] = None,
        parallelizer: Optional[Parallelizer] = Parallelizer(),
        *args, **kwargs
    ):
        if transform_item is not None:
            self.transform_item = transform_item
        self.join_fn = join_fn
        self.parallelizer = parallelizer
        self.args = args
        self.kwargs = kwargs


    @classmethod
    def fork(
        cls, f: Callable, iterable: Iterable, *args,
        recurse: Optional[bool] = True, **kwargs
    ) -> Generator:
        if recurse:
            for item in iterable:
                if isinstance(item, Data) or isinstance(item, Iterator):
                    yield from cls.fork(f, item, *args, recurse=recurse, **kwargs)
                else:
                    yield f, item, args, kwargs
        else:
            for item in iterable:
                yield f, item, args, kwargs           


    @classmethod
    def join(cls, iterable: Iterable, recurse: Optional[bool] = True) -> Generator:
        for item in iterable:
            if recurse and (
                isinstance(item, Data) or isinstance(item, Iterator)
            ):
                yield from cls.join(item, recurse=recurse)
            else:
                yield item


    @staticmethod
    def transform_item(*args, **kwargs):
        raise NotImplementedError(
            (
                f"Either pass a `Callable` instance to __init__() or overwrite this "
                f"method in a subclass of {Transformer.__name__}."
            )
        )


    def _make_decorator(self, *args, recurse: Optional[bool] = True, **kwargs):
        def decorator(fn: Callable):
            @functools.wraps(fn)
            def wrapper(*wargs, **wkwargs):
                if self.join_fn is not None:
                    join = self.join_fn
                else:
                    join = self.join
                yield from join(
                    self.parallelizer(
                        self.fork(
                            self.transform_item, fn(*wargs, **wkwargs), *args, 
                            recurse=recurse, **kwargs,
                        ),
                    ),
                    recurse=recurse
                )
            return wrapper
        return decorator


    def transform(
        self, data: Data, *args, return_copy: Optional[bool] = True,
        block: Optional[bool] = False, **kwargs
    ) -> Data:
        decorator = self._make_decorator(*args, *self.args, **kwargs, **self.kwargs)
        if return_copy:
            data = data.copy(*args, **kwargs)
        data.wrap_generator(decorator, *args, **kwargs)
        if block:
            data.store_results = True # Overwrite setting when blocking on transformer
            data(block=block, no_return=True)
        return data


    def __call__(self, data: Data, *args, **kwargs):
        return self.transform(data, *args, **kwargs)


    def __ror__(self, data: Data):
        return self(data, return_copy=True)


    def __rlshift__(self, data: Data):
        """
        The left-shift operator is overloaded for symmetry with the right-shift
        operator overloading. Currently 
        ```
            Data(...) | Transformer(...)
        ```
        and 
        ```
            Data(...) << Transformer(...)
        ```
        do the same thing.
        """
        return self(data, return_copy=True)        


    def __rrshift__(self, data: Data):
        return self(data, return_copy=False)
    

def make_transformer(
    transform_item: Optional[Callable] = None
) -> Callable:
    def transformer_wrapper(*args, **kwargs) -> Transformer:
        return Transformer(transform_item=transform_item, *args, **kwargs)
    return transformer_wrapper
