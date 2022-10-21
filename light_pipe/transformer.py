import functools
from typing import Any, Callable, Generator, Iterable, Iterator, Optional

from light_pipe import data, parallelizer


class Transformer:
    __name__: str = "Transformer"


    def __init__(
        self, transform_item: Optional[Callable] = None,
        parallelizer: Optional[parallelizer.Parallelizer] = parallelizer.Parallelizer,
        *args, **kwargs
    ):
        if transform_item is not None:
            self.transform_item = transform_item
        self.parallelizer = parallelizer
        self.args = args
        self.kwargs = kwargs


    @classmethod
    def join(cls, iterable: Iterable, recurse: Optional[bool] = True) -> Generator:
        for item in iterable:
            if recurse and (
                isinstance(item, data.Data) or isinstance(item, Iterator)
            ):
                yield from cls.join(item, recurse=recurse)
            else:
                yield item


    @staticmethod
    def transform_item(*args, **kwargs):
        raise NotImplementedError(
            f"Either pass a `Callable` instance to __init__() or overwrite this \
                method in a subclass of {Transformer.__name__}."
        )


    def _make_decorator(self, *args, recurse: Optional[bool] = True, **kwargs):
        def decorator(fn: Callable):
            @functools.wraps(fn)
            def wrapper(*wargs, **wkwargs):
                yield from self.join(
                    self.parallelizer.fork(
                        self.transform_item, fn(*wargs, **wkwargs), *args, 
                        **kwargs,
                    ),
                    recurse=recurse
                )
            return wrapper
        return decorator


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
        return self(data, return_copy=True)


    def __rlshift__(self, data: data.Data):
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


    def __rrshift__(self, data: data.Data):
        return self(data, return_copy=False) 


class Pipeline(Transformer):
    __name__: str = "Pipeline"


    def __init__(
        self, transformers: Iterable[Transformer],
        parallelizer: Optional[parallelizer.Parallelizer] = parallelizer.Parallelizer,
        *args, **kwargs
    ):
        self.transformers = transformers
        self.parallelizer = parallelizer
        self.args = args
        self.kwargs = kwargs


    def transform_item(self, item: Any, *args, **kwargs):
        for transformer in self.transformers:
            item = transformer.transform_item(
                item, *args, *self.args, **kwargs, **self.kwargs
            )
        return item


    def transform(
        self, data: data.Data, *args, return_copy: Optional[bool] = True,
        **kwargs
    ) -> data.Data:
        for transformer in self.transformers:
            data = transformer.transform(
                data, *args, return_copy=return_copy, **kwargs,
            )
        return data
    