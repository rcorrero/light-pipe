from typing import Callable, Generator, Iterable, Optional

from light_pipe import data


class Parallelizer:
    @classmethod
    def fork(
        cls, f: Callable, iterable: Iterable, *args, 
        recurse: Optional[bool] = True, **kwargs
    ) -> Generator:
        if recurse:
            results = [
                cls.fork(f, item, *args, recurse=recurse, **kwargs) if \
                    isinstance(item, data.Data) else \
                    f(item, *args, recurse=recurse, **kwargs) for item in iterable
            ]
        else:
            results = [
                f(item, *args, recurse=recurse, **kwargs) for item in iterable
            ]
        yield from results
