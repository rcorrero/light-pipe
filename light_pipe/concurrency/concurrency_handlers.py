__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains the definition of `ConcurrencyHandler`. `ConcurrencyHandler`
instances are associated with two methods, `fork` and `join`, which are used
by `SampleMaker` instances to create samples in a concurrent and/or parallel
manner.
"""


import concurrent.futures
from typing import Callable, Generator, Iterable, Optional, Tuple


class ConcurrencyHandler:
    parallel: bool = False


    @classmethod
    def fork(cls, f: Callable, iterable: Iterable, *args, **kwargs) -> Generator:
        results = [
            f(item, *args, **kwargs) for item in iterable
        ]
        yield from results
        # return results


    @classmethod
    def join(cls, iterable: Iterable[Tuple]) -> Generator:
        results = dict()
        for item in iterable:
            if isinstance(item, Generator) or isinstance(item, list):
                for key, values in cls.join(item):
                    if key in results.keys():
                        results[key] += values
                    else:
                        results[key] = values
            else:
                key, value = item
                if key in results.keys():
                    results[key].append(value)
                else:
                    results[key] = [value]
        yield from results.items()
        # return results


class ThreadPoolHandler(ConcurrencyHandler):
    def __init__(
        self, max_workers: Optional[int] = None,
        executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
    ):
        if executor is None:
            assert max_workers is not None, \
                "`max_workers` must be set if `executor` is not passed."
        self.max_workers = max_workers
        self.executor = executor


    def fork(
        self, f: Callable, iterable: Iterable, max_workers: Optional[int] = None,
        executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
        *args, **kwargs) -> Generator:
        if executor is None:
            executor = self.executor
        if max_workers is None:
            max_workers = self.max_workers

        if executor is not None:
            futures = [
                executor.submit(f, item, *args, **kwargs) for item in iterable
            ]
            for future in concurrent.futures.as_completed(futures):
                yield future.result()
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(f, item, *args, **kwargs) for item in iterable
                ]
                for future in concurrent.futures.as_completed(futures):
                    yield future.result()


class ProcessPoolHandler(ConcurrencyHandler):
    parallel: bool = True


    def __init__(
        self, max_workers: Optional[int] = None,
        executor: Optional[concurrent.futures.ProcessPoolExecutor] = None
    ):
        # @TODO: IMPLEMENT:
        # Checks must be put in place to ensure that inputs and outputs are
        # serializable. This means no SWIG objects and no generators.
        if executor is None:
            assert max_workers is not None, \
                "`max_workers` must be set if `executor` is not passed."
        self.max_workers = max_workers
        self.executor = executor


    def fork(
        self, f: Callable, iterable: Iterable, max_workers: Optional[int] = None,
        executor: Optional[concurrent.futures.ProcessPoolExecutor] = None,
        *args, **kwargs) -> Generator:
        if executor is None:
            executor = self.executor
        if max_workers is None:
            max_workers = self.max_workers

        kwargs['make_pickleable'] = True

        if executor is not None:
            futures = [
                executor.submit(f, item, *args, **kwargs) for item in iterable
            ]
            for future in concurrent.futures.as_completed(futures):
                yield future.result()        
        else:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(f, item, *args, **kwargs) for item in iterable
                ]
                for future in concurrent.futures.as_completed(futures):
                    yield future.result()
