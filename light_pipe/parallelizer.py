import concurrent.futures
from typing import Callable, Generator, Iterable, Iterator, Optional

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
                    (isinstance(item, data.Data) or isinstance(item, Iterator)) else \
                    f(item, *args, recurse=recurse, **kwargs) for item in iterable
            ]
        else:
            results = [
                f(item, *args, recurse=recurse, **kwargs) for item in iterable
            ]
        yield from results


class ThreadPool(Parallelizer):
    def __init__(
        self, max_workers: Optional[int] = None,
        executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
    ):
        if executor is None:
            assert max_workers is not None, \
                "`max_workers` must be set if `executor` is not passed."
        self.max_workers = max_workers
        self.executor = executor


    # @TODO: Make recursive
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


class ProcessPool(Parallelizer):
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


    # @TODO: Make recursive
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