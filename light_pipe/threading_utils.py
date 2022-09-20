__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains decorators which can be used to execute user-defined
functions in a concurrent manner.
"""


import asyncio
import concurrent.futures
import functools
import queue
from typing import Callable, Iterable, Iterator, Optional, Union


def mmap(fn: Callable, iterable: Iterable, **kwargs):
    mapfunc = functools.partial(fn, **kwargs)
    return map(mapfunc, iterable)


# From: https://stackoverflow.com/a/61478547
# Credit: stackoverflow user "Andrei"
async def gather_with_concurrency(n, *coros):
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro
    return await asyncio.gather(*(sem_coro(coro) for coro in coros))


def make_worker(f):
    @functools.wraps(f)
    def worker_wrapper(
        in_q: queue.Queue, out_q: Optional[Union[queue.Queue, None]] = None,
        worker_callback: Optional[Union[None, Callable]] = None,
        *args, **kwargs
    ):
        while True:
            new_kwargs = in_q.get(block=True)
            assert isinstance(new_kwargs, dict), \
                f"Queued items must be dicts. Received object of type {type(new_kwargs)}."
            kwargs = {**kwargs, **new_kwargs}
            res = f(*args, **kwargs)
            if worker_callback is not None:
                res = worker_callback(res, *args, **kwargs)
            in_q.task_done()
            if out_q is not None:
                out_q.put(res, block=True)
    return worker_wrapper


# async def get_future_result(future: concurrent.futures.Future):
#     res = future.result()
#     return res


# def make_coro(f):
#     @functools.wraps(f)
#     async def make_coro_wrapper(
#         executor: concurrent.futures.ThreadPoolExecutor, *args, **kwargs
#     ):
#         future = executor.submit(
#             f, *args, **kwargs
#         )
#         res = await get_future_result(future)
#         return res
#     return make_coro_wrapper


def make_coro(f):
    @functools.wraps(f)
    def make_coro_wrapper(
        iterable: Iterator, 
        executor: Optional[concurrent.futures.ThreadPoolExecutor] = None, 
        max_workers: Optional[int] = 1, *args, **kwargs
    ):
        if executor is None:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers)
        with executor as exec:
            # I don't use executor.map so as to preserve lazy collection of `iterable`.
            futures = [
                exec.submit(f, item, *args, **kwargs) for item in iterable
            ]
            for future in concurrent.futures.as_completed(futures):
                yield future.result()
    return make_coro_wrapper
    