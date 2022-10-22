import asyncio
import concurrent.futures
import functools
from typing import (Any, AsyncGenerator, Callable, Coroutine, Generator,
                    Iterable, Iterator, List, Optional, Tuple)

from light_pipe import data
from light_pipe.blocking_executors import BlockingThreadPoolExecutor


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
                    f(item, *args, **kwargs) for item in iterable
            ]
        else:
            results = [
                f(item, *args, **kwargs) for item in iterable
            ]
        yield from results


class BlockingParallelizer:
    @classmethod
    def fork(
        cls, f: Callable, iterable: Iterable, *args, 
        recurse: Optional[bool] = True, **kwargs
    ) -> Generator:
        if recurse:
            for item in iterable:
                if isinstance(item, data.Data) or isinstance(item, Iterator):
                    yield from cls.fork(f, item, *args, recurse=recurse, **kwargs)
                else:
                    yield f(item, *args, **kwargs)
        else:
            for item in iterable:
                yield f(item, *args, **kwargs)   


class ThreadPooler(Parallelizer):
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



class BlockingThreadPooler(Parallelizer):
    def __init__(
        self, max_workers: Optional[int] = None, queue_size: Optional[int] = None,
        executor: Optional[BlockingThreadPoolExecutor] = None
    ):
        if executor is None:
            assert max_workers is not None and queue_size is not None, \
                "Both `max_workers` and `queue_size` must be set if `executor` is not passed."
        self.max_workers = max_workers
        self.queue_size = queue_size
        self.executor = executor


    # @TODO: Make recursive
    def fork(
        self, f: Callable, iterable: Iterable, max_workers: Optional[int] = None,
        queue_size: Optional[int] = None,
        executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
        *args, **kwargs) -> Generator:
        if max_workers is None:
            max_workers = self.max_workers
        if queue_size is None:
            queue_size = self.queue_size
        if executor is None:
            executor = self.executor            

        if executor is not None:
            futures = dict()
            while True:
                while not executor._work_queue.full():
                    try:
                        item = next(iterable)
                    except StopIteration:
                        break
                    futures[executor.submit(f, item, *args, **kwargs)] = "Done"
                done, not_done = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for future in done:
                    yield future.result()
                    del(futures[future])


            for future in concurrent.futures.as_completed(futures):
                yield future.result()
        else:
            with BlockingThreadPoolExecutor(
                max_workers=max_workers, queue_size=queue_size
            ) as executor:
                futures = [
                    executor.submit(f, item, *args, **kwargs) for item in iterable
                ]
                for future in concurrent.futures.as_completed(futures):
                    yield future.result()                    


class ProcessPooler(Parallelizer):
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


class AsyncGatherer(Parallelizer):
    @classmethod
    def _get_tasks(
        cls, f: Callable, iterable: Iterable, *args, 
        recurse: Optional[bool] = True, **kwargs
    ) -> List[asyncio.Task]:
        tasks = list()
        for item in iterable:
            if recurse and (isinstance(item, data.Data) or isinstance(item, Iterator)):
                tasks += cls._get_tasks(f, item, *args, recurse=recurse, **kwargs)
            else:
                tasks.append(f(item, *args, **kwargs))
        return tasks
            

    @classmethod
    async def _fork(
        cls, f: Callable, iterable: Iterable, *args, 
        recurse: Optional[bool] = True, **kwargs
    ) -> AsyncGenerator:
        tasks = cls._get_tasks(f, iterable, *args, recurse=recurse, **kwargs)
        for result in asyncio.as_completed(tasks):
            result = await result
            yield result


    @classmethod
    def _make_async_decorator(cls, f: Callable):
        @functools.wraps(f)
        async def async_wrapper(*args, **kwargs):
            result = f(*args, **kwargs)
            if isinstance(result, Coroutine):
                return await result
            return result
        return async_wrapper


    @classmethod
    def _iter(cls, loop: asyncio.AbstractEventLoop, async_generator: AsyncGenerator):
        ait = async_generator.__aiter__()
        async def get_next() -> Tuple[bool, Any]:
            try:
                obj = await ait.__anext__()
                done = False
            except StopAsyncIteration:
                obj = None
                done = True
            return done, obj


        while True:
            done, obj = loop.run_until_complete(get_next())
            if done:
                break
            yield obj        


    @classmethod
    def fork(
        cls, f: Callable, iterable: Iterable, *args, 
        recurse: Optional[bool] = True, 
        loop: Optional[asyncio.AbstractEventLoop] = None, 
        **kwargs
    ) -> Generator:
        f = cls._make_async_decorator(f)
        if loop is None:
            loop = asyncio.get_event_loop()
        async_generator = cls._fork(f, iterable, *args, recurse=recurse, **kwargs)
        yield from cls._iter(loop, async_generator)
