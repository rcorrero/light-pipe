__author__ = "Richard Correro (rcorrero@gmail.com)"


import asyncio
import concurrent.futures
import functools
import time
import unittest
from typing import (Any, AsyncGenerator, Callable, Coroutine, Generator,
                    Iterable, List, Optional, Tuple, Union)


class Parallelizer:
    def __call__(
        self, iterable: Iterable    
    ):
        for f, item, args, kwargs in iterable:
            yield f(item, *args, **kwargs)


class Pooler(Parallelizer):
    def __init__(
        self, max_workers: Optional[int] = None,
        DefaultExecutor: Optional[type] = None,
        executor: Optional[Union[
            concurrent.futures.ThreadPoolExecutor, 
            concurrent.futures.ProcessPoolExecutor
        ]] = None
    ):
        if executor is None:
            assert max_workers is not None, \
                "`max_workers` must be set if `executor` is not passed."
        self.max_workers = max_workers
        self.DefaultExecutor = DefaultExecutor
        self.executor = executor


    def __call__(
        self, iterable: Iterable, max_workers: Optional[int] = None,
        executor: Optional[Union[
            concurrent.futures.ThreadPoolExecutor, 
            concurrent.futures.ProcessPoolExecutor
        ]] = None
    ) -> Generator:
        if executor is None:
            executor = self.executor
        if max_workers is None:
            max_workers = self.max_workers

        if executor is not None:
            futures = [
                executor.submit(f, item, *args, **kwargs) for 
                f, item, args, kwargs in iterable
            ]
            for future in concurrent.futures.as_completed(futures):
                yield future.result()
        else:
            with self.DefaultExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(f, item, *args, **kwargs) for 
                    f, item, args, kwargs in iterable
                ]
                for future in concurrent.futures.as_completed(futures):
                    yield future.result()   


class ThreadPooler(Pooler):
    def __init__(
        self, *args, 
        DefaultExecutor: Optional[type] = concurrent.futures.ThreadPoolExecutor,
        **kwargs
    ):
        super().__init__(*args, DefaultExecutor=DefaultExecutor, **kwargs)


class ProcessPooler(Pooler):
    def __init__(
        self, *args, 
        DefaultExecutor: Optional[type] = concurrent.futures.ProcessPoolExecutor,
        **kwargs
    ):
        super().__init__(*args, DefaultExecutor=DefaultExecutor, **kwargs)              


class BlockingPooler(Parallelizer):
    def __init__(
        self, max_workers: Optional[int] = None, queue_size: Optional[int] = None,
        DefaultBlockingExecutor: Optional[type] = None,
        executor: Optional[Union[
            concurrent.futures.ThreadPoolExecutor, 
            concurrent.futures.ProcessPoolExecutor
        ]] = None
    ):
        if executor is None:
            assert max_workers is not None and queue_size is not None, \
                "Both `max_workers` and `queue_size` must be set if `executor` is not passed."
        self.max_workers = max_workers
        self.DefaultBlockingExecutor = DefaultBlockingExecutor
        self.queue_size = queue_size
        self.executor = executor


    def _blocking_submitter(
        self,  iterable: Iterable, queue_size: int,
        executor: Optional[Union[
            concurrent.futures.ThreadPoolExecutor, 
            concurrent.futures.ProcessPoolExecutor
        ]] = None,
    ) -> Generator:
        futures = dict()
        exhausted = False
        num_submitted = 0
        while True:
            while not exhausted and num_submitted < queue_size:
                try:
                    f, item, args, kwargs = next(iterable)
                except StopIteration:
                    exhausted = True
                    break
                futures[executor.submit(f, item, *args, **kwargs)] = "Done"
                num_submitted += 1
            if futures: # There's at least one task left to await
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                ) # Will block until at least one future finishes or cancels
                future = done.pop()
                yield future.result()
                num_submitted -= 1
                del(futures[future])
            else:
                assert num_submitted == 0, \
                    f"There are still {num_submitted} tasks left to await."
                break


    def __call__(
        self, iterable: Iterable, max_workers: Optional[int] = None,
        queue_size: Optional[int] = None,
        executor: Optional[Union[
            concurrent.futures.ThreadPoolExecutor, 
            concurrent.futures.ProcessPoolExecutor
        ]] = None
    ) -> Generator:
        if max_workers is None:
            max_workers = self.max_workers
        if queue_size is None:
            queue_size = self.queue_size
        if executor is None:
            executor = self.executor     

        if executor is not None:
            yield from self._blocking_submitter(
                iterable=iterable, queue_size=queue_size, executor=executor
            )
        else:
            with self.DefaultBlockingExecutor(
                max_workers=max_workers,
            ) as executor:
                yield from self._blocking_submitter(
                    iterable=iterable, queue_size=queue_size, executor=executor
                )


class BlockingThreadPooler(BlockingPooler):
    def __init__(
        self, *args, 
        DefaultBlockingExecutor: Optional[type] = concurrent.futures.ThreadPoolExecutor,
        **kwargs
    ):
        super().__init__(
            *args, DefaultBlockingExecutor=DefaultBlockingExecutor, **kwargs
        )


class BlockingProcessPooler(BlockingPooler):
    def __init__(
        self, *args, 
        DefaultBlockingExecutor: Optional[type] = concurrent.futures.ProcessPoolExecutor,
        **kwargs
    ):
        super().__init__(
            *args, DefaultBlockingExecutor=DefaultBlockingExecutor, **kwargs
        )           


class AsyncGatherer(Parallelizer):
    def _make_async_decorator(self, f: Callable):
        @functools.wraps(f)
        async def async_wrapper(*args, **kwargs):
            result = f(*args, **kwargs)
            if isinstance(result, Coroutine):
                return await result
            return result
        return async_wrapper


    def _get_tasks(
        self, iterable: Iterable, **kwargs
    ) -> List[asyncio.Task]:
        tasks = list()
        for f, item, args, wkwargs in iterable:
            f = self._make_async_decorator(f)
            tasks.append(f(item, *args, **kwargs, **wkwargs))
        return tasks
            

    async def _async_gen(
        self, iterable: Iterable, **kwargs
    ) -> AsyncGenerator:
        tasks = self._get_tasks(iterable, **kwargs)
        for result in asyncio.as_completed(tasks):
            result = await result
            yield result


    def _iter(self, loop: asyncio.AbstractEventLoop, async_generator: AsyncGenerator):
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


    def __call__(
        self, iterable: Iterable,
        loop: Optional[asyncio.AbstractEventLoop] = None, 
    ) -> Generator:
        if loop is None:
            loop = asyncio.get_event_loop()
        async_generator = self._async_gen(iterable)
        yield from self._iter(loop, async_generator)


class TestParallelizers(unittest.TestCase):
    @staticmethod
    def task(num_tasks_submitted: int):
        num_tasks_submitted[0] += 1       


    def test_blocking_thread_pooler(self):
        num_tasks_submitted = [0]


        def gen(num_tasks):
            iterable = [
                (self.task, num_tasks_submitted, list(), dict()) for _ in range(num_tasks)
            ]
            yield from iterable


        num_tasks = 1000
        iterable = gen(num_tasks=num_tasks)

        max_workers = 8
        queue_size = 3
        p = BlockingThreadPooler(max_workers=max_workers, queue_size=queue_size)        
        for _ in p(iterable=iterable):
            assert num_tasks_submitted[0] <= queue_size
            num_tasks_submitted[0] -= 1


    def test_blocking_process_pooler(self):
        num_tasks_submitted = [0]


        def gen(num_tasks):
            iterable = [
                (self.task, num_tasks_submitted, list(), dict()) for _ in range(num_tasks)
            ]
            yield from iterable


        num_tasks = 1000
        iterable = gen(num_tasks=num_tasks)

        max_workers = 8
        queue_size = 3
        p = BlockingProcessPooler(max_workers=max_workers, queue_size=queue_size)        
        for _ in p(iterable=iterable):
            assert num_tasks_submitted[0] <= queue_size
            num_tasks_submitted[0] -= 1            
        

    def test_async_gatherer(self):
        async def sleep(seconds: int, *args, **kwargs):
            await asyncio.sleep(seconds)


        num_tasks = 1000
        seconds = 1
        iterable = [
            (sleep, seconds, list(), dict()) for _ in range(num_tasks)
        ]
        start = time.time()
        list(AsyncGatherer()(iterable=iterable))
        end = time.time()
        self.assertLessEqual(end - start, 1.5 * seconds)


if __name__ == "__main__":
    unittest.main()
