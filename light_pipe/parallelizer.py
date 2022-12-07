import asyncio
import concurrent.futures
import functools
from typing import (Any, AsyncGenerator, Callable, Coroutine, Generator,
                    Iterable, List, Optional, Tuple, Union)

from light_pipe.blocking_executors import (BlockingProcessPoolExecutor,
                                           BlockingThreadPoolExecutor)


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
            BlockingThreadPoolExecutor, 
            BlockingProcessPoolExecutor
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
        self,  iterable: Iterable,
        executor: Optional[Union[
            BlockingThreadPoolExecutor, 
            BlockingProcessPoolExecutor
        ]] = None        
    ) -> Generator:
        futures = dict()
        exhausted = False
        while True:
            while not exhausted and not executor._work_queue.full():
                try:
                    f, item, args, kwargs = next(iterable)
                except StopIteration:
                    exhausted = True
                    break
                futures[executor.submit(f, item, *args, **kwargs)] = "Done"
            if futures: # There's at least one task left to await
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                ) # Will block until at least one future finishes or cancels
                future = done.pop()
                yield future.result()
                del(futures[future])
            else:
                break


    def __call__(
        self, iterable: Iterable, max_workers: Optional[int] = None,
        queue_size: Optional[int] = None,
        executor: Optional[Union[
            BlockingThreadPoolExecutor, 
            BlockingProcessPoolExecutor
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
                iterable=iterable, executor=executor
            )
        else:
            with self.DefaultBlockingExecutor(
                max_workers=max_workers, queue_size=queue_size
            ) as executor:
                yield from self._blocking_submitter(
                    iterable=iterable, executor=executor
                )


class BlockingThreadPooler(BlockingPooler):
    def __init__(
        self, *args, 
        DefaultBlockingExecutor: Optional[type] = BlockingThreadPoolExecutor,
        **kwargs
    ):
        super().__init__(
            *args, DefaultBlockingExecutor=DefaultBlockingExecutor, **kwargs
        )


class BlockingProcessPooler(BlockingPooler):
    def __init__(
        self, *args, 
        DefaultBlockingExecutor: Optional[type] = BlockingProcessPoolExecutor,
        **kwargs
    ):
        super().__init__(
            *args, DefaultBlockingExecutor=DefaultBlockingExecutor, **kwargs
        )           


class AsyncGatherer(Parallelizer):
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


    def _make_async_decorator(self, f: Callable):
        @functools.wraps(f)
        async def async_wrapper(*args, **kwargs):
            result = f(*args, **kwargs)
            if isinstance(result, Coroutine):
                return await result
            return result
        return async_wrapper


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
