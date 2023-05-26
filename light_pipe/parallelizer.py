__author__ = "Richard Correro (richard@richardcorrero.com)"


import asyncio
import concurrent.futures
import functools
from concurrent.futures import Future
from typing import (Any, AsyncGenerator, Callable, Coroutine, Dict, Generator,
                    Iterable, List, Optional, Tuple, Union)


class Parallelizer:
    def __call__(
        self, iterable: Iterable, tuple_to_args: Optional[bool] = True,
        dict_to_kwargs: Optional[bool] = True
    ):
        for f, item, args, kwargs in iterable:
            if tuple_to_args and isinstance(item, Tuple):
                yield f(*item, *args, **kwargs)
            elif dict_to_kwargs and isinstance(item, Dict):
                yield f(*args, **item, **kwargs)
            else:
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
        ]] = None, 
        tuple_to_args: Optional[bool] = True,
        dict_to_kwargs: Optional[bool] = True
    ) -> Generator:
        if executor is None:
            executor = self.executor
        if max_workers is None:
            max_workers = self.max_workers

        if executor is not None:
            # futures = [
            #     executor.submit(f, item, *args, **kwargs) for 
            #     f, item, args, kwargs in iterable
            # ]
            yield from self._submit_tasks(
                iterable=iterable, executor=executor, tuple_to_args=tuple_to_args, 
                dict_to_kwargs=dict_to_kwargs
            )
        else:
            with self.DefaultExecutor(max_workers=max_workers) as executor:
                # futures = [
                #     executor.submit(f, item, *args, **kwargs) for 
                #     f, item, args, kwargs in iterable
                # ]
                # for future in concurrent.futures.as_completed(futures):
                #     yield future.result()
                yield from self._submit_tasks(
                    iterable=iterable, executor=executor, 
                    tuple_to_args=tuple_to_args, dict_to_kwargs=dict_to_kwargs
                )


    def _submit_tasks(
        self, iterable: Iterable, 
        executor: Union[
            concurrent.futures.ThreadPoolExecutor, 
            concurrent.futures.ProcessPoolExecutor
        ],
        tuple_to_args: Optional[bool] = True, 
        dict_to_kwargs: Optional[bool] = True
    ) -> Generator:
        futures: list = list()
        for f, item, args, kwargs in iterable:
            if tuple_to_args and isinstance(item, Tuple):
                futures.append(executor.submit(f, *item, *args, **kwargs))
            elif dict_to_kwargs and isinstance(item, Dict):
                futures.append(executor.submit(f, *args, **item, **kwargs))
            else:
                futures.append(executor.submit(f, item, *args, **kwargs))
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


    def _submit_task(
        self, f: Callable, item: Any,
        executor: Union[
            concurrent.futures.ThreadPoolExecutor, 
            concurrent.futures.ProcessPoolExecutor
        ],
        tuple_to_args: Optional[bool] = True, 
        dict_to_kwargs: Optional[bool] = True,
        *args, **kwargs
    ) -> Future:
        if tuple_to_args and isinstance(item, Tuple):
            return executor.submit(f, *item, *args, **kwargs)
        elif dict_to_kwargs and isinstance(item, Dict):
            return executor.submit(f, *args, **item, **kwargs)
        else:
            return executor.submit(f, item, *args, **kwargs)


    def _blocking_submitter(
        self,  iterable: Iterable, queue_size: int,
        executor: Optional[Union[
            concurrent.futures.ThreadPoolExecutor, 
            concurrent.futures.ProcessPoolExecutor
        ]] = None,
        tuple_to_args: Optional[bool] = True, 
        dict_to_kwargs: Optional[bool] = True,
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
                # futures[executor.submit(f, item, *args, **kwargs)] = "Done"
                futures[
                    self._submit_task(
                        f=f, item=item, executor=executor, tuple_to_args=tuple_to_args, 
                        dict_to_kwargs=dict_to_kwargs, *args, **kwargs
                    )
                ] = "Done"
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
        ]] = None,
        tuple_to_args: Optional[bool] = True, 
        dict_to_kwargs: Optional[bool] = True,
    ) -> Generator:
        if max_workers is None:
            max_workers = self.max_workers
        if queue_size is None:
            queue_size = self.queue_size
        if executor is None:
            executor = self.executor     

        if executor is not None:
            yield from self._blocking_submitter(
                iterable=iterable, queue_size=queue_size, executor=executor,
                tuple_to_args=tuple_to_args, dict_to_kwargs=dict_to_kwargs
            )
        else:
            with self.DefaultBlockingExecutor(
                max_workers=max_workers,
            ) as executor:
                yield from self._blocking_submitter(
                    iterable=iterable, queue_size=queue_size, executor=executor,
                    tuple_to_args=tuple_to_args, dict_to_kwargs=dict_to_kwargs
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
        self, iterable: Iterable, tuple_to_args: Optional[bool] = True, 
        dict_to_kwargs: Optional[bool] = True, **kwargs
    ) -> List[asyncio.Task]:
        tasks = list()
        for f, item, args, wkwargs in iterable:
            f = self._make_async_decorator(f)
            if tuple_to_args and isinstance(item, Tuple):
                tasks.append(f(*item, *args, **kwargs, **wkwargs))
            elif dict_to_kwargs and isinstance(item, Dict):
                tasks.append(f(*args, **item, **kwargs, **wkwargs))
            else:
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
        tuple_to_args: Optional[bool] = True, 
        dict_to_kwargs: Optional[bool] = True,
    ) -> Generator:
        if loop is None:
            loop = asyncio.get_event_loop()
        async_generator = self._async_gen(
            iterable, tuple_to_args=tuple_to_args, dict_to_kwargs=dict_to_kwargs
        )
        yield from self._iter(loop, async_generator)
