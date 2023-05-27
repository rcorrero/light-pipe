__author__ = "Richard Correro (richard@richardcorrero.com)"


import asyncio
import concurrent.futures
import functools
import logging
import queue
import threading
from concurrent.futures import Future
from typing import (Any, AsyncGenerator, Callable, Coroutine, Dict, Generator,
                    Iterable, List, Optional, Tuple, Union)


class QueueEmptySignal:
    pass


class Parallelizer:
    # def __init__(
    #     self, num_tries: Optional[int] = 1, 
    #     raise_after_retries: Optional[bool] = True, 
    #     failed_tasks: Optional[List[Tuple[Callable, Tuple, Dict]]] = None
    # ):
    #     if not raise_after_retries:
    #         assert failed_tasks is not None, \
    #             "`failed_tasks` must be passed when `raise_after_retries` is `False`."

    #     self._error_handler: Callable = self._make_error_handler_decorator(
    #         num_tries=num_tries, raise_after_retries=raise_after_retries,
    #         failed_tasks=failed_tasks
    #     )

    #     self.num_tries = num_tries
    #     self.raise_after_retries = raise_after_retries
    #     self.failed_tasks = failed_tasks


    def _make_error_handler_decorator(
        self, num_tries: Optional[int] = 1, 
        raise_after_retries: Optional[bool] = True,
        failed_tasks: Optional[List[Tuple[Callable, Tuple, Dict]]] = None
    ) -> Callable:
        if not raise_after_retries:
            assert failed_tasks is not None, \
                "`failed_tasks` must be passed when `raise_after_retries` is `False`."
        def handle_errors(fn: Callable) -> Callable:
            @functools.wraps(fn)
            def handle_errors_wrapper(*args, **kwargs) -> Any:
                error: Union[None, Exception] = None
                for _ in range(num_tries):
                    try:
                        result: Any = fn(*args, **kwargs)
                        return result
                    except Exception as e:
                        error = e
                        pass
                if raise_after_retries:
                    raise error
                else:
                    logging.warn(
                        f"An exception occurred while processing an item: {type(error).__name__}: {str(error)}"
                    )
                    failed_tasks.append((fn, args, kwargs))
            return handle_errors_wrapper
        return handle_errors


    def __call__(
        self, iterable: Iterable, tuple_to_args: Optional[bool] = True,
        dict_to_kwargs: Optional[bool] = True, num_tries: Optional[int] = 1, 
        raise_after_retries: Optional[bool] = True, 
        failed_tasks: Optional[List[Tuple[Callable, Tuple, Dict]]] = None
    ):
        error_handler: Callable = self._make_error_handler_decorator(
            num_tries=num_tries, raise_after_retries=raise_after_retries,
            failed_tasks=failed_tasks
        )
        for f, item, args, kwargs in iterable:
            f: Callable = error_handler(f)
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
        ]] = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
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
        dict_to_kwargs: Optional[bool] = True, num_tries: Optional[int] = 1, 
        raise_after_retries: Optional[bool] = True, 
        failed_tasks: Optional[List[Tuple[Callable, Tuple, Dict]]] = None
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
                dict_to_kwargs=dict_to_kwargs, num_tries=num_tries, 
                raise_after_retries=raise_after_retries, 
                failed_tasks=failed_tasks
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
                    tuple_to_args=tuple_to_args, dict_to_kwargs=dict_to_kwargs,
                    num_tries=num_tries, raise_after_retries=raise_after_retries, 
                    failed_tasks=failed_tasks
                )


    def _submit_tasks(
        self, iterable: Iterable, 
        executor: Union[
            concurrent.futures.ThreadPoolExecutor, 
            concurrent.futures.ProcessPoolExecutor
        ],
        tuple_to_args: Optional[bool] = True, 
        dict_to_kwargs: Optional[bool] = True, num_tries: Optional[int] = 1, 
        raise_after_retries: Optional[bool] = True, 
        failed_tasks: Optional[List[Tuple[Callable, Tuple, Dict]]] = None
    ) -> Generator:
        if isinstance(executor, concurrent.futures.ProcessPoolExecutor):
            if num_tries != 1 or not raise_after_retries:
                logging.warn("Error handling is not implemented for `ProcessPooler` instances.")
            error_handler: None = None
        else:
            error_handler: Callable = self._make_error_handler_decorator(
                num_tries=num_tries, raise_after_retries=raise_after_retries,
                failed_tasks=failed_tasks
            )
        futures: list = list()
        for f, item, args, kwargs in iterable:
            if error_handler is not None:
                f: Callable = error_handler(f)
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
        ]] = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
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
        dict_to_kwargs: Optional[bool] = True, num_tries: Optional[int] = 1, 
        raise_after_retries: Optional[bool] = True, 
        failed_tasks: Optional[List[Tuple[Callable, Tuple, Dict]]] = None
    ) -> Generator:
        if isinstance(executor, concurrent.futures.ProcessPoolExecutor):
            if num_tries != 1 or not raise_after_retries:
                logging.warn("Error handling is not implemented for `BlockingProcessPooler` instances.")
            error_handler: None = None
        else:
            error_handler: Callable = self._make_error_handler_decorator(
                num_tries=num_tries, raise_after_retries=raise_after_retries,
                failed_tasks=failed_tasks
            )
        futures = dict()
        exhausted = False
        num_submitted = 0
        while True:
            while not exhausted and num_submitted < queue_size:
                try:
                    f, item, args, kwargs = next(iterable)
                    if error_handler is not None:
                        f: Callable = error_handler(f)
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
        dict_to_kwargs: Optional[bool] = True, num_tries: Optional[int] = 1, 
        raise_after_retries: Optional[bool] = True, 
        failed_tasks: Optional[List[Tuple[Callable, Tuple, Dict]]] = None
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
                tuple_to_args=tuple_to_args, dict_to_kwargs=dict_to_kwargs,
                num_tries=num_tries, raise_after_retries=raise_after_retries,
                failed_tasks=failed_tasks
            )
        else:
            with self.DefaultBlockingExecutor(
                max_workers=max_workers,
            ) as executor:
                yield from self._blocking_submitter(
                    iterable=iterable, queue_size=queue_size, executor=executor,
                    tuple_to_args=tuple_to_args, dict_to_kwargs=dict_to_kwargs,
                    num_tries=num_tries, raise_after_retries=raise_after_retries,
                    failed_tasks=failed_tasks
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
    def __init__(
        self, loop: Optional[asyncio.AbstractEventLoop] = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.loop = loop
        self._terminate_flag = False


    def _make_async_error_handler_decorator(
        self, num_tries: Optional[int] = 1, 
        raise_after_retries: Optional[bool] = True,
        failed_tasks: Optional[List[Tuple[Callable, Tuple, Dict]]] = None
    ) -> Callable:
        if not raise_after_retries:
            assert failed_tasks is not None, \
                "`failed_tasks` must be passed when `raise_after_retries` is `False`."
        def handle_errors(fn: Callable) -> Callable:
            @functools.wraps(fn)
            async def handle_errors_wrapper(*args, **kwargs) -> Any:
                error: Union[None, Exception] = None
                for _ in range(num_tries):
                    try:
                        result: Any = fn(*args, **kwargs)
                        if isinstance(result, Coroutine):
                            return await result
                        return result
                    except Exception as e:
                        error = e
                        pass
                if raise_after_retries:
                    raise error
                else:
                    logging.warn(
                        f"An exception occurred while processing an item: {type(error).__name__}: {str(error)}"
                    )
                    failed_tasks.append((fn, args, kwargs))
            return handle_errors_wrapper
        return handle_errors


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
        dict_to_kwargs: Optional[bool] = True, num_tries: Optional[int] = 1, 
        raise_after_retries: Optional[bool] = True, 
        failed_tasks: Optional[List[Tuple[Callable, Tuple, Dict]]] = None,
        **kwargs
    ) -> List[asyncio.Task]:
        error_handler: Callable = self._make_async_error_handler_decorator(
            num_tries=num_tries, raise_after_retries=raise_after_retries,
            failed_tasks=failed_tasks
        )
        tasks = list()
        for f, item, args, wkwargs in iterable:
            f = self._make_async_decorator(f)
            f: Callable = error_handler(f)
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


    def _iter(
        self, loop: asyncio.AbstractEventLoop, async_generator: AsyncGenerator, 
        q: queue.Queue
    ) -> Generator:
        ait = async_generator.__aiter__()
        async def get_next() -> Tuple[bool, Any]:
            try:
                obj = await ait.__anext__()
                done = False
            except StopAsyncIteration:
                obj = None
                done = True
            return done, obj


        while not self._terminate_flag:
            done, obj = loop.run_until_complete(get_next())
            if done:
                q.put(QueueEmptySignal())
                break
            # yield obj 
            q.put(obj)   


    def queue_generator(self, q: queue.Queue) -> Generator:
        while not self._terminate_flag:
            obj: Any = q.get()
            if isinstance(obj, QueueEmptySignal):
                break
            yield obj   


    def __call__(
        self, iterable: Iterable,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        tuple_to_args: Optional[bool] = True, 
        dict_to_kwargs: Optional[bool] = True, num_tries: Optional[int] = 1, 
        raise_after_retries: Optional[bool] = True, 
        failed_tasks: Optional[List[Tuple[Callable, Tuple, Dict]]] = None
    ) -> Generator:
        if loop is None:
            if self.loop is not None:
                loop = self.loop
            else:
                loop = asyncio.new_event_loop()
        async_generator = self._async_gen(
            iterable, tuple_to_args=tuple_to_args, dict_to_kwargs=dict_to_kwargs,
            num_tries=num_tries, raise_after_retries=raise_after_retries,
            failed_tasks=failed_tasks
        )
        q: queue.Queue = queue.Queue()
        t = threading.Thread(
            target=self._iter, 
            kwargs={
                "loop": loop,
                "async_generator": async_generator,
                "q": q
            }
        )
        t.start()
        yield from self.queue_generator(q=q)
        t.join()
