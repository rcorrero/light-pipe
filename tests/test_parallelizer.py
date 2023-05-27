__author__ = "Richard Correro (richard@richardcorrero.com)"


import asyncio
import time
import unittest

from light_pipe import (AsyncGatherer, BlockingProcessPooler,
                        BlockingThreadPooler)


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
            self.assertLessEqual(num_tasks_submitted[0], queue_size)
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
            self.assertLessEqual(num_tasks_submitted[0], queue_size)
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
