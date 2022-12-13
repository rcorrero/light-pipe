__author__ = "Richard Correro (rcorrero@stanford.edu)"


import queue
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor


class BlockingThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, *args, queue_size=0, **kwargs):
        super().__init__(*args, **kwargs)
        self._work_queue = queue.Queue(queue_size)


class BlockingProcessPoolExecutor(ProcessPoolExecutor):
    def __init__(self, *args, queue_size=0, **kwargs):
        super().__init__(*args, **kwargs)
        self._work_queue = queue.Queue(queue_size)        
