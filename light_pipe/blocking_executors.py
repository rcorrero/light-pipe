from concurrent.futures import ThreadPoolExecutor
import queue


class BlockingThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, *args, queue_size=0, **kwargs):
        super(ThreadPoolExecutor).__init__(*args, **kwargs)
        self._work_queue = queue.Queue(queue_size)