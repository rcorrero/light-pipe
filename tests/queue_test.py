__author__ = "Richard Correro (rcorrero@stanford.edu)"


import os
import queue
import threading
import time

import numpy as np
from light_pipe import threading_utils

from .make_arrays import MANIFEST_PATH, RANDOM_SEED
from .single_thread_test import process_array_cpu

np.random.seed(RANDOM_SEED)


@threading_utils.make_worker
def process_array_worker(*args, **kwargs):
    return process_array_cpu(*args, **kwargs)


def main():
    maxsize = 0
    n_workers = int(input("Number of threads: "))
    manifest_path = MANIFEST_PATH
    assert os.path.exists(manifest_path), \
        f"{manifest_path} not found."

    in_q = queue.Queue(maxsize=maxsize)
    kwargs = {
        "in_q": in_q
    }
    for _ in range(n_workers):
        thread = threading.Thread(
            target=process_array_worker, kwargs=kwargs, daemon=True
        )
        thread.start()
    start = time.time()
    with open(manifest_path, "r") as f:
        for line in f.read().splitlines():
            array_path = line
            if array_path != "":
                item = {
                    "array_path": array_path
                }
                in_q.put(item)
    in_q.join()
    end = time.time()
    print(f"Time to run {__file__}: {end - start} seconds.")


if __name__ == "__main__":
    main()
