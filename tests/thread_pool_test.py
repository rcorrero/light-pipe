__author__ = "Richard Correro (rcorrero@stanford.edu)"


import os
import asyncio
import concurrent.futures
import time

import numpy as np
from light_pipe import threading_utils

from .make_arrays import MANIFEST_PATH, RANDOM_SEED
from .single_thread_test import process_array_cpu

np.random.seed(RANDOM_SEED)


@threading_utils.make_coro
def process_array_coro(*args, **kwargs):
    return process_array_cpu(*args, **kwargs)


async def main():
    n_workers = int(input("Number of threads: "))
    manifest_path = MANIFEST_PATH
    assert os.path.exists(manifest_path), \
        f"{manifest_path} not found."
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=n_workers)
    start = time.time()
    coros = []
    with open(manifest_path, "r") as f:
        for line in f.read().splitlines():
            array_path = line
            if array_path != "":
                coros.append(process_array_coro(executor=executor, array_path=array_path))
    await asyncio.gather(*coros)
    end = time.time()
    print(f"Time to run {__file__}: {end - start} seconds.")


if __name__ == "__main__":
    asyncio.run(main())