__author__ = "Richard Correro (rcorrero@stanford.edu)"


import os
import time
import numpy as np

from .make_arrays import MANIFEST_PATH, RANDOM_SEED

np.random.seed(RANDOM_SEED)

SLEEP_TIME = 0.1


def process_array_cpu(array_path: str, *args, **kwargs) -> None:
    # with open(array_path, "r") as f:
    arr = np.load(array_path)
    time.sleep(SLEEP_TIME)
    # arr += arr
    # arr -= arr
    # with open(array_path, "w") as f:
    np.save(array_path, arr)


def main():
    manifest_path = MANIFEST_PATH
    assert os.path.exists(manifest_path), \
        f"{manifest_path} not found."
    start = time.time()
    with open(manifest_path, "r") as f:
        for line in f.read().splitlines():
            array_path = line
            try:
                process_array_cpu(array_path)
            except FileNotFoundError:
                print(f"File {array_path} not found.")
                # continue
                raise
    end = time.time()
    print(f"Time to run {__file__}: {end - start} seconds.")


if __name__ == "__main__":
    main()
