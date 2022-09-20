__author__ = "Richard Correro (rcorrero@stanford.edu)"


import os
import time
import numpy as np

from light_pipe import raster_io, tiling

MANIFEST_PATH = "./data/_tests/manifest.txt"
SAVE_DIR = "./data/_tests/arrays/"
RANDOM_SEED = 8675309

np.random.seed(RANDOM_SEED)

def make_arrays(
    n_arrays: int, save_dir: str, C, H, W
):
    out_paths = []
    for i in range(n_arrays):
        uid = tiling.get_uid(i)
        out_filename = str(uid) + ".npy"
        out_path = os.path.join(save_dir, out_filename)
        out_paths.append(out_path)
        arr = np.random.rand(C, H, W) # np.float
        np.save(out_path, arr)
        # with open(out_path, "w") as f:
        #     np.save(f, arr)
    return out_paths


def save_paths(manifest_path: str, paths: list):
    with open(manifest_path, "w") as f:
        for path in paths:
            f.write(path + "\n")


def main():
    manifest_path = MANIFEST_PATH
    save_dir = SAVE_DIR
    if os.path.exists(save_dir):
        raster_io.remove(save_dir)
    n_arrays = int(input("Number of arrays to make: "))
    start = time.time()
    # if not os.path.exists(save_dir):
    os.makedirs(save_dir)
    C = 6
    H = 2017
    W = 1997
    out_paths = make_arrays(n_arrays, save_dir, C, H, W)
    save_paths(manifest_path, out_paths)
    end = time.time()
    print(f"Time to run {__file__}: {end - start} seconds.")


if __name__ == "__main__":
    main()