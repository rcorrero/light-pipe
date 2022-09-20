import concurrent.futures
import functools
import time
from typing import List, Optional, Union

from light_pipe import gridding, mercantile, raster_io, raster_trans
from osgeo import gdal, ogr, osr


def thread_map(
    fn, iterable, max_workers: Optional[int] = 1, executor = None, *args, **kwargs
):
    if executor is None:
        executor = concurrent.futures.ThreadPoolExecutor(max_workers)
    with executor as exec:
        # I don't use executor.map so as to preserve lazy collection of `iterable`.
        futures = [
            exec.submit(fn, item, *args, **kwargs) for item in iterable
        ]
        for future in concurrent.futures.as_completed(futures):
            yield future.result()


def main():
    fp = "./data/image.tif"
    shpf = "./data/label/label.shp"
    manifest_save_path = './data/gridded_manifest.json'
    zoom = 20
    dset = gdal.Open(fp)
    dsource = ogr.Open(shpf)
    manifest_dict = {}
    pixel_x_meters = 1.0
    pixel_y_meters = -1.0
    out_dir = "./data/gridded/"
    truncate = False
    max_workers = 10
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)

    map = functools.partial(thread_map, executor=executor)

    data = [dsource]

    start = time.time()
    data, grid_cell_datasets, manifest_dict = gridding.make_grid_cell_datasets(
        data, manifest_dict, zoom, pixel_x_meters, pixel_y_meters, out_dir,
        truncate=truncate, map=map
    )
    end = time.time()
    print(f"Total runtime: {end - start} seconds.")
    
    raster_io.save_dict_as_json(manifest_dict, manifest_save_path)


if __name__ == "__main__":
    main()
