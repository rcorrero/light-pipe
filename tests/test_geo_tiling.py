import os
import random
import time

import matplotlib.pyplot as plt
from light_pipe import concurrency, gridding, mercantile, pipeline, processing
from osgeo import gdal, ogr
from rio_tiler.io import COGReader
from solaris import tile

gdal.UseExceptions()
ogr.UseExceptions()

wgs_84_epsg = 4326
zoom = 18
max_workers = 16
dataset_filepath = './data/big_image.tif'
# dataset_filepath = './data/image.tif'
raster_dest_dir = './data/tests/test_geo_tiling/solaris'
# vector_dest_dir = './data/solaris/rio_labels'

inputs = [
    {
        'datum': dataset_filepath,
        'is_label': False
    }
]


def test_solaris():
    start = time.time()
    ds = gdal.Open(dataset_filepath)

    grid_cells = gridding.get_grid_cells_from_dataset(ds, zoom=zoom, 
        default_dd_epsg=gridding.DEFAULT_DD_EPSG)

    tile_bounds = list()
    for grid_cell in grid_cells:
        bounds = mercantile.bounds(grid_cell)

        minx = bounds.west
        miny = bounds.south
        maxx = bounds.east
        maxy = bounds.north
        tile_bounds += [(minx, miny, maxx, maxy)]
    raster_tiler = tile.raster_tile.RasterTiler(dest_dir=raster_dest_dir,  # the directory to save images to
                                                tile_bounds=tile_bounds,  # the size of the output chips
                                                    )
    raster_tiler.tile(dataset_filepath)

    onlyfiles = [os.path.join(raster_dest_dir, f) for f in os.listdir(raster_dest_dir) if \
        os.path.isfile(os.path.join(raster_dest_dir, f))]

    raster_tiles = [gdal.Open(fp).ReadAsArray() for fp in onlyfiles]
    end = time.time()  

    return end - start


def test_rio_tiler():
    start = time.time()
    ds = gdal.Open(dataset_filepath)

    grid_cells = gridding.get_grid_cells_from_dataset(ds, zoom=zoom, 
        default_dd_epsg=gridding.DEFAULT_DD_EPSG)

    raster_tiles = list()
    with COGReader(dataset_filepath) as image:
        for grid_cell in grid_cells:
            x = grid_cell.x
            y = grid_cell.y
            z = grid_cell.z
            tile = image.tile(x, y, z)
            raster_tiles += [tile]
    end = time.time()  

    return end - start


def test_light_pipe():
    start = time.time()
    ch = concurrency.ThreadPoolHandler(max_workers=max_workers)
    gh = processing.GridSampleMaker(concurrency_handler=ch)
    # gh = processing.GridSampleMaker()
    pipe = pipeline.LightPipeline(inputs, processors=[gh])
    pipe.run(blocking=True, zoom=zoom)
    for sample in pipe:
        sample.load()
    end = time.time()  

    return end - start


if __name__ == "__main__":
    num_trials = 3
    plt_savepath = "./data/plots/test_geo_tiling.png"

    tests = [
        ("solaris", test_solaris),
        ("rio_tiler", test_rio_tiler),
        ("light_pipe", test_light_pipe)
    ]

    res = {tup[0]: list() for tup in tests}
    num_tests = len(tests)
    ids = list(range(num_tests))

    for trial in range(num_trials):
        random.shuffle(ids) # Randomize order
        for id in ids:
            test_name, test = tests[id]
            run_time = test()
            res[test_name].append(run_time)

    x = list(range(num_trials))
    plt.xlabel("Trial Number")
    plt.ylabel("Runtime in Seconds")
    plt.title(f"Comparison of Runtimes When Using Geographic Coordinates")
    for key, val in res.items():
        plt.plot(x, val, label=key)
    plt.legend()
    plt.savefig(plt_savepath)
