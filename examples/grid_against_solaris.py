import os
from solaris import tile
import time
from light_pipe import pipeline, processing, gridding, mercantile, concurrency
import numpy as np
from osgeo import gdal, ogr
import pyproj
from rio_tiler.io import COGReader

gdal.UseExceptions()
ogr.UseExceptions()

wgs_84_epsg = 4326
zoom = 18
max_workers = 16
dataset_filepath = './data/big_image.tif'
# dataset_filepath = './data/image.tif'
raster_dest_dir = './data/solaris/grid_against_solaris'
# vector_dest_dir = './data/solaris/rio_labels'

inputs = [
    {
        'datum': dataset_filepath,
        'is_label': False
    }
]


def test_solaris():
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


def test_rio_tiler():
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


def test_light_pipe():
    ch = concurrency.ThreadPoolHandler(max_workers=max_workers)
    gh = processing.GridSampleMaker(concurrency_handler=ch)
    # gh = processing.GridSampleMaker()
    pipe = pipeline.LightPipeline(inputs, processors=[gh])
    pipe.run(blocking=True, zoom=zoom)
    for sample in pipe:
        sample.load()


if __name__ == "__main__":
    # start = time.time()
    # test_solaris()
    # end = time.time()  
    # print(f"Time for Solaris to make grid cells: {end - start} seconds.")

    # time.sleep(16)

    # start = time.time()
    # test_rio_tiler()
    # end = time.time()  
    # print(f"Time for rio-tiler to make grid cells: {end - start} seconds.")

    # time.sleep(16)
    
    start = time.time()
    test_light_pipe()
    end = time.time()  
    print(f"Time for Light-Pipe to make grid cells: {end - start} seconds.")