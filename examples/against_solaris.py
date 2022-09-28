import os
from solaris import tile
import time
from light_pipe import pipeline, processing
import numpy as np
from osgeo import gdal, ogr

gdal.UseExceptions()
ogr.UseExceptions()


tile_y = 128
tile_x = 128
dataset_filepath = './data/big_image.tif'
raster_dest_dir = './data/solaris/against_solaris'


def test_solaris():
    raster_tiler = tile.raster_tile.RasterTiler(dest_dir=raster_dest_dir,  # the directory to save images to
                                                    src_tile_size=(tile_y, tile_x),  # the size of the output chips
                                                    )
    raster_tiler.tile(dataset_filepath)
    onlyfiles = [os.path.join(raster_dest_dir, f) for f in os.listdir(raster_dest_dir) if \
        os.path.isfile(os.path.join(raster_dest_dir, f))]

    raster_tiles = [gdal.Open(fp).ReadAsArray() for fp in onlyfiles]  


def test_light_pipe():
    lp_sample = processing.LightPipeSample(dataset_filepath, array_dtype=np.uint8)

    tiles = lp_sample.tile(tile_y=tile_y, tile_x=tile_x)

    tiles = list(tiles)   


if __name__ == "__main__":
    start = time.time()
    test_solaris()
    end = time.time()  
    print(f"Time for Solaris to make tiles: {end - start} seconds.")

    time.sleep(16)
    
    start = time.time()
    test_light_pipe()
    end = time.time()  
    print(f"Time for Light-Pipe to make tiles: {end - start} seconds.")
