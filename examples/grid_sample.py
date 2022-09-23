import time

import numpy as np
from light_pipe import concurrency, pipeline, processing
from osgeo import gdal, ogr

gdal.UseExceptions()
ogr.UseExceptions()


def get_pred(arr: np.ndarray) -> bool:
    return not np.allclose(arr, 0)


def get_savepath(uid) -> str:
    savepath = \
        './data/tile_preds/grid_samples/tile_preds' + '_' + str(uid) + '.tif'
    return savepath


def process_fn(sample, tile_counter):
    tiles = sample.tile(assert_tile_smaller_than_raster=True)
    preds = list()
    for tile in tiles:
        tile_counter[0] +=1
        pred = get_pred(tile.y)
        preds.append(pred)

    savepath = get_savepath(sample.uid)
    preds = np.array(preds)
    sample.save(savepath, preds)
    return sample.uid, savepath


def main():
    inputs = [
        {
            'datum': './data/image.tif',
            'is_label': False,
        },
        {
            'datum': './data/label/label.shp',
            'is_label': True
        }
    ]
    gh = processing.GridSampleMaker()
    pipe = pipeline.LightPipeline(inputs, processors=[gh])
    start = time.time()
    pipe.run(blocking=True)
    end = time.time()
    print(f"Time to run pipeline: {end - start} seconds.")
    ch = concurrency.ThreadPoolHandler(max_workers=64)
    proc = processing.SampleProcessor(fn=process_fn, concurrency_handler=ch)
    tile_counter = [0]
    start = time.time()
    results = proc.run(pipe, tile_counter = tile_counter)
    list(results)
    end = time.time()  
    print(f"Time to make make predictions: {end - start} seconds.")
    print(f"Total number of tiles: {tile_counter[0]}.")


if __name__ == "__main__":
    main()
