import time

import numpy as np
from light_pipe.concurrency import concurrency_handlers
from light_pipe.pipeline import LightPipeline
from light_pipe.samples.sample_handlers import SampleHandler, GridSampleHandler
from osgeo import gdal, ogr

def test_grid_samples():
    ds = gdal.Open('./data/image.tif')
    ds2 = ogr.Open('./data/label/label.shp')
    iterable = [
        {
            'datum': ds,
            'is_label': False,
        },
        {
            'datum': ds2,
            'is_label': True
        }
    ]
    ch = concurrency_handlers.ThreadPoolHandler(max_workers=10)
    # ch = concurrency_handlers.ConcurrencyHandler
    gh = GridSampleHandler(concurrency_handler=ch)
    pipe = LightPipeline(iterable, sample_handler=gh)
    pipe.run(iterable, zoom=14)
    sample_id = 0
    for sample in pipe:
        tiles = sample.shuffle()
        # tiles = sample.tile()
        preds = list()
        for tile in tiles:
            preds.append(not np.allclose(tile.y, 0))

        savepath = './data/tile_preds/grid_samples/tile_preds' + '_' + str(sample_id) + '.tif'
        preds = np.array(preds)
        sample.save(savepath, preds)
        sample_id += 1


def test_item_samples():
    ds = gdal.Open('./data/image.tif')
    ds2 = ogr.Open('./data/label/label.shp')
    iterable = [
        {
            'dataset': ds,
            'datasources': [ds2]
        },
    ]
    ch = concurrency_handlers.ThreadPoolHandler(max_workers=10)
    # ch = concurrency_handlers.ConcurrencyHandler
    sh = SampleHandler(concurrency_handler=ch)
    pipe = LightPipeline(iterable, sample_handler=sh)
    pipe.run(iterable)
    sample_id = 0
    for sample in pipe:
        tiles = sample.shuffle()
        # tiles = sample.tile()
        preds = list()
        for tile in tiles:
            preds.append(not np.allclose(tile.y, 0))

        savepath = './data/tile_preds/item_samples/tile_preds' + '_' + str(sample_id) + '.tif'
        preds = np.array(preds)
        sample.save(savepath, preds)
        sample_id += 1


if __name__ == "__main__":
    start = time.time()
    test_item_samples()
    test_grid_samples()
    end = time.time()
    print(f"Total runtime: {end - start} seconds.")

        