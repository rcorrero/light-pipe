import numpy as np
from light_pipe import pipeline
from light_pipe import concurrency
from light_pipe import processing
from osgeo import gdal, ogr

gdal.UseExceptions()
ogr.UseExceptions()


def get_pred(arr: np.ndarray) -> bool:
    return not np.allclose(arr, 0)


def get_savepath(uid) -> str:
    savepath = \
        './data/tile_preds/grid_samples/tile_preds' + '_' + str(uid) + '.tif'
    return savepath


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
    ch = concurrency.ThreadPoolHandler(max_workers=2)
    gh = processing.GridSampleMaker(concurrency_handler=ch)
    pipe = pipeline.LightPipeline(inputs, processors=[gh])
    pipe.run()

    sample_id = 0
    for sample in pipe:
        tiles = sample.tile(assert_tile_smaller_than_raster=True)
        preds = list()
        for tile in tiles:
            pred = get_pred(tile.y)
            preds.append(pred)

        savepath = get_savepath(sample.uid)
        preds = np.array(preds)
        sample.save(savepath, preds)
        sample_id += 1    


if __name__ == "__main__":
    main()
