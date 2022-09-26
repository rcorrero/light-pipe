import numpy as np
from light_pipe import pipeline
from osgeo import gdal, ogr

gdal.UseExceptions()
ogr.UseExceptions()


def get_pred(arr: np.ndarray) -> bool:
    return not np.allclose(arr, 0)


def get_savepath(uid) -> str:
    savepath = \
        './data/tile_preds/item_samples/tile_preds' + '_' + str(uid) + '.tif'
    return savepath


def main():
    inputs = [
        {
            'dataset': '/vsizip/data/image.zip/image.tif',
            'datasources': ['./data/label_utm.geojson']
        },
    ]
    pipe = pipeline.LightPipeline(inputs)
    pipe.run()

    for sample in pipe:
        tiles = sample.tile()
        preds = list()
        for tile in tiles:
            pred = get_pred(tile.y)
            preds.append(pred)

        savepath = get_savepath(sample.uid)
        preds = np.array(preds)
        sample.save(savepath, preds) 


if __name__ == "__main__":
    main()