import numpy as np
from light_pipe import processing
from osgeo import gdal, ogr

gdal.UseExceptions()
ogr.UseExceptions()


def get_pred(arr: np.ndarray) -> bool:
    return not np.allclose(arr, 0)


def get_savepath(uid) -> str:
    savepath = \
        './data/tile_preds/no_pipeline/tile_preds' + '_' + str(uid) + '.tif'
    return savepath


def main():
    ds = gdal.Open('./data/image.tif')
    inputs = [ds, [ds], [ds, ds], [ds, ds, ds]]

    for sample_id, datasets in enumerate(inputs):
        lp_sample = processing.LightPipeSample(uid=sample_id, data=datasets)
        tiles = lp_sample.tile()
        preds = list()
        for tile in tiles:
            pred = get_pred(tile.y)
            preds.append(pred)
        savepath = get_savepath(lp_sample.uid)
        lp_sample.save(savepath, preds)    


if __name__ == "__main__":
    main()
