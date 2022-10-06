__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains functions necessary to efficiently generate subsample
arrays from groups of raster datasets. Note: in Light-Pipe, _tile_ refers to
subsamples of raster arrays, whereas _grid cell_ refers to rectangular regions
of a coordinate reference. Confusingly, the Mercantile project refers to the 
latter as _tiles_.
"""


from typing import List, Optional, Tuple

import numpy as np
from osgeo import gdal

from light_pipe import gdal_data_handlers

gdal.UseExceptions()


def round_up(n: int, base: int) -> int:
    return int(n + (base - n) % base)


def get_tile_id_mapping(
    raster_y: int, raster_x: int, tile_y: int, tile_x: int, row_major=False,
    assert_evenly_divisble = True, *args, **kwargs
) -> np.ndarray:
    if assert_evenly_divisble:
        assert raster_y % tile_y == 0, \
            "`tile_y` must evenly divide `raster_y` when `assert_evenly_divisible == True`."
        assert raster_x % tile_x == 0, \
            "`tile_x` must evenly divide `raster_x` when `assert_evenly_divisible == True`."            
    y_tile_ids = np.arange(0, raster_y, tile_y)
    x_tile_ids = np.arange(0, raster_x, tile_x)
    if row_major:
        tile_coords = np.dstack(np.meshgrid(y_tile_ids, x_tile_ids)).reshape(-1, 2)
    else:
        tile_coords = np.dstack(np.meshgrid(x_tile_ids, y_tile_ids)).reshape(-1, 2)
    return tile_coords


def get_tile_id_mapping_from_dataset(
    datasets: List[gdal.Dataset], tile_y: int, tile_x: int, row_major=False,
    assert_evenly_divisble = True, *args, **kwargs
) -> np.ndarray:
    assert len(datasets) == 1, \
        f"Function excepts only one dataset at a time. Number of datasets passed: {len(datasets)}."
    dataset = datasets[0]
    raster_y = dataset.RasterYSize
    raster_x = dataset.RasterXSize
    tile_coords = get_tile_id_mapping(
        raster_y, raster_x, tile_y, tile_x, row_major, assert_evenly_divisble
    )
    return datasets, tile_coords


def get_tiles_from_padded_array(
    padded: np.ndarray, tile_coords: np.ndarray, tile_y: int, tile_x: int, 
    assert_evenly_divisble = True, *args, **kwargs
):
    if assert_evenly_divisble:
        assert padded.shape[-2] % tile_y == 0, \
            "`tile_y` must evenly divide `raster_y` when `assert_evenly_divisible == True`."
        assert padded.shape[-1] % tile_x == 0, \
            "`tile_x` must evenly divide `raster_x` when `assert_evenly_divisible == True`."
    for uly, ulx in tile_coords:
        lry = uly + tile_y
        lrx = ulx + tile_x
        tile = padded[:, uly:lry, ulx:lrx]
        yield tile


def get_padded_raster_array(
    raster_dataset: gdal.Dataset, tile_y: int, tile_x: int, padded = None, 
    channels_to_write: list = None, array_dtype = np.uint16, *args, **kwargs
) -> Tuple[np.ndarray, gdal.Dataset]:
    """
    If you want to add other bands to the dataset, you should pass `n_channels`
    so that the padded array does not need to be duplicated.
    """
    n_channels = raster_dataset.RasterCount
    raster_y = raster_dataset.RasterYSize
    raster_x = raster_dataset.RasterXSize

    if padded is None:
        padded_y = round_up(raster_y, tile_y)
        padded_x = round_up(raster_x, tile_x)

        padded = np.zeros((n_channels, padded_y, padded_x), dtype=array_dtype)   
        padded[:, :raster_y, :raster_x] = raster_dataset.ReadAsArray()
    else:
        assert channels_to_write is not None, \
            "`channels_to_write` must be passed when `padded` is not None."
        assert len(channels_to_write) == n_channels, \
            "`channels_to_write` should have length equal to the number of bands in the target raster."
        padded[channels_to_write, :raster_y, :raster_x] = raster_dataset.ReadAsArray()
    return raster_dataset, padded


def get_padded_array_from_multiple_datasets(
    datasets: List[gdal.Dataset], labels: List[bool],
    tile_y: int, tile_x: int, 
    array_dtype = np.uint16, *args, **kwargs
) -> Tuple[List[gdal.Dataset], np.ndarray]:
    dataset_bands_list = []
    n_bands_total = 0
    for raster_dataset in datasets:
        n_bands = raster_dataset.RasterCount
        dataset_bands_list.append(n_bands)
        n_bands_total += n_bands

    raster_y = raster_dataset.RasterYSize
    raster_x = raster_dataset.RasterXSize

    padded_y = round_up(raster_y, tile_y)
    padded_x = round_up(raster_x, tile_x)

    padded = np.zeros((n_bands_total, padded_y, padded_x), dtype=array_dtype) 

    curr_band = 0
    band_map = {
        True: list(),
        False: list(),
    }
    for i in range(len(datasets)):
        raster_dataset = datasets[i]
        n_bands = dataset_bands_list[i]
        channels_to_write = [curr_band + i for i in range(n_bands)]
        if labels is not None:
            label = labels[i]
        else:
            label = False
        band_map[label] += channels_to_write
        curr_band += n_bands
        raster_dataset, padded = get_padded_raster_array(
            raster_dataset, tile_y, tile_x, padded, channels_to_write
        )
    return datasets, padded, band_map


@gdal_data_handlers.open_data
def get_tiles(
    datasets: List[gdal.Dataset], labels: List[bool], tile_y: int, 
    tile_x: int, array_dtype, row_major: bool, 
    tile_coords = None, shuffle_tiles: Optional[bool] = False, 
    assert_tile_smaller_than_raster: Optional[bool] = False,
    *args, **kwargs
):
    datasets, padded, band_map = get_padded_array_from_multiple_datasets(
        datasets, labels, tile_y, tile_x, array_dtype, **kwargs
    )
    raster_y = padded.shape[-2]
    raster_x = padded.shape[-1]
    if assert_tile_smaller_than_raster:
        assert tile_y <= raster_y, \
            f"Tile y size {tile_y} is larger than raster y size {raster_y}."
        assert tile_x <= raster_x, \
            f"Tile x size {tile_x} is larger than raster x size {raster_x}."
    if tile_coords is None:
        tile_coords = get_tile_id_mapping(
            raster_y, raster_x, tile_y, tile_x, row_major, 
            assert_evenly_divisble=True
        )
    if shuffle_tiles:
        shuffle_indices = np.arange(tile_coords.shape[0])
        np.random.shuffle(shuffle_indices)
        tile_coords = tile_coords[shuffle_indices]
    else:
        shuffle_indices = None
    tiles = get_tiles_from_padded_array(
        padded, tile_coords, tile_y, tile_x, assert_evenly_divisble=True, 
        *args, **kwargs
    )
    return datasets, tiles, tile_coords, shuffle_indices, band_map
