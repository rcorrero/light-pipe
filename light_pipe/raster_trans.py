__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains functions which operate on `gdal.Dataset` instances, 
`ogr.DataSource` instances, or create instances of these types.
"""


import os
from typing import Callable, List, Optional, Tuple, Union

import numpy as np
from osgeo import gdal, ogr, osr

from light_pipe import gdal_data_handlers, raster_io, tiling

gdal.UseExceptions()
ogr.UseExceptions()
osr.UseExceptions()


def translate_dataset(
    dataset: gdal.Dataset, filepath: str, raster_x_size: int,
    raster_y_size: int, n_bands: int, dtype, geotransform, projection, driver_name,
    srs: osr.SpatialReference, ulx: Union[float, int], uly: Union[float, int], 
    lrx: Union[float, int], lry: Union[float, int], *args, **kwargs
):
    proj_win = [ulx, uly, lrx, lry]
    band_list = [i for i in range(1, n_bands + 1)]
    out_dataset = gdal.Translate(
        filepath, dataset,
        format=driver_name, outputType=dtype, bandList=band_list, 
        width=raster_x_size, height=raster_y_size, projWin=proj_win, 
        projWinSRS=srs, outputBounds=proj_win,
    )
    out_dataset.SetGeoTransform(geotransform)
    out_dataset.SetProjection(projection)
    return dataset, out_dataset


def rasterize_datasource(
    vector_datasource: ogr.DataSource, out_dataset: gdal.Dataset, 
    out_bands = [1], vector_layer_index = 0, 
    vector_attribute = None, out_inverse = False, out_all_touched = True,
    *args, **kwargs
) -> Tuple[gdal.Dataset, ogr.DataSource, gdal.Dataset]:
    out_layer = vector_datasource.GetLayerByIndex(vector_layer_index)

    options = []
    if vector_attribute is not None:
        vector_datasource, attributes = raster_io.get_shapefile_attributes(vector_datasource)
        assert vector_attribute in attributes, \
            f"Attribute {vector_attribute} not found in vector file {vector_datasource.GetDescription()}."
        options.append(f"ATTRIBUTE={vector_attribute}")
    if out_inverse:
        options.append(f"OUT_INVERSE=TRUE")
    if out_all_touched:
        options.append(f"ALL_TOUCHED=TRUE")

    gdal.RasterizeLayer(out_dataset, out_bands, out_layer, options=options)

    return vector_datasource, out_dataset
    

@gdal_data_handlers.open_data
def rasterize_datasources(
    dataset: gdal.Dataset, datasources: List[ogr.DataSource],
    in_memory: Optional[bool] = True, return_filepaths: Optional[bool] = False,
    out_dtype = gdal.GDT_Byte, out_bands = [1], 
    out_dir = None, filepath_generator: Optional[Callable] = None, *args, **kwargs
):
    metadata = {**kwargs, "args":args}
    out_n_bands = len(out_bands)
    item_filepath = dataset.GetDescription()
    item_uid = os.path.basename(item_filepath)
    if in_memory:
        driver = gdal.GetDriverByName("MEM")
    else:
        driver = dataset.GetDriver()
    raster_x_size = dataset.RasterXSize
    raster_y_size = dataset.RasterYSize
    geotransform = dataset.GetGeoTransform()
    projection = dataset.GetProjection()
    datasets = []
    for vector_datasource in datasources:
        if in_memory:
            out_filepath = ""
        else:
            datasource_path = vector_datasource.GetDescription()
            uid = os.path.basename(datasource_path)
            if filepath_generator is not None:
                out_filepath = filepath_generator(
                    item_filepath=item_filepath, uid=uid, out_dir=out_dir
                )
            out_filepath = raster_io.get_descendant_filepath(
                item_filepath, uid, out_dir
            )

        # Create output dataset
        out_dataset = raster_io.make_dataset(
            driver, out_filepath, raster_x_size, raster_y_size, out_n_bands, 
            out_dtype, geotransform, projection
        )

        vector_datasource, out_dataset = rasterize_datasource(
            vector_datasource, out_dataset, out_bands, *args, **kwargs
        )

        datasets.append(out_dataset)
        if return_filepaths:
            yield item_uid, (out_filepath, True, kwargs)
        else:
            yield item_uid, (out_dataset, True, kwargs)

    if return_filepaths:
        yield item_uid, (item_filepath, False, metadata)
    else:
        yield item_uid, (dataset, False, metadata)


@gdal_data_handlers.open_data
def make_north_up_dataset_from_tiles_like(
    datasets: List[gdal.Dataset], filepath: str, tiles: np.ndarray, tile_y: int, 
    tile_x: int, row_major = False, use_ancestor_pixel_size = False, 
    pixel_x_size = None, pixel_y_size = None, n_bands = 1, 
    dtype = gdal.GDT_Byte, assert_north_up: Optional[bool] = True,
    out_driver: Optional[str] = "GTiff", *args, **kwargs
) -> Tuple[List[gdal.Dataset], gdal.Dataset]:
    def write_tiles_to_dataset(
        dataset: gdal.Dataset, tiles: np.ndarray, out_raster_y_size: int,
        out_raster_x_size: int, row_major = False, *args, **kwargs
    ) -> gdal.Dataset:
        if row_major:
            tiles = tiles.reshape(out_raster_y_size, out_raster_x_size)
        else:
            tiles = tiles.reshape(out_raster_x_size, out_raster_y_size).T
        dataset = raster_io.write_array_to_dataset(tiles, dataset)
        return dataset


    if use_ancestor_pixel_size:
        gt_0, pixel_x_size, gt_2, gt_3, gt_4, pixel_y_size = datasets[0].GetGeoTransform()
        if assert_north_up:
            ancestor_filepath = datasets[0].GetDescription()
            assert abs(gt_2) <= 1e-16 and (gt_4) <= 1e-16, \
                f"Transformation coefficients are not equal to zero for dataset {ancestor_filepath}."
    else:
        assert pixel_y_size is not None
        assert pixel_x_size is not None
        gt_0, _, _, gt_3, _, _ = datasets[0].GetGeoTransform()
    out_projection = datasets[0].GetProjection()
    out_geotransform = (
        gt_0, tile_x * pixel_x_size, 0.0, gt_3, 0.0, tile_y * pixel_y_size
    )
    raster_y = datasets[0].RasterYSize
    raster_x = datasets[0].RasterXSize

    padded_y = tiling.round_up(raster_y, tile_y)
    padded_x = tiling.round_up(raster_x, tile_x)
    out_raster_y_size = padded_y // tile_y
    out_raster_x_size = padded_x // tile_x

    if isinstance(out_driver, str):
        out_driver = gdal.GetDriverByName(out_driver)

    out_dataset = raster_io.make_dataset(
        out_driver, filepath, out_raster_x_size, out_raster_y_size, n_bands,
        dtype, out_geotransform, out_projection
    )

    out_dataset = write_tiles_to_dataset(
        out_dataset, tiles, out_raster_y_size, out_raster_x_size, row_major
    )
    return datasets, out_dataset
