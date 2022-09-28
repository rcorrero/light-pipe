__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains utility functions which facilitate file I/O.
"""


import json
import os
import shutil
from typing import List, Optional

import numpy as np
from osgeo import gdal, ogr

gdal.UseExceptions()
ogr.UseExceptions()

VECTOR_FILEPATH_EXTENSIONS = [".shp", ".geojson"]


def file_is_a(filepath, extension) -> bool:
    _, filepath_extension = os.path.splitext(filepath)
    return filepath_extension == extension


def file_is_a_vector_file(filepath, vector_extensions = VECTOR_FILEPATH_EXTENSIONS):
    for ext in vector_extensions:
        if file_is_a(filepath, ext):
            return True
    return False


def remove(path, *args, **kwargs):
    """ param <path> could either be relative or absolute. """
    if os.path.isfile(path) or os.path.islink(path):
        os.remove(path)  # remove the file
    elif os.path.isdir(path):
        shutil.rmtree(path)  # remove dir and all contains
    else:
        raise ValueError("file {} is not a file or dir.".format(path))


def save_dict_as_json(out_dict: dict, out_path, *args, **kwargs):
    with open(out_path, "w") as f:
        json.dump(out_dict, f)


def make_dataset(
    driver, filepath, raster_x_size, raster_y_size, n_bands, dtype, geotransform,
    projection, *args, **kwargs
) -> gdal.Dataset:
    dataset = driver.Create(filepath, raster_x_size, raster_y_size, n_bands, dtype)
    dataset.SetGeoTransform(geotransform)
    dataset.SetProjection(projection)
    return dataset


def write_array_to_dataset(
    array: np.ndarray, dataset: gdal.Dataset, *args, **kwargs
) -> gdal.Dataset:
    dataset.WriteArray(array)
    return dataset


def get_shapefile_attributes(
    vector_datasource: ogr.DataSource, *args, **kwargs
) -> List[str]:
    layer = vector_datasource.GetLayer()
    attributes = []
    ldefn = layer.GetLayerDefn()
    for n in range(ldefn.GetFieldCount()):
        fdefn = ldefn.GetFieldDefn(n)
        attributes.append(fdefn.name)
    return vector_datasource, attributes


def get_descendant_filepath(
    ancestor_filepath, descendant_filepath_suffix, descendant_directory = None,
    descendant_extension = None, *args, **kwargs
) -> str:
    ancestor_file_extension = os.path.splitext(ancestor_filepath)[-1]
    ancestor_file_basename = os.path.split(ancestor_filepath)[-1].split(".")[0]
    ancestor_parent_dirs = os.path.split(ancestor_filepath)[0]
    if descendant_extension is None:
        descendant_extension = ancestor_file_extension
    assert descendant_extension[0] == ".", \
        f"Extension must start with period. Passed extension: {descendant_extension}"
    
    if descendant_directory is not None:
        if not os.path.exists(descendant_directory):
            os.makedirs(descendant_directory)
        assert descendant_directory[0] != "/"
        assert descendant_directory[-1] == "/"
        out_filepath = f"{descendant_directory}{ancestor_file_basename}_{descendant_filepath_suffix}{descendant_extension}"
    else:
        out_filepath = f"{ancestor_parent_dirs}/{ancestor_file_basename}_{descendant_filepath_suffix}{descendant_extension}" 
    return out_filepath


def get_grid_cell_filepath(
        ancestor_filepath: str, qkey: str, directory: str, 
        extension: Optional[str] = ".tif", *args, **kwargs
):
    ancestor_file_basename = os.path.split(ancestor_filepath)[-1].split(".")[0]
    ancestor_parent_dirs = os.path.split(ancestor_filepath)[0]
    assert extension[0] == ".", \
        f"Extension must start with period. Passed extension: {extension}"
    
    if directory is not None:
        if directory[-1] != "/":
            directory += "/"
        directory += ancestor_file_basename + "/"
        if not os.path.exists(directory):
            try:
                os.makedirs(directory)
            except FileExistsError:
                pass
    else:
        directory = ancestor_parent_dirs + "/" + ancestor_file_basename + "/"
        if not os.path.exists(directory):
            os.makedirs(directory)
    out_filepath = f"{directory}{qkey}{extension}" 
    return out_filepath
