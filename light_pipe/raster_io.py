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

# LIGHT_PIPE_UID_KEY = "LIGHT_PIPE_UID"
# LIGHT_PIPE_ANCESTORS_KEY = "LIGHT_PIPE_ANCESTORS"
# LIGHT_PIPE_DESCENDANTS_KEY = "LIGHT_PIPE_DESCENDANTS"

# LIGHT_PIPE_PREDICTION_KEY = "LIGHT_PIPE_PREDICTION"

# MANIFEST_FILEPATH_KEY = "FILEPATH"


class LightPipeUIDNotFoundError(Exception):
    """
    Raised when a gdal.Dataset instance has not 
    """


def file_is_a(filepath, extension) -> bool:
    _, filepath_extension = os.path.splitext(filepath)
    return filepath_extension == extension


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


# def open_manifest_json(
#     json_path, manifest_filepath_key = MANIFEST_FILEPATH_KEY, 
#     descendants_key = LIGHT_PIPE_DESCENDANTS_KEY, *args, **kwargs
# ):
#     with open(json_path, "r") as f:
#         items_dict = json.load(f)
#         for item_uid, item_data_dict in items_dict.items():
#             uids = []
#             uids.append(item_uid)
#             datasets = []
#             item_filepath = item_data_dict[manifest_filepath_key]
#             datasets.append(item_filepath)

#             descendants_data_dict = item_data_dict[descendants_key]
#             for descendant_uid, descendant_filepath in descendants_data_dict.items():
#                 uids.append(descendant_uid)
#                 datasets.append(descendant_filepath)

#             kwargs = {
#                 "datasets": datasets,
#                 "uids": uids
#             }
#             yield kwargs


# def load_items_from_json_manifest(
#     q: queue.Queue, manifest_path, manifest_filepath_key = MANIFEST_FILEPATH_KEY, 
#     descendants_key = LIGHT_PIPE_DESCENDANTS_KEY, *args, **kwargs
# ):
#     items = open_manifest_json(manifest_path, manifest_filepath_key, descendants_key)
#     for item in items:
#         q.put_nowait(item)


# def open_manifest_csv(csv_path, *args, **kwargs):
#     with open(csv_path, 'r') as f:
#         reader = csv.reader(f, delimiter=",")
#         for i, line in enumerate(reader):
#             assert len(line) > 1
#             item_filepath = line[0]
#             vector_filepaths = line[1:]
#             assert vector_filepaths[-1] is not None, \
#                 f"Final item in line {i} is null."
#             kwargs = {
#                 "datasets": [item_filepath],
#                 "datasources": vector_filepaths
#             }
#             yield kwargs


# def load_items_from_csv_manifest(q: queue.Queue, manifest_path, *args, **kwargs):
#     items = open_manifest_csv(manifest_path)
#     for item in items:
#         q.put_nowait(item)


def make_dataset(
    driver, filepath, raster_x_size, raster_y_size, n_bands, dtype, geotransform,
    projection, *args, **kwargs
) -> gdal.Dataset:
    dataset = driver.Create(filepath, raster_x_size, raster_y_size, n_bands, dtype)
    dataset.SetGeoTransform(geotransform)
    dataset.SetProjection(projection)
    return dataset

# # @TODO: REIMPLEMENT USING COROUTINES FOR IO
# def open_dataset(filepath, *args, **kwargs) -> gdal.Dataset:
#     dataset = gdal.Open(filepath)
#     return dataset

# # @TODO: REIMPLEMENT USING COROUTINES FOR IO
# def close_dataset(dataset: gdal.Dataset, *args, **kwargs) -> None:
#     dataset.FlushCache()
#     del(dataset)

# # @TODO: REIMPLEMENT USING COROUTINES FOR IO
# def open_datasource(filepath, *args, **kwargs) -> ogr.DataSource:
#     datasource = ogr.Open(filepath)
#     return datasource

# # @TODO: REIMPLEMENT USING COROUTINES FOR IO
# def close_datasource(datasource: ogr.DataSource, *args, **kwargs) -> None:
#     datasource.FlushCache()
#     del(datasource)


def write_array_to_dataset(
    array: np.ndarray, dataset: gdal.Dataset, *args, **kwargs
) -> gdal.Dataset:
    dataset.WriteArray(array)
    return dataset


# def add_uid_to_metadata(
#     raster_dataset: gdal.Dataset, uid: Optional[Union[None, str]] = None, 
#     uid_key = LIGHT_PIPE_UID_KEY, *args, **kwargs
# ) -> gdal.Dataset:
#     metadata = raster_dataset.GetMetadata()
#     if uid_key not in metadata.keys():
#         if uid is None:
#             uid = tiling.get_uid()
#         metadata[uid_key] = uid
#         raster_dataset.SetMetadata(metadata)
#     return raster_dataset


# def get_uid_from_dataset(
#     raster_dataset: gdal.Dataset, uid_key = LIGHT_PIPE_UID_KEY, *args, **kwargs
# ) -> str:
#     metadata = raster_dataset.GetMetadata()
#     if uid_key not in metadata.keys():
#         raise LightPipeUIDNotFoundError(f"UID key {uid_key} not found in metadata.")
#     uid = metadata[uid_key]
#     return uid    
    

# def add_ancestor_reference_to_descendant_metadata(
#     ancestor_dataset: gdal.Dataset, descendant_dataset: gdal.Dataset,
#     ancestors_key = LIGHT_PIPE_ANCESTORS_KEY, uid_key = LIGHT_PIPE_UID_KEY,
#     *args, **kwargs
# ) -> Tuple[gdal.Dataset, gdal.Dataset]:
#     # Add uids to metadata if not present
#     ancestor_dataset = add_uid_to_metadata(ancestor_dataset)
#     descendant_dataset = add_uid_to_metadata(descendant_dataset)

#     ancestor_metadata = ancestor_dataset.GetMetadata()
#     descendant_metadata = descendant_dataset.GetMetadata()

#     ancestor_uid = ancestor_metadata[uid_key]

#     now = time.time()

#     if ancestors_key in descendant_metadata.keys():
#         ancestors_dict_str = descendant_metadata[ancestors_key]
#         ancestors_dict = json.loads(ancestors_dict_str)
#         ancestors_dict[ancestor_uid] = now
#     else:
#         ancestors_dict = {
#             ancestor_uid: now
#         }

#     descendant_metadata[ancestors_key] = json.dumps(ancestors_dict)
    
#     # Update metadata
#     ancestor_dataset.SetMetadata(ancestor_metadata)
#     descendant_dataset.SetMetadata(descendant_metadata)

#     return ancestor_dataset, descendant_dataset


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
