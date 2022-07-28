import json
import os
import time
from collections.abc import Iterable
from pathlib import Path
from typing import Generator, List, Optional, Tuple, Union

import numpy as np
from osgeo import gdal

from ..utils import exec_command, make_dir_path, rasterize_label
from .storage import PathItem, Tracker


class Transformer:
    def __init__(self, input: Optional[Union[PathItem, Tracker]] = None,
                 output: Optional[Union[PathItem, Tracker]] = None,
                 verbose: Optional[bool] = False):
        self.input = input
        self.output = output
        self.verbose = verbose


    def transform(self, input: Optional[Union[PathItem, Tracker]] = None,
                  output: Optional[Union[PathItem, Tracker]] = None,
                  depth_first: Optional[bool] = True):
        input, output = self._check_input_output(input, output)
        assert isinstance(input, Iterable), ("Input must be an object of "
            + "Iterable type.")
        if not depth_first:
            return self._transform_breadth_first(input, output)
        return self._transform_depth_first(input, output)


    def _transform(self, item: PathItem) -> Union[Iterable, PathItem]:
        pass


    def _transform_breadth_first(self, input, output) -> Union[Tracker, PathItem]:
        for i, item in enumerate(input):
            assert isinstance(item, PathItem), "Input must be a PathItem object."
            if self.verbose:
                print('Transforming item %d...' % i)
            item = self._transform(item)
            if not isinstance(item, PathItem) and isinstance(item, Iterable):
                for sub_item in item:
                    output.set_record(sub_item)
            else:
                output.set_record(item)
        return output


    def _transform_depth_first(self, input, output) -> Iterable:
        for i, item in enumerate(input):
            assert isinstance(item, PathItem), "Input must be a PathItem object."
            if self.verbose:
                print('Transforming item %d...' % i)
            item = self._transform(item)
            if not isinstance(item, PathItem) and isinstance(item, Iterable):
                for sub_item in item:
                    yield sub_item
            else:
                yield item

    
    def _check_input_output(self, input: Optional[Union[PathItem, Tracker]] = None,
                            output: Optional[Union[PathItem, Tracker]] = None) -> Tuple:
        if input is None:
            assert self.input is not None, "No input provided."
            input = self.input
        if self.output is not None:
            output = self.output
        elif output is None and self.output is None:
            output = input
        return input, output


class TargetRasterizer(Transformer):
    def __init__(self,  input: Optional[Union[PathItem, Tracker]] = None,
                 output: Optional[Union[PathItem, Tracker]] = None,
                 rasterized_target_path_ext: Optional[str] = '_rasterized',
                 otype: Optional[str] = 'GTiff',
                 attribute: Optional[Union[str, None]] = None,
                 verbose: Optional[bool] = False,
                 time_transform: Optional[bool] = False):
        self.input = input
        self.output = output
        self.rasterized_target_path_ext = rasterized_target_path_ext
        self.otype = otype
        self.attribute = attribute
        self.verbose = verbose
        self.time_transform = time_transform


    def _transform(self, item: PathItem) -> PathItem:
        if self.time_transform:
            start = time.time()
        item_paths = item.get_paths()
        assert 'image_path' in item_paths.keys()
        assert 'target_path' in item_paths.keys()
        image_path = item_paths['image_path']
        target_path = item_paths['target_path']

        rasterized_path = self._make_rasterized_path(image_path)

        self._rasterize_target(image_path, target_path, rasterized_path, 
                               otype = self.otype, attribute = self.attribute)
        item_paths['rasterized_target_path'] = rasterized_path

        item_data = item.get_data()
        item_data['target_is_rasterized'] = True

        item.set_paths(item_paths) # Unnecessary for vanilla ItemPath objs
        item.set_data(item_data) # Unnecessary for vanilla ItemPath objs
        if self.time_transform:
            end = time.time()
            runtime = end - start
            print('TargetRasterizer runtime: %f' % runtime)
        return item


    def _make_rasterized_path(self, image_path: os.PathLike) -> os.PathLike:
        ext = self.rasterized_target_path_ext
        # if type(image_path) == str:
        if isinstance(image_path, str):
            image_path = Path(image_path)
        rasterized_path = image_path.with_name(image_path.stem 
            + ext + image_path.suffix)
        return str(rasterized_path).replace("\\","/")


    def _rasterize_target(self, image_path: os.PathLike,
                          target_path: os.PathLike, 
                          rasterized_path: os.PathLike,
                          otype: Optional[str] = 'GTiff', 
                          attribute: Optional[Union[str, None]] = None) -> None:
        image_path = str(image_path)
        target_path = str(target_path)
        rasterized_path = str(rasterized_path)
        rasterize_label(sample_file=image_path, vector_file=target_path,
                        attribute=attribute, otype=otype, 
                        fname_out=rasterized_path)


class Tiler(Transformer):
    def __init__(self, tile_dir_path: os.PathLike, tile_size_x: int, 
                 tile_size_y: int, input: Optional[Union[PathItem, Tracker]] = None,
                 output: Optional[Union[PathItem, Tracker]] = None,
                 tile_target_sub_dir: Optional[str] = 'targets/',
                 tile_image_sub_dir: Optional[str] = 'images/',
                 verbose: Optional[bool] = False,
                 time_transform: Optional[bool] = False):        
        self.tile_dir_path = tile_dir_path
        self.tile_size_x = tile_size_x
        self.tile_size_y = tile_size_y
        self.input = input
        self.output = output
        self.tile_target_sub_dir = tile_target_sub_dir
        self.tile_image_sub_dir = tile_image_sub_dir
        self.verbose = verbose
        self.time_transform = time_transform


    def transform(self, input: Optional[Union[PathItem, Tracker]] = None,
                  output: Optional[Union[PathItem, Tracker]] = None,
                  depth_first: Optional[bool] = True):
        make_dir_path(self.tile_dir_path)
        return super().transform(input, output, depth_first)


    def _transform(self, item: PathItem) -> Iterable:
        # make_dir_path(self.tile_dir_path) 
        # all_tile_items = [] # Stores tile items for each item
        # for item in input:
        if self.time_transform:
            start = time.time()
        item_paths = item.get_paths()
        item_data = item.get_data()

        assert 'item_id' in item_data.keys()
        item_id = item_data['item_id']
        item_sub_dir = item_id + '/'
        item_tile_dir_path = os.path.join(self.tile_dir_path, 
            item_sub_dir).replace("\\","/")
        make_dir_path(item_tile_dir_path)

        item_paths['tile_dir_path'] = item_tile_dir_path
        # item_data['is_tile'] = False

        target_tile_items = None
        image_tile_items = None
        if 'rasterized_target_path' in item_paths.keys():
            assert item_data['target_is_rasterized'] == True
            item_tile_target_dir_path = os.path.join(item_tile_dir_path, 
                self.tile_target_sub_dir).replace("\\","/")
            make_dir_path(item_tile_target_dir_path)
            rasterized_target_path = item_paths['rasterized_target_path']
            target_tile_items = self._tile_raster(rasterized_target_path,
                item_tile_target_dir_path, item_id)
            # tile_items = list(target_tile_items.values())
            item_data['target_is_tiled'] = True

        if 'image_path' in item_paths.keys():
            item_tile_image_dir_path = os.path.join(item_tile_dir_path, 
                self.tile_image_sub_dir).replace("\\","/")
            make_dir_path(item_tile_image_dir_path)
            image_path = item_paths['image_path']
            image_tile_items = self._tile_raster(image_path,  
                item_tile_image_dir_path, item_id)
            # tile_items = list(image_tile_items.values())
            item_data['image_is_tiled'] = True

        if target_tile_items is not None and image_tile_items is not None:
            # Join the PathItem objects on item_id keys
            for image_tile_item, target_tile_item in zip(image_tile_items, target_tile_items):
                # assert key in target_tile_items
                # image_tile_item = image_tile_items[key]
                # target_tile_item = target_tile_items[key]

                target_tile_item_paths = target_tile_item.get_paths()
                target_tile_item_path = target_tile_item_paths['image_path']
                image_tile_item_paths = image_tile_item.get_paths()
                image_tile_item_paths['rasterized_target_path'] = target_tile_item_path

                image_tile_item_data = image_tile_item.get_data()
                image_tile_item_data['target_is_rasterized'] = True

                image_tile_item.set_paths(image_tile_item_paths)
                image_tile_item.set_data(image_tile_item_data)

                tile_item = image_tile_item
                yield tile_item
            # tile_items = list(image_tile_items.values())
        else:
            tile_items = image_tile_items
            for tile_item in tile_items:
                yield tile_item

        # all_tile_items += tile_items

        item.set_paths(item_paths) # Unnecessary for vanilla ItemPath objs
        item.set_data(item_data) # Unnecessary for vanilla ItemPath objs
        if self.time_transform:
            end = time.time()
            runtime = end - start
            print('Tiler runtime: %f' % runtime)

    def _tile_raster(self, raster_path: os.PathLike, 
                     item_tile_dir_path: os.PathLike, item_id, 
                     ext: Optional[str] = '.tif') -> Generator:
        ds = gdal.Open(raster_path)
        band = ds.GetRasterBand(1)
        xsize = band.XSize
        ysize = band.YSize
        # tile_items = {} # dict of PathItem objects
        # @TODO: Implement following loop more efficiently
        for i in range(0, xsize, self.tile_size_x):
            for j in range(0, ysize, self.tile_size_y):

                tile_id = str(item_id) + '_' + str(i) + '_' + str(j)
                tile_out_path = os.path.join(item_tile_dir_path, 
                    tile_id + ext).replace("\\","/")
                com_string = ("gdal_translate -q -of GTIFF -srcwin " + str(i) 
                    + ", " + str(j) + ", " + str(self.tile_size_x) + ", " 
                    + str(self.tile_size_y) + ", " + raster_path + " " 
                    + tile_out_path)
                exec_command(com_string)

                # Make PathItem object for tile
                tile_data = {
                    'item_id': tile_id,
                    'parent_item_id': item_id,
                    'is_tile': True,
                    'tile_ulx': i,
                    'tile_uly': j,
                    'tile_size_x': self.tile_size_x,
                    'tile_size_y': self.tile_size_y,
                }
                tile_paths = {
                    'image_path': tile_out_path,
                }
                tile_item = PathItem(paths = tile_paths, data = tile_data)
                # tile_items[tile_id] = tile_item
                yield tile_item
        ds = None


class PlanetPathFinder(Transformer):
    def __init__(self, order_dir_path: os.PathLike, 
                 input: Optional[Union[PathItem, Tracker]] = None,
                 output: Optional[Union[PathItem, Tracker]] = None,
                 target_asset_type: Optional[str] = 'analytic_sr', 
                 manifest_filename: Optional[str] = 'manifest.json',
                 verbose: Optional[bool] = False,
                 time_transform: Optional[bool] = False):
        self.order_dir_path = order_dir_path
        self.input = input
        self.output = output
        self.target_asset_type = target_asset_type
        self.manifest_filename = manifest_filename
        self.verbose = verbose
        self.time_transform = time_transform
        self.order_manifest_path = os.path.join(self.order_dir_path, 
            self.manifest_filename).replace("\\","/")
        with open(self.order_manifest_path) as f_in:
            self.manifest = json.load(f_in)


    def _transform(self, item: PathItem) -> PathItem:
        # input, output = self._check_input_output(input, output)
        # assert isinstance(input, PathItem), "Input must be a PathItem object."
        # item = input
        for file_info in self.manifest['files']:
            if 'planet/asset_type' in file_info['annotations'].keys():
                asset_type = file_info['annotations']['planet/asset_type']
                if asset_type == self.target_asset_type:
                    # get item id
                    file_item_id = file_info['annotations']['planet/item_id']

                    item_data = item.get_data()
                    item_id = item_data['item_id']

                    if file_item_id == item_id:
                        image_path = os.path.join(self.order_dir_path, 
                            file_info['path']).replace("\\","/")

                        # Update item
                        item_paths = item.get_paths()
                        item_paths['image_path'] = image_path
                        item.set_paths(item_paths)
                        return item


class RasterLabeler(Transformer):
    def __init__(self,  input: Optional[Union[PathItem, Tracker]] = None,
                 output: Optional[Union[PathItem, Tracker]] = None,
                 target_value: Optional[int] = 255,
                 verbose: Optional[bool] = False,
                 time_transform: Optional[bool] = False):
        self.input = input
        self.output = output
        self.target_value = target_value
        self.verbose = verbose
        self.time_transform = time_transform


    def _transform(self, item: PathItem) -> PathItem:
        if self.time_transform:
            start = time.time()
        item_paths = item.get_paths()
        item_data = item.get_data()
        # Check whether any data in file at image_path
        if 'image_path' in item_paths.keys():
            image_path = item_paths['image_path']
            image_arr = self._load_arr(image_path)
            any_nonzero_px = np.any(image_arr > 0)
            item_data['nonzero_pixels'] = any_nonzero_px

        # Check whether self.target_value in file at target_path
        if 'target_is_rasterized' in item_data.keys():
            if 'rasterized_target_path' in item_paths.keys():
                target_path = item_paths['rasterized_target_path']
                target_arr = self._load_arr(target_path)
                any_target_px = np.any(target_arr == self.target_value)
                item_data['positive_target'] = any_target_px
                item.set_data(item_data)
        if self.time_transform:
            end = time.time()
            runtime = end - start
            print('RasterLabeler runtime: %f' % runtime)
        return item


    def _load_arr(self, arr_path: os.PathLike) -> np.array:
        ds = gdal.Open(arr_path)
        arr = np.array(ds.GetRasterBand(1).ReadAsArray())
        ds = None
        return arr


class Pipeline(Transformer):
    def __init__(self, transformers: List[Transformer], 
                 input: Optional[Union[PathItem, Tracker]] = None,
                 output: Optional[Union[PathItem, Tracker]] = None,
                 verbose: Optional[bool] = False,
                 time_transform: Optional[bool] = False):
        self.transformers = transformers
        self.input = input
        self.output = output
        self.verbose = verbose
        self.time_transform = time_transform


    def transform(self, input: Optional[Union[PathItem, Tracker]] = None, 
                  output: Optional[Union[PathItem, Tracker]] = None,
                  depth_first: Optional[bool] = True):
        input, output = self._check_input_output(input, output)
        for transformer in self.transformers:
            input = transformer.transform(input, depth_first=depth_first)
        for item in input:
            output.set_record(item)
        return output
