__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains the definition of `LightPipeSample`, a key component of the
Light-Pipe API. `LightPipeSample` instances are the fundamental unit of data
which are generated by `LightPipeline` instances.
"""


from collections import namedtuple
from typing import Dict, Optional, Sequence, Tuple, Union, Generator

import numpy as np
from light_pipe import raster_trans, raster_io, tiling
from osgeo import gdal

gdal.UseExceptions()


class LightPipeTile(namedtuple("LightPipeTile", ["X", "y", "band_map"])):
    def __new__(cls, X, y, band_map):
        # @TODO: IMPLEMENT
        return tuple.__new__(cls, [X, y, band_map])


class LightPipeSample:
    """
    Serves analysis-ready subsamples from arbitrarily-large raster(s) and 
    contains necessary variables and methods to format "predictions" into
    georeferenced files.
    """
    def __init__(
        self, uid: Union[int, float, str], 
        data: Sequence[Tuple[gdal.Dataset, bool, Dict]], 
        preds: Optional[Sequence] = None, pos_only: Optional[bool] = False, 
        non_null_only: Optional[bool] = False, tile_y: Optional[int] = 224, 
        tile_x: Optional[int] = 224, array_dtype = np.uint16, 
        row_major: Optional[bool] = False, tile_coords: Optional[Sequence] = None,
        shuffle_indices: Optional[Sequence] = None
    ):
        self.uid = uid
        datasets = list()
        labels = list() # Used to choose samples for y
        metadata_list = list()
        for dataset, is_label, metadata in data:
            datasets.append(dataset)
            labels.append(is_label)
            metadata_list.append(metadata)
        self.datasets = datasets
        self.labels = labels
        self.metadata_list = metadata_list

        self.preds = preds
        self.pos_only = pos_only
        self.non_null_only = non_null_only
        self.tile_y = tile_y 
        self.tile_x = tile_x
        self.array_dtype = array_dtype 
        self.row_major = row_major
        self.tile_coords = tile_coords
        self.shuffle_indices = shuffle_indices

        self.n = len(datasets)
        self._i = 0


    def __iter__(self):
        return self


    def __next__(self):
        return self.next()


    def next(self):
        i = self._i
        n = self.n
        if i < n:
            self._i += 1
            dataset = self.datasets[i]
            is_label = self.labels[i]
            metadata = self.metadata_list[i]
            return dataset, is_label, metadata
        raise StopIteration

    
    def shuffle(
        self, *args, **kwargs
    ) -> None:
        yield from self.tile(shuffle_tiles=True, *args, **kwargs)


    def unshuffle(
        self, preds: Optional[Union[Sequence, np.ndarray]] = None,
        shuffle_indices: Optional[np.ndarray] = None
    ):
        if preds is None:
            preds = self.preds
        assert preds is not None, \
            "`self.preds` instance variable must be set if `preds` not passed as parameter."
        if not isinstance(preds, np.ndarray):
            preds = np.array(preds)  
        if shuffle_indices is None and self.shuffle_indices is not None:
            shuffle_indices = self.shuffle_indices
        if shuffle_indices is not None:
            unshuffle_indices = np.zeros_like(shuffle_indices)
            unshuffle_indices[shuffle_indices] = np.arange(len(shuffle_indices))
            preds = preds[unshuffle_indices]
        return preds


    def tile(
        self, tile_y: Optional[int] = None, tile_x: Optional[int] = None, 
        array_dtype = None, row_major: Optional[bool] = None, 
        tile_coords = None, pos_only: Optional[bool] = None, 
        non_null_only: Optional[bool] = None, 
        shuffle_tiles: Optional[bool] = False, *args, **kwargs
    ) -> Generator:
        datasets = self.datasets
        labels = self.labels
        if tile_y is None:
            tile_y = self.tile_y
        else:
            self.tile_y = tile_y
        if tile_x is None:
            tile_x = self.tile_x
        else:
            self.tile_x = tile_x
        if array_dtype is None:
            array_dtype = self.array_dtype
        else:
            self.array_dtype = array_dtype
        if row_major is None:
            row_major = self.row_major
        else:
            self.row_major = row_major
        if tile_coords is None:
            tile_coords = self.tile_coords
        else:
            self.tile_coords = tile_coords
        if pos_only is None:
            pos_only = self.pos_only
        else:
            self.pos_only = pos_only
        if non_null_only is None:
            non_null_only = self.non_null_only
        else:
            self.non_null_only = non_null_only

        datasets, tiles, tile_coords, shuffle_indices, band_map = tiling.get_tiles(
            datasets=datasets, labels=labels, tile_y=tile_y, tile_x=tile_x, 
            array_dtype=array_dtype, row_major=row_major, tile_coords=tile_coords, 
            shuffle_tiles=shuffle_tiles, *args, **kwargs
        )
        self.tile_coords = tile_coords
        self.shuffle_indices = shuffle_indices
        self.band_map = band_map
        for tile_array in tiles:
            X = tile_array[band_map[False]]
            y = tile_array[band_map[True]]

            if np.allclose(y, 0) and pos_only:
                continue
            if np.allclose(X, 0) and non_null_only:
                continue

            tile = LightPipeTile(X=X, y=y, band_map=band_map)
            yield tile


    def load(self) -> None:
        # @TODO: IMPLEMENT THIS
        pass


    def save(
        self, savepath: str, preds: Optional[Sequence] = None, *args, **kwargs
    ) -> None:
        # @TODO: Delegate based on file extension.
        # @TODO: IMPLEMENT THIS
        if raster_io.file_is_a(savepath, extension=".tif"):
            self._save_preds_as_geotiff(
                geotiff_path=savepath, preds=preds, *args, **kwargs
            )
        elif raster_io.file_is_a(savepath, extension=".csv"):
            self._save_preds_as_csv(
                geotiff_path=savepath, preds=preds, *args, **kwargs
            )
        else:
            raise NotImplementedError("`save` is not implemented for this file type.")
        

    def _save_preds_as_geotiff(
        self, geotiff_path: str, preds: Optional[Sequence] = None, 
        tile_y: Optional[int] = None, tile_x: Optional[int] = None, 
        row_major: Optional[bool] = None, 
        use_ancestor_pixel_size: Optional[bool] = True, 
        pixel_x_size: Optional[Union[int, float]] = None,
        pixel_y_size: Optional[Union[int, float]] = None,
        n_bands: Optional[int] = 1, dtype = gdal.GDT_Byte,
        assert_north_up: Optional[bool] = True, *args, **kwargs
    ) -> None:
        if preds is None:
            preds = self.preds
        assert preds is not None, \
            "`self.preds` instance variable must be set if `preds` not passed as parameter."
        if not isinstance(preds, np.ndarray):
            preds = np.array(preds)    
        if self.shuffle_indices is not None:
            preds = self.unshuffle(preds=preds)

        if tile_y is None:
            tile_y = self.tile_y
        if tile_x is None:
            tile_x = self.tile_x
        if row_major is None:
            row_major = self.row_major

        _, out_dataset = raster_trans.make_north_up_dataset_from_tiles_like(
            datasets=self.datasets, filepath=geotiff_path, tiles=preds,
            tile_y=tile_y, tile_x=tile_x, row_major=row_major, 
            use_ancestor_pixel_size=use_ancestor_pixel_size, 
            pixel_x_size=pixel_x_size, pixel_y_size=pixel_y_size, n_bands=n_bands,
            dtype=dtype, assert_north_up=assert_north_up, *args, **kwargs
        )
        return out_dataset


    def _save_preds_as_csv(
        self, geotiff_path: str, preds: Optional[Sequence] = None, 
        tile_y: Optional[int] = None, tile_x: Optional[int] = None,
        *args, **kwargs
    ) -> None:
        # @TODO: IMPLEMENT THIS
        pass


class GridSample(LightPipeSample):
    # @TODO: IMPLEMENT THIS
    def __init__(
        self, quad_key: int, data: Sequence[Tuple[gdal.Dataset, bool, Dict]], 
        *args, **kwargs
    ):
        super().__init__(uid=quad_key, data=data, *args, **kwargs)