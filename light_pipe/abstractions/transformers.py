import functools
import math
import pathlib
import random
from typing import Any, Callable, Optional, Tuple, Union

import numpy as np
from light_pipe import (abstractions, adapters, concurrency, mercantile,
                        raster_io)
from osgeo import gdal, osr

gdal.UseExceptions()
osr.UseExceptions()

PSEUDO_MERCATOR_EPSG = 3857


class Transformer:
    def __init__(
        self, 
        concurrency: Optional[concurrency.ConcurrencyHandler] \
            = concurrency.ConcurrencyHandler,
        *args, **kwargs
    ):
        self.concurrency = concurrency
        self.args = args
        self.kwargs = kwargs


    @staticmethod
    def _transformation_fn(n: int, *args, **kwargs):
        return n, n


    def _make_decorator(self, *args, **kwargs):
        def decorator(fn: Callable):
            @functools.wraps(fn)
            def wrapper(*wargs, **wkwargs):
                return self.concurrency.join(
                    self.concurrency.fork(
                        self._transformation_fn, fn(*wargs, **wkwargs), *args, 
                        **kwargs,
                    )
                )
            return wrapper
        return decorator


    def transform(self, data: abstractions.Data, *args, **kwargs):
        decorator = self._make_decorator(*args, *self.args, **kwargs, **self.kwargs)
        data.wrap_generator(decorator)
        return data


    def __call__(self, data: abstractions.Data, *args, **kwargs):
        return self.transform(data, *args, **kwargs)


    def __ror__(self, data: abstractions.Data):
        return self(data)


class AnyArrayFilter(Transformer):
    @staticmethod
    def _transformation_fn(arr: np.array, *args, **kwargs):
        return np.any(arr), arr


class RandomPartitioner(Transformer):
    @staticmethod
    def _transformation_fn(
        datum: Any, num_partitions: Optional[int] = 2, *args, **kwargs
    ):
        partition_id = random.randint(0, num_partitions - 1)
        return partition_id, datum


class Rasterizer(Transformer):

    @staticmethod
    def _transformation_fn(datum: Tuple, *args, **kwargs):
        def _transformation_fn_dataset(
            data_source: adapters.DataSourceAdapter,
            dataset: adapters.DatasetAdapter,
            return_dataset: Optional[bool] = False, 
            out_filepath: Optional[Union[pathlib.Path, str]] = None, 
            driver = None, vector_attribute = None, out_inverse = False, 
            out_all_touched = True, dtype = gdal.GDT_Byte, 
            use_ancestor_dtype: Optional[bool] = True, *args, **kwargs
        ):
            if return_dataset:
                assert out_filepath is not None, \
                    "`out_filepath` must be passed when `return_dataset == True`."
                if driver is None:
                    driver = dataset.GetDriver()
            else:
                out_filepath = ""
                driver = gdal.GetDriverByName("MEM")

            if use_ancestor_dtype:
                dtype = dataset.GetRasterBand(1).DataType

            out_n_bands = data_source.GetLayerCount()
            raster_x_size = dataset.RasterXSize()
            raster_y_size = dataset.RasterYSize()
            geotransform = dataset.GetGeoTransform()
            projection = dataset.GetProjection()

            out_dataset = raster_io.make_dataset(
                driver=driver, filepath=out_filepath, raster_x_size=raster_x_size,
                raster_y_size=raster_y_size, n_bands=out_n_bands, dtype=dtype,
                geotransform=geotransform, projection=projection, *args, **kwargs
            )

            options = list()
            if vector_attribute is not None:
                with data_source as src:
                    _, attributes = \
                        raster_io.get_shapefile_attributes(src.data_source)
                    assert vector_attribute in attributes, \
                        f"Attribute {vector_attribute} not found in \
                            vector file {src.data_source.GetDescription()}."
                    options.append(f"ATTRIBUTE={vector_attribute}")
            if out_inverse:
                options.append(f"OUT_INVERSE=TRUE")
            if out_all_touched:
                options.append(f"ALL_TOUCHED=TRUE")            

            with data_source as src:
                for i in range(out_n_bands):
                    layer = src.GetLayerByIndex(i)        
                    gdal.RasterizeLayer(
                        out_dataset, [i+1], layer, options=options
                    )
          
            if return_dataset:
                del(out_dataset) # Write changes to disk - Don't question GDAL!
                out_dataset = adapters.DatasetAdapter(out_filepath)
                return data_source, out_dataset
            arr = out_dataset.ReadAsArray()
            return data_source, arr           


        def _transformation_fn_extent(
            data_source: adapters.DataSourceAdapter, 
            extent: mercantile.LngLatBbox,
            return_dataset: Optional[bool] = False, 
            pixel_x_meters: Optional[float] = 3.0, 
            pixel_y_meters: Optional[float] = -3.0,     
            mercantile_projection: Optional[int] = PSEUDO_MERCATOR_EPSG, 
            driver = None, vector_attribute = None, out_inverse = False, 
            out_all_touched = True, xskew: Optional[Union[int, float]] = 0.0,
            yskew: Optional[Union[int, float]] = 0.0, dtype = gdal.GDT_Byte,
            *args, **kwargs
        ):
            if return_dataset:
                assert out_filepath is not None, \
                    "`out_filepath` must be passed when `return_dataset == True`."
                if driver is None:
                    driver = gdal.GetDriverByName("GTiff")
            else:
                out_filepath = ""
                driver = gdal.GetDriverByName("MEM")

            out_n_bands = data_source.GetLayerCount()

            pixel_x_meters = abs(pixel_x_meters)
            pixel_y_meters = abs(pixel_y_meters)
            minx, maxy = mercantile.xy(extent.west, extent.north)
            maxx, miny = mercantile.xy(extent.east, extent.south) 

            raster_x_size = math.ceil((maxx - minx) / pixel_x_meters)
            raster_y_size = math.ceil((maxy - miny) / pixel_y_meters)
            geotransform = (
                minx, pixel_x_meters, xskew, maxy, yskew, -pixel_y_meters
            )

            srs = osr.SpatialReference()
            srs.ImportFromEPSG(mercantile_projection)
            projection = srs.ExportToWkt()

            out_dataset = raster_io.make_dataset(
                driver=driver, filepath=out_filepath, 
                raster_x_size=raster_x_size, raster_y_size=raster_y_size, 
                n_bands=out_n_bands, dtype=dtype, geotransform=geotransform, 
                projection=projection, *args, **kwargs
            )

            options = list()
            if vector_attribute is not None:
                with data_source as src:
                    _, attributes = \
                        raster_io.get_shapefile_attributes(src.data_source)
                    assert vector_attribute in attributes, \
                        f"Attribute {vector_attribute} not found in \
                            vector file {src.data_source.GetDescription()}."
                    options.append(f"ATTRIBUTE={vector_attribute}")
            if out_inverse:
                options.append(f"OUT_INVERSE=TRUE")
            if out_all_touched:
                options.append(f"ALL_TOUCHED=TRUE")    

            with data_source as src:
                for i in range(out_n_bands):
                    layer = src.GetLayerByIndex(i)        
                    gdal.RasterizeLayer(
                        out_dataset, [i+1], layer, options=options
                    )

            if return_dataset:
                del(out_dataset) # Write changes to disk - Don't question GDAL!
                out_dataset = adapters.DatasetAdapter(out_filepath)
                return data_source, out_dataset
            arr = out_dataset.ReadAsArray()
            return data_source, arr      


        data_source, extent = datum
        if isinstance(extent, adapters.DatasetAdapter):
            dataset = extent
            return _transformation_fn_dataset(
                data_source, dataset=dataset, *args, **kwargs
            )
        return _transformation_fn_extent(
            data_source, extent=extent, *args, **kwargs
        )
