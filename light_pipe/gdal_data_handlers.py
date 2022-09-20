__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
The module defines decorators which may be used to handle dataset opening and 
closing.
"""


import concurrent.futures
import functools
from typing import List, Optional, Union, Callable, Sequence

from osgeo import gdal, ogr

from light_pipe import raster_io


def merge_data(f):
    @functools.wraps(f)
    def merge_data_wrapper(
        datasets: Optional[Union[Sequence[gdal.Dataset], None]] = None, 
        datasources: Optional[Union[Sequence[ogr.DataSource], None]] = None,
        *args, **kwargs
    ):
        data = datasets + datasources
        # _, grid_cell_datasets, manifest_dict = f(
        #     data, manifest_dict, *args, **kwargs
        # )
        res = f(data=data, *args, **kwargs)
        return res
        # return datasets, datasources, grid_cell_datasets, manifest_dict
    return merge_data_wrapper


def open_data(f):
    @functools.wraps(f)
    def open_data_wrapper(
        datasets: Optional[Union[List[str], None]] = None, 
        datasources: Optional[Union[List[str], None]] = None, 
        open_data_callback: Optional[Union[None, Callable]] = None,
        use_thread_pool_executor: Optional[bool] = False,
        max_workers: Optional[Union[int, None]] = None, *args, **kwargs
    ):
        if use_thread_pool_executor:
            assert max_workers is not None, \
                "`max_workers` must be set if `use_thread_pool_executor == True`."
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        raster_filepaths = datasets
        vector_filepaths = datasources
        if raster_filepaths is not None:
            try:
                iter(raster_filepaths)
            except TypeError:
                assert raster_filepaths is not None, "No datasets passed."
                raster_filepaths = [raster_filepaths]
            if use_thread_pool_executor:
                futures = [
                    executor.submit(gdal.Open, filepath) for filepath in raster_filepaths
                ]
                datasets = []
                for future in concurrent.futures.as_completed(futures):
                    datasets.append(future.result())
            else:
                datasets = []
                for filepath in raster_filepaths:
                    if filepath is None:
                        continue
                    dataset = gdal.Open(filepath)
                    datasets.append(dataset)
        if vector_filepaths is not None:
            try:
                iter(vector_filepaths)
            except TypeError:
                assert vector_filepaths is not None, "No datasources passed."
                data_filepaths = [data_filepaths]
            if use_thread_pool_executor:
                futures = [
                    executor.submit(ogr.Open, filepath) for filepath in vector_filepaths
                ]
                datasources = []
                for future in concurrent.futures.as_completed(futures):
                    datasources.append(future.result())
            else:
                datasources = []
                for filepath in vector_filepaths:
                    if filepath is None:
                        continue
                    datasource = ogr.Open(filepath)
                    datasources.append(datasource)
        if use_thread_pool_executor:
            executor.shutdown(wait=True)
        res = f(
            datasets=datasets, datasources=datasources, 
            use_thread_pool_executor=use_thread_pool_executor, 
            max_workers=max_workers, *args, **kwargs
        )
        if open_data_callback is not None:
            res = open_data_callback(res, *args, **kwargs)
        return res
    return open_data_wrapper


def close_data(f):
    @functools.wraps(f)
    def close_data_wrapper(
        close_data_callback: Optional[Union[None, Callable]] = None,
        use_thread_pool_executor: Optional[bool] = False,
        max_workers: Optional[Union[int, None]] = None, *args, **kwargs
    ):
        if use_thread_pool_executor:
            assert max_workers is not None, \
                "`max_workers` must be set if `use_thread_pool_executor == True`."
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)        
        res = f(
            use_thread_pool_executor=use_thread_pool_executor, 
            max_workers=max_workers, *args, **kwargs
        )
        if close_data_callback is not None:
            res = close_data_callback(res, *args, **kwargs)
        datasets, datasources, res = res
        # if "datasets" in res_dict.keys():
        #     datasets = res_dict["datasets"]
        if datasets is not None:
            try:
                iter(datasets)
            except TypeError:
                assert isinstance(datasets, gdal.Dataset), \
                    "No datasets passed."
                datasets = [datasets]
            if use_thread_pool_executor:
                for dataset in datasets:
                    executor.submit(raster_io.close_dataset, dataset)
            else:
                for dataset in datasets:
                    if dataset is None:
                        continue
                    assert isinstance(dataset, gdal.Dataset), \
                        "Object is not a gdal.Dataset."
                    raster_io.close_dataset(dataset)                       
        # if "datasources" in res_dict.keys():
        #     datasources = res_dict["datasources"]
        if datasources is not None:
            try:
                iter(datasources)
            except TypeError:
                assert isinstance(datasources, ogr.DataSource), \
                    "No datasources passed."
                datasources = [datasources]
            if use_thread_pool_executor:
                for datasource in datasources:
                    executor.submit(raster_io.close_datasource, datasource)
            else:
                for datasource in datasources:
                    if datasource is None:
                        continue
                    assert isinstance(datasource, ogr.DataSource), \
                        "Object is not a ogr.DataSource."
                    raster_io.close_datasource(datasource)
        # new_res = {
        #     key:value for key, value in res_dict.items() if key not in ("datasets", "datasources")
        # }
        if use_thread_pool_executor:
            executor.shutdown(wait=True)
        return res
    return close_data_wrapper
