__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
The module defines decorators which may be used to handle dataset opening and 
closing.
"""


import functools
from typing import Optional, Sequence, Union

from osgeo import gdal, ogr

from light_pipe import raster_io

DATA_ARGS = ["datum", "dataset", "datasets", "datasource", "datasources"]


class UnknownInputError(Exception):
    """
    Raised when a function or method is unsure what to do with an input.
    """


def merge_data(f):
    @functools.wraps(f)
    def merge_data_wrapper(
        datasets: Optional[Union[Sequence[gdal.Dataset], None]] = None, 
        datasources: Optional[Union[Sequence[ogr.DataSource], None]] = None,
        *args, **kwargs
    ):
        data = datasets + datasources
        res = f(data=data, *args, **kwargs)
        return res
    return merge_data_wrapper


def open_osgeo_inputs(input):
    if input is not None:
        if isinstance(input, gdal.Dataset):
            output = input
        elif isinstance(input, ogr.DataSource):
            output = input
        elif isinstance(input, str):
            if raster_io.file_is_a(input, extension=".tif"):
                output = gdal.Open(input)
            else:
                output = ogr.Open(input)
        elif isinstance(input, list):
            output = list()
            for item in input:
                output.append(open_osgeo_inputs(item))
        else:
            raise UnknownInputError(f"Not sure what to do with input {input}.")
        return output


def open_data(f):
    @functools.wraps(f)
    def open_data_wrapper(
        *args, **kwargs
    ):
        for data_arg in DATA_ARGS:
            if data_arg in kwargs.keys():
                data_val = kwargs[data_arg]
                data_val = open_osgeo_inputs(data_val)
                kwargs[data_arg] = data_val            
        return f(*args, **kwargs)
    return open_data_wrapper


def close_data(f):
    @functools.wraps(f)
    def close_data_wrapper(        
        *args, **kwargs
    ):
        res = f(*args, **kwargs)
        for data_arg in DATA_ARGS:
            if data_arg in kwargs.keys():
                data_val = kwargs[data_arg]
                del(data_val)  
                kwargs[data_arg] = None         
        return res
    return close_data_wrapper   
