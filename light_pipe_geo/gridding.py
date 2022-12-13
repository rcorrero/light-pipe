__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains code used to generate standardized grid cell datasets from
arbitrarily-aligned imagery. These datasets are particularly useful in settings
in which time-series of aligned geospatial raster data are useful.
"""

import math
from typing import Iterable, List, Optional, Union

from osgeo import gdal, ogr, osr

from light_pipe_geo import (gdal_data_handlers, mercantile, raster_io,
                            raster_trans)

gdal.UseExceptions()
ogr.UseExceptions()
osr.UseExceptions()


DEFAULT_ZOOM = 16
DEFAULT_DD_EPSG = 4326
PSEUDO_MERCATOR_EPSG = 3857

LIGHT_PIPE_QUAD_KEY = "LIGHT_PIPE_QUAD_KEY"
DATETIME_KEY = 'TIFFTAG_DATETIME'


class GridCell(mercantile.Tile):
    """
    The name `tile` in Light-Pipe refers to contiguous sub-arrays extracted
    from `numpy.ndarray` instances, but Mercantile uses `tile` to refer to
    regions of the xy plane subdivided by a grid. For this reason 
    the `Tile` class is renamed `GridCell`.
    """


@gdal_data_handlers.open_data
def make_grid_cell_dataset(
    grid_cell: GridCell, datum: Union[gdal.Dataset, ogr.DataSource], 
    datum_filepath: Optional[str] = None, return_filepaths: Optional[bool] = False,
    is_label: Optional[bool] = False, in_memory: Optional[bool] = True, 
    pixel_x_meters: Optional[float] = 3.0, 
    pixel_y_meters: Optional[float] = -3.0, out_dir = None, 
    mercantile_projection: Optional[int] = PSEUDO_MERCATOR_EPSG,
    light_pipe_quad_key: Optional[str] = LIGHT_PIPE_QUAD_KEY,
    datetime_key: Optional[str] = DATETIME_KEY,
    default_dtype = gdal.GDT_Byte, default_n_bands = 1, 
    use_ancestor_driver = False, grid_cell_filepath: Optional[str] = None,
    default_driver_name: Optional[str] = "GTIff",
    no_data_value = None,
    *args, **kwargs
):
    if datum_filepath is None:
        datum_filepath = datum.GetDescription()
    qkey = mercantile.quadkey(grid_cell)
    # @TODO: MAKE FILEPATH FUNCTION A PARAMETER (CALLBACK)
    if in_memory:
        grid_cell_filepath = ""
        default_driver_name = "MEM" 
    elif grid_cell_filepath is None:
        grid_cell_filepath = raster_io.get_grid_cell_filepath(
            ancestor_filepath=datum_filepath, qkey=qkey, directory=out_dir, 
        )
    pixel_x_meters = abs(pixel_x_meters)
    pixel_y_meters = abs(pixel_y_meters)
    bounds = mercantile.xy_bounds(grid_cell)

    minx = bounds.left
    miny = bounds.bottom
    maxx = bounds.right
    maxy = bounds.top

    raster_x_size = math.ceil((maxx - minx) / pixel_x_meters)
    raster_y_size = math.ceil((maxy - miny) / pixel_y_meters)
    geotransform = (minx, pixel_x_meters, 0.0, maxy, 0.0, -pixel_y_meters)

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(mercantile_projection)
    projection = srs.ExportToWkt()

    if isinstance(datum, ogr.DataSource):
        driver = gdal.GetDriverByName(default_driver_name)
        n_bands = default_n_bands
        dtype = default_dtype
        grid_cell_dataset = raster_io.make_dataset(
            driver=driver, filepath=grid_cell_filepath, raster_x_size=raster_x_size,
            raster_y_size=raster_y_size, n_bands=n_bands, dtype=dtype,
            geotransform=geotransform, projection=projection, *args, **kwargs
        )
        datum, grid_cell_dataset = raster_trans.rasterize_datasource(
            datum, grid_cell_dataset, *args, **kwargs
        )
    elif isinstance(datum, gdal.Dataset):
        if use_ancestor_driver:
            driver_name = datum.GetDriver().ShortName
        else:
            # driver_name = gdal.GetDriverByName(default_driver_name)
            driver_name = default_driver_name
        n_bands = datum.RasterCount
        dtype = datum.GetRasterBand(1).DataType
        datum, grid_cell_dataset = raster_trans.translate_dataset(
            datum, grid_cell_filepath, raster_x_size, raster_y_size, n_bands, 
            dtype, geotransform, projection, driver_name, srs, ulx=minx, uly=maxy, 
            lrx=maxx, lry=miny, noData=no_data_value, *args, **kwargs
        )
    else:
        raise TypeError("Input must be a `gdal.Dataset` or `ogr.DataSource` instance.")
    metadata = {
        "AREA_OR_POINT": "Area",
        light_pipe_quad_key: qkey,
    }
    if isinstance(datum, gdal.Dataset):
        ancestor_metadata = datum.GetMetadata()
        if datetime_key in ancestor_metadata:
            datetime = ancestor_metadata[datetime_key]
            metadata[datetime_key] = datetime
    grid_cell_dataset.SetMetadata(metadata)
    item_metadata = {**kwargs, **metadata, "args":args}
    if return_filepaths:
        return qkey, (grid_cell_filepath, is_label, item_metadata)
    return qkey, (grid_cell_dataset, is_label, item_metadata)            


def get_grid_cells_from_dataset(
    dataset: gdal.Dataset, zoom: Optional[int] = DEFAULT_ZOOM,
    dstSRS: Optional[Union[osr.SpatialReference, None]] = None,
    default_dd_epsg: Optional[int] = DEFAULT_DD_EPSG,
    truncate: Optional[bool] = False, *args, **kwargs
) -> Iterable[GridCell]:
    def get_dataset_extent(
        dataset: gdal.Dataset, dstSRS: osr.SpatialReference, 
        assert_north_up: Optional[bool] = True
    ):    
        gt_0, gt_1, gt_2, gt_3, gt_4, gt_5 = dataset.GetGeoTransform()
        if assert_north_up:
            filepath = dataset.GetDescription()
            assert abs(gt_2) <= 1e-16 and (gt_4) <= 1e-16, \
                f"Transformation coefficients are non-zero for dataset {filepath}."
        lr_x = gt_0 + gt_1 * dataset.RasterXSize
        lr_y = gt_3 + gt_5 * dataset.RasterYSize

        srcSRS = dataset.GetSpatialRef()
        transformation = osr.CoordinateTransformation(srcSRS, dstSRS)
        maxy, minx, _ = transformation.TransformPoint(gt_0, gt_3)
        miny, maxx, _ = transformation.TransformPoint(lr_x, lr_y)
        return minx, maxx, miny, maxy


    if dstSRS is None:
        dstSRS = osr.SpatialReference()
        dstSRS.ImportFromEPSG(default_dd_epsg) 
    minx, maxx, miny, maxy = get_dataset_extent(dataset, dstSRS)
    grid_cells = mercantile.tiles(
        west=minx, south=miny, east=maxx, north=maxy, zooms=zoom,
        truncate=truncate
    )
    return grid_cells


def get_grid_cells_from_datasource(
    datasource: ogr.DataSource, zoom: Optional[int] = DEFAULT_ZOOM,
    dstSRS: Optional[Union[osr.SpatialReference, None]] = None,
    default_dd_epsg: Optional[int] = DEFAULT_DD_EPSG,    
    truncate: Optional[bool] = False, *args, **kwargs
) -> List[GridCell]:
    if dstSRS is None:
        dstSRS = osr.SpatialReference()
        dstSRS.ImportFromEPSG(default_dd_epsg) 
    n_layers = datasource.GetLayerCount()
    grid_cells = set()

    # @TODO: Add support for concurrency
    for i in range(n_layers):
        layer = datasource.GetLayerByIndex(i)
        srcSRS = layer.GetSpatialRef()
        transformation = osr.CoordinateTransformation(srcSRS, dstSRS)

        # extent = layer.GetExtent()
        for feature in layer:
            feature.GetGeometryRef().Transform(transformation)
            extent = feature.GetGeometryRef().GetEnvelope()
            # minx, maxx, miny, maxy = extent
            miny, maxy, minx, maxx = extent
            layer_cells = mercantile.tiles(
                west=minx, south=miny, east=maxx, north=maxy, zooms=zoom,
                truncate=truncate
            )
            grid_cells.update(layer_cells)
    return grid_cells


@gdal_data_handlers.open_data
def get_grid_cells(
    datum, zoom: Optional[int] = DEFAULT_ZOOM,
    dstSRS: Optional[Union[osr.SpatialReference, None]] = None,
    default_dd_epsg: Optional[int] = DEFAULT_DD_EPSG,
    truncate: Optional[bool] = False, *args, **kwargs
):
    if isinstance(datum, gdal.Dataset):
        grid_cells = get_grid_cells_from_dataset(
            dataset=datum, zoom=zoom, dstSRS=dstSRS, 
            default_dd_epsg=default_dd_epsg, truncate=truncate, *args, **kwargs
        )
    elif isinstance(datum, ogr.DataSource):
        grid_cells = get_grid_cells_from_datasource(
            datasource=datum, zoom=zoom, dstSRS=dstSRS, 
            default_dd_epsg=default_dd_epsg, truncate=truncate, *args, **kwargs
        )
    return grid_cells
