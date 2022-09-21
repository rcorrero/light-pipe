__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains code used to generate standardized grid cell datasets from
arbitrarily-aligned imagery. These datasets are particularly useful in settings
in which time-series of aligned geospatial raster data are useful.
"""


import math
from typing import Generator, Iterable, List, Optional, Union

from osgeo import gdal, ogr, osr

from light_pipe import mercantile, raster_io, raster_trans

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


def make_grid_cell_datasets(
    datum: Union[gdal.Dataset, ogr.DataSource], is_label: Optional[bool] = False,
    in_memory: Optional[bool] = True, zoom: Optional[int] = 16, 
    pixel_x_meters: Optional[float] = 3.0,
    pixel_y_meters: Optional[float] = -3.0, out_dir = None, 
    mercantile_projection: Optional[int] = PSEUDO_MERCATOR_EPSG,
    default_dd_epsg: Optional[int] = DEFAULT_DD_EPSG, 
    light_pipe_quad_key: Optional[str] = LIGHT_PIPE_QUAD_KEY,
    datetime_key: Optional[str] = DATETIME_KEY,
    default_dtype = gdal.GDT_Byte, default_n_bands = 1, 
    truncate: Optional[bool] = False, use_ancestor_driver = False,
    *args, **kwargs
) -> Generator:
    def get_grid_cells_from_dataset(
        dataset: gdal.Dataset, dstSRS: Optional[Union[osr.SpatialReference, None]] = None
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
        datasource: ogr.DataSource
    ) -> List[GridCell]:
        n_layers = datasource.GetLayerCount()
        grid_cells = []
        for i in range(n_layers):
            layer = datasource.GetLayerByIndex(i)
            # extent = layer.GetExtent()
            for feature in layer:
                extent = feature.GetGeometryRef().GetEnvelope()
                minx, maxx, miny, maxy = extent
                layer_cells = mercantile.tiles(
                    west=minx, south=miny, east=maxx, north=maxy, zooms=zoom,
                    truncate=truncate
                )
                grid_cells += layer_cells
        return grid_cells


    assert isinstance(datum, gdal.Dataset) or isinstance(datum, ogr.DataSource)
    metadata = {**kwargs, "args":args}
    if in_memory:
        default_driver_name = "MEM"
    else:
        default_driver_name = "GTiff"
    if isinstance(datum, gdal.Dataset):
        grid_cells = get_grid_cells_from_dataset(datum)
    elif isinstance(datum, ogr.DataSource):
        grid_cells = get_grid_cells_from_datasource(datum)
    else:
        raise TypeError(
            f"`data` must contain objects either of type `gdal.Dataset` or `ogr.DataSource`. Received instance of type {type(datum)}.")
    datum_filepath = datum.GetDescription()
    for grid_cell in grid_cells:
        qkey = mercantile.quadkey(grid_cell)
        # @TODO: MAKE FILEPATH FUNCTION A PARAMETER (CALLBACK)
        if in_memory:
            grid_cell_filepath = ""
        else:
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
                datum, grid_cell_filepath, raster_x_size, raster_y_size, n_bands, dtype, 
                geotransform, projection, driver_name, srs, ulx=minx, uly=maxy, 
                lrx=maxx, lry=miny, *args, **kwargs
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
        yield qkey, (grid_cell_dataset, is_label, metadata)
