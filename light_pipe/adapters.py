import pathlib
from osgeo import ogr, osr, gdal

from typing import Union, Optional
from light_pipe import mercantile


ogr.UseExceptions()
osr.UseExceptions()
gdal.UseExceptions()

DEFAULT_EPSG = 4326


class GDALAdapter:
    def __init__(
        self, filepath: Union[str, pathlib.Path], 
        keep_open: Optional[bool] = False
    ):
        self.filepath = filepath
        self.keep_open = keep_open

        self.data = None


    def _Open(self):
        raise NotImplementedError


    def Open(self):
        if self.data is not None:
            return self.data
        filepath = str(self.filepath)
        data = self._Open(filepath)
        if self.keep_open:
            self.data = data
        return data


    def Close(self):
        if not self.keep_open:
            self.data = None


    def __enter__(self):
        self.data = self.Open() # Overrides `keep_open`
        return self


    def __exit__(self, *args, **kwargs):
        self.Close()


class DatasetAdapter(GDALAdapter):
    def _Open(self, filepath: str):
        return gdal.Open(filepath)


    def ReadAsArray(self):
        if self.data is not None:
            arr = self.data.ReadAsArray()
        else:
            data = self.Open()
            arr = data.ReadAsArray()
        return arr

    
    def GetExtent(self, target_epsg = DEFAULT_EPSG):
        src = self.Open()
        ulx, xres, xskew, uly, yskew, yres  = src.GetGeoTransform()
        lrx = ulx + (src.RasterXSize * xres)
        lry = uly + (src.RasterYSize * yres)  

        source = osr.SpatialReference()
        source.ImportFromWkt(src.GetProjection())

        target = osr.SpatialReference()
        target.ImportFromEPSG(target_epsg)

        transform = osr.CoordinateTransformation(source, target)

        ul_lat, ul_lon, _ = transform.TransformPoint(ulx, uly)     
        lr_lat, lr_lon, _ = transform.TransformPoint(lrx, lry)

        extent = mercantile.LngLatBbox(
            west=ul_lon, south=lr_lat, east=lr_lon, north=ul_lat
        )

        return extent

        
    def GetDriver(self):
        src = self.Open()
        driver = src.GetDriver()
        return driver


    def RasterXSize(self):
        src = self.Open()
        return src.RasterXSize


    def RasterYSize(self):
        src = self.Open()
        return src.RasterYSize


    def GetGeoTransform(self):
        src = self.Open()
        return src.GetGeoTransform()


    def GetProjection(self):
        src = self.Open()
        return src.GetProjection()


    def GetRasterBand(self, index: int):
        src = self.Open()
        return src.GetRasterBand(index)


class DataSourceAdapter:
    def __init__(
        self, filepath: Union[str, pathlib.Path], 
        keep_open: Optional[bool] = False
    ):
        self.filepath = filepath
        self.keep_open = keep_open

        self.data_source = None


    def Open(self):
        if self.data_source is not None:
            return self.data_source
        filepath = str(self.filepath)
        data_source = ogr.Open(filepath)
        if self.keep_open:
            self.data_source = data_source
        return data_source


    def Close(self):
        self.data_source = None


    def __enter__(self):
        self.data_source = self.Open() # Overrides `keep_open`
        return self


    def __exit__(self, type, value, traceback):
        self.Close()

    
    def GetExtent(self, target_epsg = DEFAULT_EPSG):
        src = self.Open()
        layer = src.GetLayerByIndex(0)
        ul_lon, lr_lon, lr_lat, ul_lat = layer.GetExtent()

        source = layer.GetSpatialRef()

        target = osr.SpatialReference()
        target.ImportFromEPSG(target_epsg)

        transform = osr.CoordinateTransformation(source, target)

        ul_lat, ul_lon, _ = transform.TransformPoint(ul_lon, ul_lat)     
        lr_lat, lr_lon, _ = transform.TransformPoint(lr_lon, lr_lat)

        extent = mercantile.LngLatBbox(
            west=ul_lon, south=lr_lat, east=lr_lon, north=ul_lat
        )

        return extent


    def GetLayerCount(self):
        src = self.Open()
        return src.GetLayerCount()


    def GetLayerByIndex(self, index: int):
        src = self.Open()
        return src.GetLayerByIndex(index)   
