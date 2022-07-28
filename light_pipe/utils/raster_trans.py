from osgeo import gdal, ogr


# Adapted From : https://gis.stackexchange.com/a/377751
# Credit: Blessed Stack Exchange user "Jose"
def rasterize_label(sample_file, vector_file,
                    attribute=None, otype="MEM", fname_out=""):
    """Rasterize a vector layer `vector_file` using a sample raster file 
    (`sample_file`) to provide extent and projection. You can select an
    attribute in  `vector_file`, and you can either do the
    processing in memory, or dump to disk in any GDAL  supported format.

    Parameters
    ----------
    sample_file : str
        A raster with a suitable geotransform, projection etc.
    vector_file : str
        The vector file you want rasterized
    attribute : str, optional
        The attribute field to rasterize, by default None
    otype : str, optional
        GDAL output type, by default "MEM"  (in memory). Set to `GTIff` for
        file output
    fname_out : str, optional
        The output filename, by default "" (e.g.  in memory raster). 
        
    Returns
    -------
    ndarray
        An array with the rasterised raster as a numpy array.
    """
    g = gdal.Open(sample_file)
    dst = gdal.GetDriverByName(otype).Create(fname_out,
                                             g.RasterXSize,
                                             g.RasterYSize, 1,
                                             gdal.GDT_UInt32)
    dst.SetGeoTransform(g.GetGeoTransform())
    dst.SetSpatialRef(g.GetSpatialRef())
    f = ogr.Open(vector_file)
    if attribute is not None:
        gdal.RasterizeLayer(dst, [1], f.GetLayerByIndex(0), 
                            options=[f'ATTRIBUTE={attribute}', 
                            f'ALL_TOUCHED=TRUE'])

    else:
        gdal.RasterizeLayer(dst, [1], f.GetLayerByIndex(0), 
                            options=[f'ALL_TOUCHED=TRUE'])
                            #options=[f'ATTRIBUTE={attribute}', f'ALL_TOUCHED=TRUE'])
    if otype != "MEM":
        # Read data and close dataset to flush to disk if
        # not using in-memory array.
        # retval = dst.ReadAsArray()
        dst = None
    else:
        return dst.ReadAsArray()
