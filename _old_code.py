# # Adapted from: https://stackoverflow.com/a/1213943
# # Credit: stackoverflow user mluebke
# def get_uid(multiplier: int = 100000000000, *args, **kwargs):
#     """
#     Generates a universally unique ID.
#     Any arguments only create more randomness.
#     """
#     t = int(time.time() * multiplier)
#     r = int(random.random() * multiplier)
#     data = f"{t} {r} {args}"
#     uid = hashlib.md5(data.encode()).hexdigest()
#     return uid


# async def get_future_result(future: concurrent.futures.Future):
#     res = future.result()
#     return res


# def make_coro(f):
#     @functools.wraps(f)
#     async def make_coro_wrapper(
#         executor: concurrent.futures.ThreadPoolExecutor, *args, **kwargs
#     ):
#         future = executor.submit(
#             f, *args, **kwargs
#         )
#         res = await get_future_result(future)
#         return res
#     return make_coro_wrapper

# def convert_shapefile_to_geojson(geojson_path: str, shp_path: str) -> None:
#     gdal.VectorTranslate(geojson_path, shp_path, format="GeoJSON")

# def rasterize_vector_files(
#     datasets: Union[gdal.Dataset, List[gdal.Dataset]], datasources: List[ogr.DataSource],
#     manifest_dict: dict, *args, **kwargs
# ) -> dict:
#     try:
#         iter(datasets)
#     except TypeError:
#         assert isinstance(datasets, gdal.Dataset), \
#             "No datasets passed."
#         datasets = [datasets]
#     datasets, datasources, manifest_dict = rasterize_datasources(
#         datasets=datasets, datasources=datasources, manifest_dict=manifest_dict,
#         *args, **kwargs
#     )
#     return datasets, datasources, manifest_dict


# # @gdal_data_handlers.close_data
# def make_north_up_dataset_from_tiles_like(
#     datasets: List[gdal.Dataset], tiles: np.ndarray, tile_y: int, 
#     tile_x: int, row_major = False, use_ancestor_pixel_size = False, 
#     pixel_x_size = None, pixel_y_size = None, n_bands = 1, 
#     dtype = gdal.GDT_Byte, out_dir = None, assert_north_up: Optional[bool] = True,
#     *args, **kwargs
# ) -> Tuple[List[gdal.Dataset], gdal.Dataset]:
#     def write_tiles_to_dataset(
#         dataset: gdal.Dataset, tiles: np.ndarray, out_raster_y_size: int,
#         out_raster_x_size: int, row_major = False, *args, **kwargs
#     ) -> gdal.Dataset:
#         if row_major:
#             tiles = tiles.reshape(out_raster_y_size, out_raster_x_size)
#         else:
#             tiles = tiles.reshape(out_raster_x_size, out_raster_y_size).T
#         dataset = raster_io.write_array_to_dataset(tiles, dataset)
#         return dataset


#     if use_ancestor_pixel_size:
#         gt_0, pixel_x_size, gt_2, gt_3, gt_4, pixel_y_size = datasets[0].GetGeoTransform()
#         if assert_north_up:
#             filepath = datasets[0].GetDescription()
#             assert abs(gt_2) <= 1e-16 and (gt_4) <= 1e-16, \
#                 f"Transformation coefficients are not equal to zero for dataset {filepath}."
#     else:
#         assert pixel_y_size is not None
#         assert pixel_x_size is not None
#         gt_0, _, _, gt_3, _, _ = datasets[0].GetGeoTransform()
#     out_projection = datasets[0].GetProjection()
#     out_geotransform = (
#         gt_0, tile_x * pixel_x_size, 0.0, gt_3, 0.0, tile_y * pixel_y_size
#     )
#     out_driver = datasets[0].GetDriver()

#     raster_y = datasets[0].RasterYSize
#     raster_x = datasets[0].RasterXSize

#     padded_y = raster_utils.round_up(raster_y, tile_y)
#     padded_x = raster_utils.round_up(raster_x, tile_x)
#     out_raster_y_size = padded_y // tile_y
#     out_raster_x_size = padded_x // tile_x

#     out_dataset_uid = raster_utils.get_uid()

#     ancestor_filepath = datasets[0].GetDescription()
#     out_filepath = raster_io.get_descendant_filepath(
#         ancestor_filepath, out_dataset_uid, out_dir
#     )

#     out_dataset = raster_io.make_dataset(
#         out_driver, out_filepath, out_raster_x_size, out_raster_y_size, n_bands,
#         dtype, out_geotransform, out_projection
#     )
#     out_dataset = raster_io.add_uid_to_metadata(out_dataset, out_dataset_uid)

#     for ancestor_dataset in datasets:
#         ancestor_dataset, out_dataset = raster_io.add_ancestor_reference_to_descendant_metadata(
#             ancestor_dataset, out_dataset
#         )

#     out_dataset = write_tiles_to_dataset(
#         out_dataset, tiles, out_raster_y_size, out_raster_x_size, row_major
#     )
#     return datasets, out_dataset

# LIGHT_PIPE_UID_KEY = "LIGHT_PIPE_UID"
# LIGHT_PIPE_ANCESTORS_KEY = "LIGHT_PIPE_ANCESTORS"
# LIGHT_PIPE_DESCENDANTS_KEY = "LIGHT_PIPE_DESCENDANTS"

# LIGHT_PIPE_PREDICTION_KEY = "LIGHT_PIPE_PREDICTION"

# MANIFEST_FILEPATH_KEY = "FILEPATH"


# class LightPipeUIDNotFoundError(Exception):
#     """
#     Raised when a gdal.Dataset instance has not 
#     """

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


# from typing import Generator, Iterable, Sequence, Union, Optional, Callable
# from collections import namedtuple
# import concurrent.futures
# import csv

# from light_pipe import gdal_data_handlers, threading_utils, gridding, raster_io

# LIGHT_PIPE_ITEM_INDEX_KEY = "ITEM"
# LIGHT_PIPE_GRID_INDEX_KEY = "GRID"

# TRUE_KEYS = (True, "TRUE", "True", "true", "T", "t")


# class LightPipeTile(namedtuple("LightPipeTile", ["X", "y", "n_bands"])):
#     def __new__(cls, X, y, n_bands):
#         # @TODO: IMPLEMENT
#         return tuple.__new__(cls, [X, y, n_bands])


# class LightPipeSample:
#     # @TODO: IMPLEMENT THIS
#     """
#     Serves analysis-ready subsamples from arbitrarily-large raster(s) and 
#     contains necessary variables and methods to format "predictions" into
#     georeferenced files. This class should not be instantiated directly.
#     """
#     def __init__(self, preds = list(), pos_only = False, non_null_only = False):
#         self.preds = preds
#         self.pos_only = pos_only
#         self.non_null_only = non_null_only


#     def __iter__(self) -> LightPipeTile:
#         # @TODO: IMPLEMENT THIS
#         pass

    
#     def shuffle(self) -> None:
#         # @TODO: IMPLEMENT THIS
#         pass


#     def save(self, savepath: str) -> None:
#         # @TODO: Delegate based on file extension.
#         # @TODO: IMPLEMENT THIS
#         pass


#     def _save_preds_as_geotiff(self, geotiff_path: str) -> None:
#         # @TODO: IMPLEMENT THIS
#         pass


#     def _save_preds_as_csv(self, csv_path: str) -> None:
#         # @TODO: IMPLEMENT THIS
#         pass


# class GridSample(LightPipeSample):
#     # @TODO: IMPLEMENT THIS
#     pass


# class ItemSample(LightPipeSample):
#     # @TODO: IMPLEMENT THIS
#     pass


# class LightPipeDataset:
#     """
#     Implements iterator interface for loading Light-Pipe data samples.
#     """
#     def __init__(
#         self, data_generator: Generator,
#         pos_only: Optional[bool] = False, 
#         non_null_only: Optional[bool] = False,
#         samples: Optional[list] = None
#     ):
#         self.data_generator = data_generator
#         self.pos_only = pos_only
#         self.non_null_only = non_null_only
#         self.samples = samples
#         self.is_loaded = False
#         self.len =  None
#         self.idx = None


#     def load(self, data_generator: Optional[Generator] = None):
#         """
#         Creates Light-Pipe data samples in an eager fashion. Useful for small
#         datasets that may completely fit in memory. Do not use this method if
#         your data will not fit in memory.
#         """
#         if data_generator is None:
#             assert self.data_generator is not None, \
#                 "`data_generator` not set."
#             data_generator = self.data_generator
#         samples = list()
#         for data in data_generator:
#             sample: LightPipeSample = LightPipeSample(
#                 data=data, pos_only=self.pos_only, non_null_only=self.non_null_only
#             )
#             # @TODO: IMPLEMENT THIS
#             # Calculate number of tiles
#             samples.append(sample)
#         self.len = len(samples)
#         self.samples = samples
#         self.idx = 0
#         self.is_loaded = True


#     def __iter__(self):
#         return self


#     def __next__(self):
#         return self.next()


#     def next(self) -> LightPipeSample:
#         if self.is_loaded:
#             if self.idx >= self.len:
#                 raise StopIteration()
#             sample: LightPipeSample = self.samples[self.idx]
#             self.idx += 1
#             return sample
#         else:
#             # @TODO: Implement
#             data = next(self.data_generator)
#             return data


# class ManifestItem(
#     namedtuple("ManifestItem", ["filepath", "label_filepaths", "is_label"])
# ):
#     def __new__(
#         cls, filepath, label_filepaths = None, is_label = False
#     ):
#         return tuple.__new__(cls, [filepath, label_filepaths, is_label])


# class LightPipeline:
#     """
#     Makes Light-Pipe datasets from manifest files. These files take a standard
#     form and it's up to the user to define a function which produces either
#     a manifest object directly or a file with a supported structure.
#     """
#     def __init__(
#         self, manifest = None, n_threads = None, n_processes = None
#     ):
#         self.index_keys = [
#             LIGHT_PIPE_GRID_INDEX_KEY, LIGHT_PIPE_ITEM_INDEX_KEY
#         ]
#         self.manifest = manifest
#         self.n_threads = n_threads
#         self.n_processes = n_processes
#         # @TODO: IMPLEMENT THIS


#     def make_manifest(self, manifest_path, index_type: str) -> Generator:
#         if raster_io.file_is_a(filepath=manifest_path, extension=".csv"):
#             manifest = self._make_manifest_from_csv(
#                 csv_path=manifest_path, index_type=index_type
#             )
#             self.manifest = manifest
#         elif raster_io.file_is_a(filepath=manifest_path, extension=".json"):
#             manifest = self._make_manifest_from_json(
#                 json_path=manifest_path, index_type=index_type
#             )
#             self.manifest = manifest
#         else:
#             raise NotImplementedError(
#                 "`make_manifest` not implemented for files of this type."
#             )
#         return manifest

    
#     def make_dataset(
#         self, manifest: Optional[Union[Generator, str, None]] = None, 
#         index_type = LIGHT_PIPE_ITEM_INDEX_KEY,
#     ) -> LightPipeDataset:
#         assert index_type in self.index_keys, \
#             f"Unknown index type {index_type}."
#         if manifest is None:
#             assert self.manifest is not None, \
#                 "No manifest passed."
#             manifest = self.manifest
#         elif isinstance(manifest, str):
#             manifest = self.make_manifest(
#                 manifest_path=manifest, index_type=index_type
#             )
#         if index_type == LIGHT_PIPE_ITEM_INDEX_KEY:
#             lp_dataset = self._make_item_indexed_dataset(manifest=manifest)
#         elif index_type == LIGHT_PIPE_GRID_INDEX_KEY:
#             lp_dataset = self._make_grid_indexed_dataset(manifest=manifest)
#         else:
#             raise NotImplementedError(
#                 f"`make_dataset` not implemented for index type {index_type}."
#             )
#         return lp_dataset


#     def _make_manifest_from_csv(
#         self, csv_path, index_type: str, mode = "r"
#     ) -> Generator:
#         def make_item_from_csv_line(
#             line: Sequence, index_type: str = index_type
#         ) -> ManifestItem:
#             line_len = len(line)
#             assert line_len > 0, \
#                 f"{csv_path} contains an empty line."
#             item_filepath = line[0]
#             if index_type == LIGHT_PIPE_ITEM_INDEX_KEY:
#                 if len(line) > 1:
#                     label_filepaths = line[1:]
#                 else:
#                     label_filepaths = None
#                 item = ManifestItem(
#                     filepath=item_filepath, label_filepaths=label_filepaths,
#                     is_label=False
#                 )
#             elif index_type == LIGHT_PIPE_GRID_INDEX_KEY:
#                 if len(line) > 1:
#                     is_label = line[1] in TRUE_KEYS
#                 else:
#                     is_label = False
#                 item = ManifestItem(
#                     filepath=item_filepath, label_filepaths=None, is_label=is_label
#                 )
#             else:
#                 raise NotImplementedError(
#                     f"`make_dataset` not implemented for index type {index_type}."
#                 )
#             return item


#         with open(csv_path, model=mode) as f:
#             reader = csv.reader(f, delimiter=",")
#             for line in reader:
#                 item: ManifestItem = make_item_from_csv_line(
#                     line=line, index_type=index_type
#                 )
#                 yield item


#     def _make_manifest_from_json(
#         self, json_path, index_type: str
#     ) -> Generator:
#         # @TODO: IMPLEMENT THIS
#         raise NotImplementedError(
#             "This method is not implemented."
#         )


#     def _use_threadpool(
#         self, f: Callable, iterable: Iterable, max_workers: Optional[int] = None,
#         executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
#         *args, **kwargs
#     ) -> Generator:
#         if max_workers is None:
#             max_workers = self.n_threads
#         sample_generator = f(
#             iterable=iterable, executor=executor, max_workers=max_workers,
#             *args, **kwargs
#         )
#         return sample_generator


#     def _use_queue(self, *args, **kwargs):
#         # @TODO: IMPLEMENT THIS
#         raise NotImplementedError(
#             "This method is not implemented."
#         )
        

#     def _make_item_indexed_dataset(self, manifest: Generator) -> LightPipeDataset:
#         # @TODO: IMPLEMENT THIS
#         pass


#     def _make_grid_indexed_dataset(
#         self, manifest: Generator, use_threadpool: Optional[bool] = False,
#         executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
#         pos_only: Optional[bool] = False, non_null_only: Optional[bool] = False,
#         *args, **kwargs
#     ) -> LightPipeDataset:
#         def grid_cell_generator(
#             manifest: Generator = manifest, pos_only: bool = pos_only,
#             non_null_only: bool = non_null_only
#         ) -> Generator:
#             @gdal_data_handlers.open_data
#             def process_fn(data, manifest_dict, *args, **kwargs):
#                 data, grid_cell_datasets, manifest_dict = gridding.make_grid_cell_datasets(
#                     data=data, manifest_dict=manifest_dict, *args, **kwargs
#                 )
#                 return data, grid_cell_datasets, manifest_dict


#             # @TODO: IMPLEMENT
#             manifest_dict = dict()

#             for item in manifest:
                
#             stuff = None
#             yield stuff


#         lp_dataset = LightPipeDataset(
#             data_generator=grid_cell_generator, pos_only=pos_only, 
#             non_null_only=non_null_only
#         )
#         return lp_dataset


# def make_grid_cell_datasets(
#     data: List[Union[gdal.Dataset, ogr.DataSource]], manifest_dict: dict, 
#     zoom: int, pixel_x_meters: Optional[float] = 3.0,
#     pixel_y_meters: Optional[float] = -3.0, out_dir = None, 
#     mercantile_projection: Optional[int] = PSEUDO_MERCATOR_EPSG,
#     default_dd_epsg: Optional[int] = DEFAULT_DD_EPSG, 
#     uid_key = raster_io.LIGHT_PIPE_UID_KEY,
#     ancestors_key = raster_io.LIGHT_PIPE_ANCESTORS_KEY, 
#     light_pipe_quad_key: Optional[str] = LIGHT_PIPE_QUAD_KEY,
#     default_driver_name = "GTiff", datetime_key: Optional[str] = 'TIFFTAG_DATETIME',
#     default_dtype = gdal.GDT_Byte, default_n_bands = 1, 
#     truncate: Optional[bool] = False, use_ancestor_driver = True,
#     map: Optional[Union[Generator, Callable]] = threading_utils.mmap, 
#     *args, **kwargs
# ):
#     def make_grid_cell_sub_dataset(
#         datum: Union[gdal.Dataset, ogr.DataSource], grid_cell: GridCell, 
#         qkey: int, filepath: str, pixel_x_meters=pixel_x_meters,
#         pixel_y_meters=pixel_y_meters
#     ) -> gdal.Dataset:
#         def add_metadata_to_grid_cell_dataset(
#             datum: Union[gdal.Dataset, ogr.DataSource], grid_cell_dataset: gdal.Dataset,
#             uid: Optional[str] = None
#         ):
#             if uid is None:
#                 uid = raster_utils.get_uid()
#             metadata = {
#                 "AREA_OR_POINT": "Area",
#                 light_pipe_quad_key: qkey,
#                 uid_key: uid
#             }
#             if isinstance(datum, gdal.Dataset):
#                 datum = raster_io.add_uid_to_metadata(datum)
#                 ancestor_metadata = datum.GetMetadata()
#                 if datetime_key in ancestor_metadata:
#                     datetime = ancestor_metadata[datetime_key]
#                     metadata[datetime_key] = datetime
#                 ancestor_uid = ancestor_metadata[uid_key]
#                 now = time.time()
#                 ancestors_dict = {
#                     ancestor_uid: now
#                 }
#                 metadata[ancestors_key] = json.dumps(ancestors_dict)
#             grid_cell_dataset.SetMetadata(metadata)
#             return datum, grid_cell_dataset


#         pixel_x_meters = abs(pixel_x_meters)
#         pixel_y_meters = abs(pixel_y_meters)
#         bounds = mercantile.xy_bounds(grid_cell)

#         minx = bounds.left
#         miny = bounds.bottom
#         maxx = bounds.right
#         maxy = bounds.top

#         raster_x_size = math.ceil((maxx - minx) / pixel_x_meters)
#         raster_y_size = math.ceil((maxy - miny) / pixel_y_meters)
#         geotransform = (minx, pixel_x_meters, 0.0, maxy, 0.0, -pixel_y_meters)

#         srs = osr.SpatialReference()
#         srs.ImportFromEPSG(mercantile_projection)
#         projection = srs.ExportToWkt()

#         if isinstance(datum, ogr.DataSource):
#             driver = gdal.GetDriverByName(default_driver_name)
#             n_bands = default_n_bands
#             dtype = default_dtype
#             grid_cell_dataset = raster_io.make_dataset(
#                 driver=driver, filepath=filepath, raster_x_size=raster_x_size,
#                 raster_y_size=raster_y_size, n_bands=n_bands, dtype=dtype,
#                 geotransform=geotransform, projection=projection, *args, **kwargs
#             )
#             datum, grid_cell_dataset = raster_trans.rasterize_datasource(
#                 datum, grid_cell_dataset, *args, **kwargs
#             )
#         elif isinstance(datum, gdal.Dataset):
#             if use_ancestor_driver:
#                 driver_name = datum.GetDriver().ShortName
#             else:
#                 driver_name = gdal.GetDriverByName(default_driver_name)
#             n_bands = datum.RasterCount
#             dtype = datum.GetRasterBand(1).DataType
#             datum, grid_cell_dataset = raster_trans.translate_dataset(
#                 datum, filepath, raster_x_size, raster_y_size, n_bands, dtype, 
#                 geotransform, projection, driver_name, srs, ulx=minx, uly=maxy, 
#                 lrx=maxx, lry=miny, *args, **kwargs
#             )
#         else:
#             raise TypeError("Input must be a `gdal.Dataset` or `ogr.DataSource` instance.")
#         datum, grid_cell_dataset = add_metadata_to_grid_cell_dataset(
#             datum, grid_cell_dataset
#         )
#         return grid_cell_dataset


#     def get_grid_cells_from_dataset(
#         dataset: gdal.Dataset, dstSRS: Optional[Union[osr.SpatialReference, None]] = None
#     ) -> Iterable[GridCell]:
#         def get_dataset_extent(
#             dataset: gdal.Dataset, dstSRS: osr.SpatialReference, 
#             assert_north_up: Optional[bool] = True
#         ):    
#             gt_0, gt_1, gt_2, gt_3, gt_4, gt_5 = dataset.GetGeoTransform()
#             if assert_north_up:
#                 filepath = dataset.GetDescription()
#                 assert abs(gt_2) <= 1e-16 and (gt_4) <= 1e-16, \
#                     f"Transformation coefficients are non-zero for dataset {filepath}."
#             lr_x = gt_0 + gt_1 * dataset.RasterXSize
#             lr_y = gt_3 + gt_5 * dataset.RasterYSize

#             srcSRS = dataset.GetSpatialRef()
#             transformation = osr.CoordinateTransformation(srcSRS, dstSRS)
#             maxy, minx, _ = transformation.TransformPoint(gt_0, gt_3)
#             miny, maxx, _ = transformation.TransformPoint(lr_x, lr_y)
#             return minx, maxx, miny, maxy


#         if dstSRS is None:
#             dstSRS = osr.SpatialReference()
#             dstSRS.ImportFromEPSG(default_dd_epsg) 
#         minx, maxx, miny, maxy = get_dataset_extent(dataset, dstSRS)
#         grid_cells = mercantile.tiles(
#             west=minx, south=miny, east=maxx, north=maxy, zooms=zoom,
#             truncate=truncate
#         )
#         return grid_cells


#     def get_grid_cells_from_datasource(
#         datasource: ogr.DataSource
#     ) -> List[GridCell]:
#         n_layers = datasource.GetLayerCount()
#         grid_cells = []
#         for i in range(n_layers):
#             layer = datasource.GetLayerByIndex(i)
#             # @TODO: ADD OPTION TO USE FEATURE EXTENTS, NOT JUST LAYER EXTENTS
#             extent = layer.GetExtent()
#             minx, maxx, miny, maxy = extent
#             layer_cells = mercantile.tiles(
#                 west=minx, south=miny, east=maxx, north=maxy, zooms=zoom,
#                 truncate=truncate
#             )
#             grid_cells += layer_cells
#         return grid_cells

    
#     def make_grid_cells_from_datum(
#         datum: Union[gdal.Dataset, ogr.DataSource], 
#         grid_cell_datasets: List[gdal.Dataset], manifest_dict: dict
#     ) -> None: 
#         def make_grid_cell_from_datum(
#             grid_cell: gdal.Dataset, datum_filepath: str
#         ):
#             qkey = mercantile.quadkey(grid_cell)
#             # @TODO: MAKE FILEPATH FUNCTION A PARAMETER (CALLBACK)
#             grid_cell_filepath = raster_io.get_grid_cell_filepath(
#                 ancestor_filepath=datum_filepath, qkey=qkey, directory=out_dir, 
#             )
#             grid_cell_dataset = make_grid_cell_sub_dataset(
#                 datum=datum, grid_cell=grid_cell, qkey=qkey, filepath=grid_cell_filepath
#             )
#             grid_cell_datasets.append(grid_cell_dataset)
#             grid_cell_uid = raster_io.get_uid_from_dataset(grid_cell_dataset)
#             if qkey in manifest_dict:
#                 manifest_dict[qkey][grid_cell_uid] = grid_cell_filepath
#             else:
#                 manifest_dict[qkey] = {
#                     grid_cell_uid: grid_cell_filepath
#                 }


#         if isinstance(datum, gdal.Dataset):
#             grid_cells = get_grid_cells_from_dataset(datum)
#         elif isinstance(datum, ogr.DataSource):
#             grid_cells = get_grid_cells_from_datasource(datum)
#         else:
#             raise TypeError("`data` must contain objects either of type `gdal.Dataset` or `ogr.DataSource`.")
#         datum_filepath = datum.GetDescription()
#         # mapfunc = functools.partial(
#         #     make_grid_cell_from_datum, datum_filepath=datum_filepath,
#         # )
#         # map(mapfunc, grid_cells)
#         # mgen = map(make_grid_cell_from_datum, grid_cells, datum_filepath=datum_filepath)
#         # for _ in mgen:
#         #     pass
#         for grid_cell in grid_cells:
#             make_grid_cell_from_datum(grid_cell, datum_filepath=datum_filepath)

#     try:
#         iter(data)
#     except TypeError:
#         assert isinstance(data, gdal.Dataset) or \
#             isinstance(data, ogr.DataSource), "No data passed."
#         data = [data]
#     grid_cell_datasets = []
#     # mapfunc = functools.partial(
#     #     make_grid_cells_from_datum, grid_cell_datasets=grid_cell_datasets,
#     #     manifest_dict=manifest_dict
#     # )
#     # map(mapfunc, data)
#     mgen = map(make_grid_cells_from_datum, data, grid_cell_datasets=grid_cell_datasets,
#         manifest_dict=manifest_dict
#     )
#     for _ in mgen:
#         pass
#     # for datum in data:
#     #     make_grid_cells_from_datum(
#     #         datum, grid_cell_datasets=grid_cell_datasets, manifest_dict=manifest_dict
#     #     )
#     return data, grid_cell_datasets, manifest_dict

# def make_grid_cell_datasets(make_pickleable: Optional[bool] = False, *args, **kwargs):
#     if make_pickleable:
#         return _make_grid_cell_datasets_pickleable(*args, **kwargs)
#     else:
#         return _make_grid_cell_datasets(*args, **kwargs)

# def _make_grid_cell_datasets_pickleable(
#     datum: Union[gdal.Dataset, ogr.DataSource], is_label: Optional[bool] = False,
#     in_memory: Optional[bool] = True, zoom: Optional[int] = 16, 
#     pixel_x_meters: Optional[float] = 3.0,
#     pixel_y_meters: Optional[float] = -3.0, out_dir = None, 
#     mercantile_projection: Optional[int] = PSEUDO_MERCATOR_EPSG,
#     default_dd_epsg: Optional[int] = DEFAULT_DD_EPSG, 
#     light_pipe_quad_key: Optional[str] = LIGHT_PIPE_QUAD_KEY,
#     datetime_key: Optional[str] = DATETIME_KEY,
#     default_dtype = gdal.GDT_Byte, default_n_bands = 1, 
#     truncate: Optional[bool] = False, use_ancestor_driver = False,
#     *args, **kwargs
# ) -> list:
#     def get_grid_cells_from_dataset(
#         dataset: gdal.Dataset, dstSRS: Optional[Union[osr.SpatialReference, None]] = None
#     ) -> Iterable[GridCell]:
#         def get_dataset_extent(
#             dataset: gdal.Dataset, dstSRS: osr.SpatialReference, 
#             assert_north_up: Optional[bool] = True
#         ):    
#             gt_0, gt_1, gt_2, gt_3, gt_4, gt_5 = dataset.GetGeoTransform()
#             if assert_north_up:
#                 filepath = dataset.GetDescription()
#                 assert abs(gt_2) <= 1e-16 and (gt_4) <= 1e-16, \
#                     f"Transformation coefficients are non-zero for dataset {filepath}."
#             lr_x = gt_0 + gt_1 * dataset.RasterXSize
#             lr_y = gt_3 + gt_5 * dataset.RasterYSize

#             srcSRS = dataset.GetSpatialRef()
#             transformation = osr.CoordinateTransformation(srcSRS, dstSRS)
#             maxy, minx, _ = transformation.TransformPoint(gt_0, gt_3)
#             miny, maxx, _ = transformation.TransformPoint(lr_x, lr_y)
#             return minx, maxx, miny, maxy


#         if dstSRS is None:
#             dstSRS = osr.SpatialReference()
#             dstSRS.ImportFromEPSG(default_dd_epsg) 
#         minx, maxx, miny, maxy = get_dataset_extent(dataset, dstSRS)
#         grid_cells = mercantile.tiles(
#             west=minx, south=miny, east=maxx, north=maxy, zooms=zoom,
#             truncate=truncate
#         )
#         return grid_cells


#     def get_grid_cells_from_datasource(
#         datasource: ogr.DataSource
#     ) -> List[GridCell]:
#         n_layers = datasource.GetLayerCount()
#         grid_cells = []
#         for i in range(n_layers):
#             layer = datasource.GetLayerByIndex(i)
#             # @TODO: ADD OPTION TO USE FEATURE EXTENTS, NOT JUST LAYER EXTENTS
#             extent = layer.GetExtent()
#             minx, maxx, miny, maxy = extent
#             layer_cells = mercantile.tiles(
#                 west=minx, south=miny, east=maxx, north=maxy, zooms=zoom,
#                 truncate=truncate
#             )
#             grid_cells += layer_cells
#         return grid_cells


#     assert isinstance(datum, gdal.Dataset) or isinstance(datum, ogr.DataSource)
#     metadata = {**kwargs, "args":args}
#     if in_memory:
#         default_driver_name = "MEM"
#     else:
#         default_driver_name = "GTiff"
#     if isinstance(datum, gdal.Dataset):
#         grid_cells = get_grid_cells_from_dataset(datum)
#     elif isinstance(datum, ogr.DataSource):
#         grid_cells = get_grid_cells_from_datasource(datum)
#     else:
#         raise TypeError(
#             f"`data` must contain objects either of type `gdal.Dataset` or `ogr.DataSource`. Received instance of type {type(datum)}.")
#     datum_filepath = datum.GetDescription()
#     results = list()
#     for grid_cell in grid_cells:
#         qkey = mercantile.quadkey(grid_cell)
#         # @TODO: MAKE FILEPATH FUNCTION A PARAMETER (CALLBACK)
#         if in_memory:
#             grid_cell_filepath = ""
#         else:
#             grid_cell_filepath = raster_io.get_grid_cell_filepath(
#                 ancestor_filepath=datum_filepath, qkey=qkey, directory=out_dir, 
#             )
#         pixel_x_meters = abs(pixel_x_meters)
#         pixel_y_meters = abs(pixel_y_meters)
#         bounds = mercantile.xy_bounds(grid_cell)

#         minx = bounds.left
#         miny = bounds.bottom
#         maxx = bounds.right
#         maxy = bounds.top

#         raster_x_size = math.ceil((maxx - minx) / pixel_x_meters)
#         raster_y_size = math.ceil((maxy - miny) / pixel_y_meters)
#         geotransform = (minx, pixel_x_meters, 0.0, maxy, 0.0, -pixel_y_meters)

#         srs = osr.SpatialReference()
#         srs.ImportFromEPSG(mercantile_projection)
#         projection = srs.ExportToWkt()

#         if isinstance(datum, ogr.DataSource):
#             driver = gdal.GetDriverByName(default_driver_name)
#             n_bands = default_n_bands
#             dtype = default_dtype
#             grid_cell_dataset = raster_io.make_dataset(
#                 driver=driver, filepath=grid_cell_filepath, raster_x_size=raster_x_size,
#                 raster_y_size=raster_y_size, n_bands=n_bands, dtype=dtype,
#                 geotransform=geotransform, projection=projection, *args, **kwargs
#             )
#             datum, grid_cell_dataset = raster_trans.rasterize_datasource(
#                 datum, grid_cell_dataset, *args, **kwargs
#             )
#         elif isinstance(datum, gdal.Dataset):
#             if use_ancestor_driver:
#                 driver_name = datum.GetDriver().ShortName
#             else:
#                 # driver_name = gdal.GetDriverByName(default_driver_name)
#                 driver_name = default_driver_name
#             n_bands = datum.RasterCount
#             dtype = datum.GetRasterBand(1).DataType
#             datum, grid_cell_dataset = raster_trans.translate_dataset(
#                 datum, grid_cell_filepath, raster_x_size, raster_y_size, n_bands, dtype, 
#                 geotransform, projection, driver_name, srs, ulx=minx, uly=maxy, 
#                 lrx=maxx, lry=miny, *args, **kwargs
#             )
#         else:
#             raise TypeError("Input must be a `gdal.Dataset` or `ogr.DataSource` instance.")
#         metadata = {
#             "AREA_OR_POINT": "Area",
#             light_pipe_quad_key: qkey,
#         }
#         if isinstance(datum, gdal.Dataset):
#             ancestor_metadata = datum.GetMetadata()
#             if datetime_key in ancestor_metadata:
#                 datetime = ancestor_metadata[datetime_key]
#                 metadata[datetime_key] = datetime
#         grid_cell_dataset.SetMetadata(metadata)
#         results.append((qkey, (grid_cell_dataset, is_label, metadata)))
#     return results


# def _make_grid_cell_datasets_pickleable(
#     datum: Union[gdal.Dataset, ogr.DataSource], is_label: Optional[bool] = False,
#     in_memory: Optional[bool] = True, zoom: Optional[int] = 16, 
#     pixel_x_meters: Optional[float] = 3.0,
#     pixel_y_meters: Optional[float] = -3.0, out_dir = None, 
#     mercantile_projection: Optional[int] = PSEUDO_MERCATOR_EPSG,
#     default_dd_epsg: Optional[int] = DEFAULT_DD_EPSG, 
#     light_pipe_quad_key: Optional[str] = LIGHT_PIPE_QUAD_KEY,
#     datetime_key: Optional[str] = DATETIME_KEY,
#     default_dtype = gdal.GDT_Byte, default_n_bands = 1, 
#     truncate: Optional[bool] = False, use_ancestor_driver = False,
#     *args, **kwargs
# ) -> list:
#     def get_grid_cells_from_dataset(
#         dataset: gdal.Dataset, dstSRS: Optional[Union[osr.SpatialReference, None]] = None
#     ) -> Iterable[GridCell]:
#         def get_dataset_extent(
#             dataset: gdal.Dataset, dstSRS: osr.SpatialReference, 
#             assert_north_up: Optional[bool] = True
#         ):    
#             gt_0, gt_1, gt_2, gt_3, gt_4, gt_5 = dataset.GetGeoTransform()
#             if assert_north_up:
#                 filepath = dataset.GetDescription()
#                 assert abs(gt_2) <= 1e-16 and (gt_4) <= 1e-16, \
#                     f"Transformation coefficients are non-zero for dataset {filepath}."
#             lr_x = gt_0 + gt_1 * dataset.RasterXSize
#             lr_y = gt_3 + gt_5 * dataset.RasterYSize

#             srcSRS = dataset.GetSpatialRef()
#             transformation = osr.CoordinateTransformation(srcSRS, dstSRS)
#             maxy, minx, _ = transformation.TransformPoint(gt_0, gt_3)
#             miny, maxx, _ = transformation.TransformPoint(lr_x, lr_y)
#             return minx, maxx, miny, maxy


#         if dstSRS is None:
#             dstSRS = osr.SpatialReference()
#             dstSRS.ImportFromEPSG(default_dd_epsg) 
#         minx, maxx, miny, maxy = get_dataset_extent(dataset, dstSRS)
#         grid_cells = mercantile.tiles(
#             west=minx, south=miny, east=maxx, north=maxy, zooms=zoom,
#             truncate=truncate
#         )
#         return grid_cells


#     def get_grid_cells_from_datasource(
#         datasource: ogr.DataSource
#     ) -> List[GridCell]:
#         n_layers = datasource.GetLayerCount()
#         grid_cells = []
#         for i in range(n_layers):
#             layer = datasource.GetLayerByIndex(i)
#             # @TODO: ADD OPTION TO USE FEATURE EXTENTS, NOT JUST LAYER EXTENTS
#             extent = layer.GetExtent()
#             minx, maxx, miny, maxy = extent
#             layer_cells = mercantile.tiles(
#                 west=minx, south=miny, east=maxx, north=maxy, zooms=zoom,
#                 truncate=truncate
#             )
#             grid_cells += layer_cells
#         return grid_cells


#     assert isinstance(datum, gdal.Dataset) or isinstance(datum, ogr.DataSource)
#     metadata = {**kwargs, "args":args}
#     if in_memory:
#         default_driver_name = "MEM"
#     else:
#         default_driver_name = "GTiff"
#     if isinstance(datum, gdal.Dataset):
#         grid_cells = get_grid_cells_from_dataset(datum)
#     elif isinstance(datum, ogr.DataSource):
#         grid_cells = get_grid_cells_from_datasource(datum)
#     else:
#         raise TypeError(
#             f"`data` must contain objects either of type `gdal.Dataset` or `ogr.DataSource`. Received instance of type {type(datum)}.")
#     datum_filepath = datum.GetDescription()
#     results = list()
#     for grid_cell in grid_cells:
#         qkey = mercantile.quadkey(grid_cell)
#         # @TODO: MAKE FILEPATH FUNCTION A PARAMETER (CALLBACK)
#         if in_memory:
#             grid_cell_filepath = ""
#         else:
#             grid_cell_filepath = raster_io.get_grid_cell_filepath(
#                 ancestor_filepath=datum_filepath, qkey=qkey, directory=out_dir, 
#             )
#         pixel_x_meters = abs(pixel_x_meters)
#         pixel_y_meters = abs(pixel_y_meters)
#         bounds = mercantile.xy_bounds(grid_cell)

#         minx = bounds.left
#         miny = bounds.bottom
#         maxx = bounds.right
#         maxy = bounds.top

#         raster_x_size = math.ceil((maxx - minx) / pixel_x_meters)
#         raster_y_size = math.ceil((maxy - miny) / pixel_y_meters)
#         geotransform = (minx, pixel_x_meters, 0.0, maxy, 0.0, -pixel_y_meters)

#         srs = osr.SpatialReference()
#         srs.ImportFromEPSG(mercantile_projection)
#         projection = srs.ExportToWkt()

#         if isinstance(datum, ogr.DataSource):
#             driver = gdal.GetDriverByName(default_driver_name)
#             n_bands = default_n_bands
#             dtype = default_dtype
#             grid_cell_dataset = raster_io.make_dataset(
#                 driver=driver, filepath=grid_cell_filepath, raster_x_size=raster_x_size,
#                 raster_y_size=raster_y_size, n_bands=n_bands, dtype=dtype,
#                 geotransform=geotransform, projection=projection, *args, **kwargs
#             )
#             datum, grid_cell_dataset = raster_trans.rasterize_datasource(
#                 datum, grid_cell_dataset, *args, **kwargs
#             )
#         elif isinstance(datum, gdal.Dataset):
#             if use_ancestor_driver:
#                 driver_name = datum.GetDriver().ShortName
#             else:
#                 # driver_name = gdal.GetDriverByName(default_driver_name)
#                 driver_name = default_driver_name
#             n_bands = datum.RasterCount
#             dtype = datum.GetRasterBand(1).DataType
#             datum, grid_cell_dataset = raster_trans.translate_dataset(
#                 datum, grid_cell_filepath, raster_x_size, raster_y_size, n_bands, dtype, 
#                 geotransform, projection, driver_name, srs, ulx=minx, uly=maxy, 
#                 lrx=maxx, lry=miny, *args, **kwargs
#             )
#         else:
#             raise TypeError("Input must be a `gdal.Dataset` or `ogr.DataSource` instance.")
#         metadata = {
#             "AREA_OR_POINT": "Area",
#             light_pipe_quad_key: qkey,
#         }
#         if isinstance(datum, gdal.Dataset):
#             ancestor_metadata = datum.GetMetadata()
#             if datetime_key in ancestor_metadata:
#                 datetime = ancestor_metadata[datetime_key]
#                 metadata[datetime_key] = datetime
#         grid_cell_dataset.SetMetadata(metadata)
#         results.append((qkey, (grid_cell_dataset, is_label, metadata)))
#     return results

# def open_data(f):
#     @functools.wraps(f)
#     def open_data_wrapper(
#         datasets: Optional[Union[List[str], None]] = None, 
#         datasources: Optional[Union[List[str], None]] = None, 
#         open_data_callback: Optional[Union[None, Callable]] = None,
#         use_thread_pool_executor: Optional[bool] = False,
#         max_workers: Optional[Union[int, None]] = None, *args, **kwargs
#     ):
#         if use_thread_pool_executor:
#             assert max_workers is not None, \
#                 "`max_workers` must be set if `use_thread_pool_executor == True`."
#             executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
#         raster_filepaths = datasets
#         vector_filepaths = datasources
#         if raster_filepaths is not None:
#             try:
#                 iter(raster_filepaths)
#             except TypeError:
#                 assert raster_filepaths is not None, "No datasets passed."
#                 raster_filepaths = [raster_filepaths]
#             if use_thread_pool_executor:
#                 futures = [
#                     executor.submit(gdal.Open, filepath) for filepath in raster_filepaths
#                 ]
#                 datasets = []
#                 for future in concurrent.futures.as_completed(futures):
#                     datasets.append(future.result())
#             else:
#                 datasets = []
#                 for filepath in raster_filepaths:
#                     if filepath is None:
#                         continue
#                     dataset = gdal.Open(filepath)
#                     datasets.append(dataset)
#         if vector_filepaths is not None:
#             try:
#                 iter(vector_filepaths)
#             except TypeError:
#                 assert vector_filepaths is not None, "No datasources passed."
#                 data_filepaths = [data_filepaths]
#             if use_thread_pool_executor:
#                 futures = [
#                     executor.submit(ogr.Open, filepath) for filepath in vector_filepaths
#                 ]
#                 datasources = []
#                 for future in concurrent.futures.as_completed(futures):
#                     datasources.append(future.result())
#             else:
#                 datasources = []
#                 for filepath in vector_filepaths:
#                     if filepath is None:
#                         continue
#                     datasource = ogr.Open(filepath)
#                     datasources.append(datasource)
#         if use_thread_pool_executor:
#             executor.shutdown(wait=True)
#         res = f(
#             datasets=datasets, datasources=datasources, 
#             use_thread_pool_executor=use_thread_pool_executor, 
#             max_workers=max_workers, *args, **kwargs
#         )
#         if open_data_callback is not None:
#             res = open_data_callback(res, *args, **kwargs)
#         return res
#     return open_data_wrapper


# def close_data(f):
#     @functools.wraps(f)
#     def close_data_wrapper(
#         close_data_callback: Optional[Union[None, Callable]] = None,
#         use_thread_pool_executor: Optional[bool] = False,
#         max_workers: Optional[Union[int, None]] = None, *args, **kwargs
#     ):
#         if use_thread_pool_executor:
#             assert max_workers is not None, \
#                 "`max_workers` must be set if `use_thread_pool_executor == True`."
#             executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)        
#         res = f(
#             use_thread_pool_executor=use_thread_pool_executor, 
#             max_workers=max_workers, *args, **kwargs
#         )
#         if close_data_callback is not None:
#             res = close_data_callback(res, *args, **kwargs)
#         datasets, datasources, res = res
#         # if "datasets" in res_dict.keys():
#         #     datasets = res_dict["datasets"]
#         if datasets is not None:
#             try:
#                 iter(datasets)
#             except TypeError:
#                 assert isinstance(datasets, gdal.Dataset), \
#                     "No datasets passed."
#                 datasets = [datasets]
#             if use_thread_pool_executor:
#                 for dataset in datasets:
#                     executor.submit(raster_io.close_dataset, dataset)
#             else:
#                 for dataset in datasets:
#                     if dataset is None:
#                         continue
#                     assert isinstance(dataset, gdal.Dataset), \
#                         "Object is not a gdal.Dataset."
#                     raster_io.close_dataset(dataset)                       
#         # if "datasources" in res_dict.keys():
#         #     datasources = res_dict["datasources"]
#         if datasources is not None:
#             try:
#                 iter(datasources)
#             except TypeError:
#                 assert isinstance(datasources, ogr.DataSource), \
#                     "No datasources passed."
#                 datasources = [datasources]
#             if use_thread_pool_executor:
#                 for datasource in datasources:
#                     executor.submit(raster_io.close_datasource, datasource)
#             else:
#                 for datasource in datasources:
#                     if datasource is None:
#                         continue
#                     assert isinstance(datasource, ogr.DataSource), \
#                         "Object is not a ogr.DataSource."
#                     raster_io.close_datasource(datasource)
#         # new_res = {
#         #     key:value for key, value in res_dict.items() if key not in ("datasets", "datasources")
#         # }
#         if use_thread_pool_executor:
#             executor.shutdown(wait=True)
#         return res
#     return close_data_wrapper
