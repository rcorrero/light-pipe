__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains the definition of `LightPipeline`, a key component of the
API of this package and the primary method through which the user creates 
samples.
"""


from typing import Iterable, Optional

from light_pipe.concurrency import concurrency_handlers
from light_pipe.samples import sample, sample_handlers


class LightPipeline:
    def __init__(
        self, inputs: Optional[Iterable] = None,
        concurrency_handler: Optional[concurrency_handlers.ConcurrencyHandler] = None,
        sample_handler: Optional[sample_handlers.SampleHandler] = None,
        blocking: Optional[bool] = False, in_memory: Optional[bool] = True
    ):
        self.inputs = inputs
        if concurrency_handler is not None and sample_handler is not None:
            self.sample_handler = sample_handler(concurrency_handler)
        if concurrency_handler is None:
            concurrency_handler = concurrency_handlers.ConcurrencyHandler()
        if sample_handler is None:
            sample_handler = sample_handlers.SampleHandler(
                concurrency_handler, in_memory=in_memory
            )
        self.concurrency_handler = concurrency_handler
        self.sample_handler = sample_handler
        self.blocking = blocking
        self.in_memory = in_memory
        self.samples = None


    def run(
        self, iterable: Optional[Iterable] = None, blocking: Optional[bool] = None,
        in_memory: Optional[bool] = None, *args, **kwargs
    ):
        if iterable is None:
            iterable = self.inputs
            assert iterable is not None, "Parameter `iterable` not set."
        if blocking is None:
            blocking = self.blocking
        if in_memory is None:
            in_memory = self.in_memory
        samples = self.sample_handler.make_samples(
            iterable=iterable, in_memory=in_memory, *args, **kwargs
        )
        if blocking:
            samples_list = list()
            for sample in samples:
                samples_list.append(sample)
                samples = samples_list
        self.samples = samples
        return self


    def __iter__(self):
        if self.samples is None:
            self.run()
            assert self.samples is not None
        return self


    def __next__(self):
        return self.next()


    def next(self) -> sample.LightPipeSample:
        sample = next(self.samples)
        return sample


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
