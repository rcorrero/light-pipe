__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains the definition of `SampleProcessor` and associated subclasses.
These contain the definition of the sequence of steps necessary to make samples
of a specific type. This operation is defined in the `make_samples` method.
"""


from typing import Callable, Generator, Iterable, Optional, Sequence

from light_pipe_geo import concurrency, gridding, raster_trans
from light_pipe_geo.processing import sample


class SampleProcessor:
    def __init__(
        self, fn: Optional[Callable] = None, 
        wrappers: Optional[Sequence[Callable]] = None,
        concurrency_handler: Optional[concurrency.ConcurrencyHandler] = None,
        fork: Optional[Callable] = None, join: Optional[Callable] = None,
        make_parallelizable: Optional[bool] = None
    ):
        if wrappers is not None:
            for wrapper in wrappers:
                fn = wrapper(fn)
        self.fn = fn
        self.wrappers = wrappers
        if fork is not None and join is not None:
            self.fork = fork
            self.join = join
        elif concurrency_handler is None:
            concurrency_handler = concurrency.ConcurrencyHandler
            self.fork = concurrency_handler.fork
            self.join = concurrency_handler.join
        else:
            self.fork = concurrency_handler.fork
            self.join = concurrency_handler.join
        if make_parallelizable is None:
            if concurrency_handler is not None:
                make_parallelizable = concurrency_handler.parallel
            else:
                make_parallelizable = False
        self.make_parallelizable = make_parallelizable


    def run(
        self, iterable: Iterable, fn: Optional[Callable] = None, *args, **kwargs
    ) -> Generator:
        if fn is None:
            fn = self.fn
        results = self.join(
            self.fork(f=fn, iterable=iterable, *args, **kwargs)
        )
        yield from results


    def set_concurrency(self, concurrency_handler: concurrency.ConcurrencyHandler):
        self.fork = concurrency_handler.fork
        self.join = concurrency_handler.join
        return self


class SampleMaker(SampleProcessor):
    def __init__(
        self, fn: Optional[Callable] = None,
        concurrency_handler: Optional[concurrency.ConcurrencyHandler] = None,
        in_memory: Optional[bool] = True, 
        make_parallelizable: Optional[bool] = None, *args, **kwargs
    ):
        if make_parallelizable is None:
            if concurrency_handler is not None:
                make_parallelizable = concurrency_handler.parallel
            else:
                make_parallelizable = False
        if fn is None:
            if make_parallelizable:
                fn = self._fork_fn_no_gen
            else:
                fn = self._fork_fn
        self.make_parallelizable = make_parallelizable
        kwargs = {
            **kwargs, 
            "fn":fn,
            "concurrency_handler": concurrency_handler,
            "make_parallelizable": make_parallelizable
        }
        super().__init__(*args, **kwargs)
        self.in_memory = in_memory


    def _fork_fn(self, iterable_kwargs, *args, **kwargs):
        kwargs = {**kwargs, **iterable_kwargs}
        return raster_trans.rasterize_datasources(*args, **kwargs)


    def _fork_fn_no_gen(self, iterable_kwargs, in_memory, *args, **kwargs):
        assert not in_memory, \
            "Cannot parallelize when `in_memory = True`."
        kwargs = {**kwargs, **iterable_kwargs}

        results = raster_trans.rasterize_datasources(
            in_memory=in_memory, return_filepaths=True, *args, **kwargs
        )
        results_list = [res for res in results]
        return results_list


    def make_samples(
        self, iterable: Iterable, in_memory: Optional[bool] = None, 
        make_parallelizable: Optional[bool] = None,
        load_samples: Optional[bool] = False, *args, **kwargs
    ) -> Generator:
        if in_memory is None:
            in_memory = self.in_memory
        if make_parallelizable is not None:
            if make_parallelizable:
                fn = self._fork_fn_no_gen
            else:
                fn = self._fork_fn
        else:
            fn = self.fn            
        results = super().run(
            iterable=iterable, fn=fn, in_memory=in_memory, *args, **kwargs
        )
        for result in results:
            uid, data_tuples = result
            datasets, labels, metadata = list(), list(), list()
            for tup in data_tuples:
                datasets.append(tup[0])
                labels.append(tup[1])
                metadata.append(tup[2])
            manifest = sample.SampleManifest(
                uid=uid, datasets=datasets, labels=labels,
                metadata=labels
            )
            sample_item = sample.LightPipeSample(data=manifest)
            if load_samples:
                sample_item.load()
            yield sample_item  

    
    def run(self, *args, **kwargs):
        return self.make_samples(*args, **kwargs)


class GridSampleMaker(SampleMaker):
    def _fork_fn(self, iterable_kwargs, *args, **kwargs):
        kwargs = {**kwargs, **iterable_kwargs}           
        grid_cells = gridding.get_grid_cells(*args, **kwargs)
        results = self.join(
            self.fork(
                f=gridding.make_grid_cell_dataset, iterable=grid_cells,
                *args, **kwargs
                )
            )
        for qkey, data_list in results:
            for data in data_list:
                yield qkey, data


    def _fork_fn_no_gen(self, iterable_kwargs, in_memory, *args, **kwargs):
        assert not in_memory, \
            "Cannot parallelize when `in_memory = True`."
        kwargs = {**kwargs, **iterable_kwargs}
        grid_cells = gridding.get_grid_cells(*args, **kwargs)
        results = self.join(
            self.fork(
                f=gridding.make_grid_cell_dataset, iterable=grid_cells,
                in_memory=in_memory, return_filepaths=True, *args, **kwargs
            )
        )
        results_list = list()
        for qkey, data_list in results:
            for data in data_list:
                results_list.append((qkey, data))
        return results_list


    def make_samples(
        self, iterable: Iterable[dict],
        zoom: Optional[int] = gridding.DEFAULT_ZOOM, 
        in_memory: Optional[bool] = None, 
        make_parallelizable: Optional[bool] = None,
        load_samples: Optional[bool] = False, 
        *args, **kwargs
    ) -> Generator:
        if in_memory is None:
            in_memory = self.in_memory
        if make_parallelizable is not None:
            if make_parallelizable:
                fn = self._fork_fn_no_gen
            else:
                fn = self._fork_fn
        else:
            fn = self.fn
        results = self.join(self.fork(
            fn, iterable, zoom=zoom,
            in_memory=in_memory, *args, **kwargs
        ))
        for result in results:
            quad_key, data_tuples = result
            datasets, labels, metadata = list(), list(), list()
            for tup in data_tuples:
                datasets.append(tup[0])
                labels.append(tup[1])
                metadata.append(tup[2])
            manifest = sample.SampleManifest(
                uid=quad_key, datasets=datasets, labels=labels,
                metadata=labels
            )
            sample_item = sample.LightPipeSample(data=manifest)            
            if load_samples:
                sample_item.load()
            yield sample_item
