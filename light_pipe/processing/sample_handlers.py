__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains the definition of `SampleHandler` and associated subclasses.
These contain the definition of the sequence of steps necessary to make samples
of a specific type. This operation is defined in the `make_samples` method.
"""


from typing import Callable, Generator, Iterable, Optional, Sequence

from light_pipe import concurrency, gdal_data_handlers, gridding, raster_trans
from light_pipe.processing import sample


class SampleProcessor:
    def __init__(
        self, fn: Optional[Callable] = None, 
        wrappers: Optional[Sequence[Callable]] = None,
        concurrency_handler: Optional[concurrency.ConcurrencyHandler] = None,
        fork: Optional[Callable] = None, join: Optional[Callable] = None
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


    def run(
        self, iterable: Iterable, fn: Optional[Callable] = None, *args, **kwargs
    ) -> Generator:
        if fn is None:
            fn = self.fn
        results = self.join(
            self.fork(self.fn, iterable, *args, **kwargs)
        )
        yield from results


    def set_concurrency(self, concurrency_handler: concurrency.ConcurrencyHandler):
        self.fork = concurrency_handler.fork
        self.join = concurrency_handler.join
        return self


class SampleMaker(SampleProcessor):
    def __init__(
        self, fn: Optional[Callable] = None,in_memory: Optional[bool] = True, 
        *args, **kwargs
    ):
        if fn is None:
            fn = self._fork_fn
        kwargs = {**kwargs, "fn":fn} # Replaces `fn` key in kwargs if present
        super().__init__(*args, **kwargs)
        self.in_memory = in_memory


    @staticmethod
    @gdal_data_handlers.open_data
    def _rasterize_datasources(*args, **kwargs):
        return raster_trans.rasterize_datasources(*args, **kwargs)


    def _fork_fn(self, iterable_kwargs, *args, **kwargs):
        kwargs = {**kwargs, **iterable_kwargs}
        return self._rasterize_datasources(*args, **kwargs)


    def make_samples(
        self, iterable: Iterable, in_memory: Optional[bool] = None, 
        load_samples: Optional[bool] = False, *args, **kwargs
    ) -> Generator:
        if in_memory is None:
            in_memory = self.in_memory
        results = super().run(iterable=iterable, *args, **kwargs)
        for result in results:
            uid, data_tuples = result
            sample_item = sample.LightPipeSample(
                uid=uid, data=data_tuples, *args, **kwargs
            )
            if load_samples:
                sample_item.load()
            yield sample_item  

    
    def run(self, *args, **kwargs):
        return self.make_samples(*args, **kwargs)


class GridSampleMaker(SampleMaker):
    def __init__(
        self, fn: Optional[Callable] = None, *args, **kwargs
    ):
        if fn is None:
            fn = self._fork_fn
        kwargs = {**kwargs, "fn":fn} # Replaces `fn` key in kwargs if present
        super().__init__(*args, **kwargs)


    @staticmethod
    @gdal_data_handlers.open_data
    def _make_grid_cell_datasets(*args, **kwargs):
        return gridding.make_grid_cell_datasets(*args, **kwargs)

    
    def _fork_fn(self, iterable_kwargs, *args, **kwargs):
        kwargs = {**kwargs, **iterable_kwargs}
        return self._make_grid_cell_datasets(*args, **kwargs)


    def make_samples(
        self, iterable: Iterable[dict], zoom: Optional[int] = 14, 
        in_memory: Optional[bool] = None, load_samples: Optional[bool] = False, 
        *args, **kwargs
    ) -> Generator:
        if in_memory is None:
            in_memory = self.in_memory
        
        results = self.join(self.fork(
            self._fork_fn, iterable, zoom=zoom,
            in_memory=in_memory, *args, **kwargs
        ))
        for result in results:
            quad_key, data_tuples = result
            grid_sample = sample.GridSample(
                quad_key=quad_key, data=data_tuples, *args, **kwargs
            )
            if load_samples:
                grid_sample.load()
            yield grid_sample
