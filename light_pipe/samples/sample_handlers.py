__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains the definition of `SampleHandler` and associated subclasses.
These contain the definition of the sequence of steps necessary to make samples
of a specific type. This operation is defined in the `make_samples` method.
"""


from typing import Callable, Generator, Iterable, Optional

from light_pipe import gridding, raster_trans
from light_pipe.concurrency import concurrency_handlers
from light_pipe.samples import sample


class SampleHandler:
    def __init__(
        self, 
        concurrency_handler: Optional[concurrency_handlers.ConcurrencyHandler] = None,
        fork: Optional[Callable] = None, join: Optional[Callable] = None, 
        in_memory: Optional[bool] = True, 
    ):
        if fork is not None and join is not None:
            self.fork = fork
            self.join = join
        elif concurrency_handler is None:
            concurrency_handler = concurrency_handlers.ConcurrencyHandler
            self.fork = concurrency_handler.fork
            self.join = concurrency_handler.join
        else:
            self.fork = concurrency_handler.fork
            self.join = concurrency_handler.join

        self.in_memory = in_memory


    @classmethod
    def fork_fn(cls, iterable_kwargs, *args, **kwargs):
        kwargs = {**iterable_kwargs, **kwargs}
        return raster_trans.rasterize_datasources(*args, **kwargs)


    def make_samples(
        self, iterable: Iterable, in_memory: Optional[bool] = None, 
        load_samples: Optional[bool] = False, *args, **kwargs
    ) -> Generator:
        if in_memory is None:
            in_memory = self.in_memory
        
        results = self.join(self.fork(
            self.fork_fn, iterable, in_memory=in_memory, *args, **kwargs
        ))
        for result in results:
            uid, data_tuples = result
            sample_item = sample.LightPipeSample(
                uid=uid, data=data_tuples, *args, **kwargs
            )
            if load_samples:
                sample_item.load()
            yield sample_item    
        
    
class GridSampleHandler(SampleHandler):
    @classmethod
    def fork_fn(cls, iterable_kwargs, *args, **kwargs):
        kwargs = {**iterable_kwargs, **kwargs}
        return gridding.make_grid_cell_datasets(*args, **kwargs)


    def make_samples(
        self, iterable: Iterable[dict], zoom: Optional[int] = 14, 
        in_memory: Optional[bool] = None, load_samples: Optional[bool] = False, 
        *args, **kwargs
    ) -> Generator:
        if in_memory is None:
            in_memory = self.in_memory
        
        results = self.join(self.fork(
            self.fork_fn, iterable, zoom=zoom,
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
