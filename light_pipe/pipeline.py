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
