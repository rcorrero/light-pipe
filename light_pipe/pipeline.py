__author__ = "Richard Correro (rcorrero@stanford.edu)"

__doc__ = """
This module contains the definition of `LightPipeline`, a key component of the
API of this package and the primary method through which the user performs
operations on data and creates samples.
"""


from typing import Generator, Iterable, Optional, Sequence

from light_pipe import concurrency, processing


class LightPipeline:
    def __init__(
        self, inputs: Optional[Iterable] = None,
        processors: Optional[Sequence[processing.SampleProcessor]] = None,
        concurrency_handler: Optional[concurrency.ConcurrencyHandler] = None,
        blocking: Optional[bool] = False, in_memory: Optional[bool] = True
    ):
        self.inputs = inputs       
        if processors is None:
            processors = [processing.SampleMaker()]
        if concurrency_handler is not None:
            for processor in processors:
                processor.set_concurrency(concurrency_handler=concurrency_handler)
        self.processors = processors
        self.concurrency_handler = concurrency_handler
        self.blocking = blocking
        self.in_memory = in_memory
    
        self._results = None
        self._i = None
        self._n = None


    def run(
        self, iterable: Optional[Iterable] = None, 
        processors: Optional[Sequence[processing.SampleProcessor]] = None,
        blocking: Optional[bool] = None, in_memory: Optional[bool] = None, 
        *args, **kwargs
    ):
        if iterable is None:
            iterable = self.inputs
            assert iterable is not None, "Parameter `iterable` not set."
        if processors is None:
            processors = self.processors
        if blocking is None:
            blocking = self.blocking
        if in_memory is None:
            in_memory = self.in_memory
        for processor in processors:
            iterable = processor.run(iterable, in_memory=in_memory, *args, **kwargs)
        if blocking:
            results_list = list()
            for item in iterable:
                results_list.append(item)
            results = results_list
            self._i = 0 # Reset each time `run` is called.
            self._n = len(results)
        else:
            results = iterable
        self._results = results
        return results


    def __iter__(self):
        if self._results is None:
            self.run()
            assert self._results is not None
        return self


    def __next__(self):
        return self.next()


    def next(self):
        if self._results is None:
            self.run()
            assert self._results is not None
        if isinstance(self._results, Generator):
            item = next(self._results)
        else:
            i = self._i
            n = self._n
            if i < n:
                item = self._results[i]
                self._i += 1
            else:
                raise StopIteration
        return item
