__author__ = "Richard Correro (rcorrero@gmail.com)"


from typing import Callable, List, Optional


class Data:
    def __init__(
        self, generator: Optional[Callable] = None, 
        store_results: Optional[bool] = False,
        _results_stored: Optional[bool] = False,
        *args, **kwargs
    ):
        self.generator = generator
        self.store_results = store_results
        self._results_stored = _results_stored        
        self.args = args
        self.kwargs = kwargs


    def wrap_generator(self, wrapper_fn: Callable, *args, **kwargs):
        self.generator = wrapper_fn(self.generator, *args, **kwargs)
        self._results_stored = False


    @staticmethod
    def _yield_results(results: List):
        def _yield_results_inner(*args, results = results, **kwargs): 
            results = iter(results)
            yield from results
        return _yield_results_inner


    def generate(self, *args, **kwargs):
        args = (*args, *self.args)
        kwargs = {**kwargs, **self.kwargs}
        if not self._results_stored and self.store_results:
            results = list()
            for res in self.generator(*args, **kwargs):
                results.append(res)
                yield res
            self.generator = self._yield_results(results)
            self._results_stored = True
        else:
            yield from self.generator(*args, **kwargs)        


    def block(self, *args, no_return: Optional[bool] = False, **kwargs):
        if no_return:
            results = self.generate(*args, **kwargs)
            while results:
                try:
                    next(results)
                except StopIteration:
                    return
        results = list(self.generate(*args, **kwargs))            
        return results


    def __call__(self, *args, block: Optional[bool] = False, **kwargs):
        if block:
            return self.block(*args, **kwargs)
        return self.generate(*args, **kwargs)


    def __iter__(self):
        return self()        


    def __enter__(
        self, generator: Optional[Callable] = None, 
        store_results: Optional[bool] = None,
        *args, **kwargs
    ):
        if generator is not None:
            self.generator = generator
        if store_results is not None:
            self.store_results = store_results
        self.args = (*args, *self.args)
        self.kwargs = {**kwargs, **self.kwargs}
        return self


    def __exit__(self, exc_type, exc_value, traceback):
        block = True
        no_return = True
        self(block=block, no_return=no_return)


    @classmethod
    def _copy(cls, *args, **kwargs):
        return cls(*args, **kwargs)


    def copy(
        self, generator: Optional[Callable] = None, 
        store_results: Optional[bool] = None,
        *args, **kwargs
    ):
        if generator is None:
            generator = self.generator
        if store_results is None:
            store_results = self.store_results
        _results_stored = self._results_stored
        args = (*args, *self.args)
        kwargs = {**kwargs, **self.kwargs}
        return self._copy(
           *args, generator=generator, store_results=store_results, 
           _results_stored=_results_stored, **kwargs
        )        
