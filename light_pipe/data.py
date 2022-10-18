from typing import Any, Callable, Optional


class Data:
    def __init__(
        self, generator: Optional[Callable] = None, 
        store_results: Optional[bool] = False, results: Optional[Any] = None
    ):
        self.generator = generator
        self.store_results = store_results
        self.results = results


    def wrap_generator(self, wrapper_fn: Callable, *args, **kwargs):
        self.generator = wrapper_fn(self.generator, *args, **kwargs)


    def generate(self, *args, **kwargs):
        if self.store_results:
            if self.results is not None:
                yield from self.results
            else:
                results = list()
                for res in self.generator(*args, **kwargs):
                    results.append(res)
                    yield res
                self.results = results
        else:
            yield from self.generator(*args, **kwargs)


    def block(self, *args, **kwargs):
        results = list(self.generate(*args, **kwargs))
        return results


    def __call__(self, *args, block: Optional[bool] = False, **kwargs):
        if block:
            return self.block(*args, **kwargs)
        return self.generate(*args, **kwargs)


    def __iter__(self):
        if self.store_results and self.results is not None:
            return self.results
        else:
            return self()        


    @classmethod
    def _copy(cls, *args, **kwargs):
        return cls(*args, **kwargs)


    def copy(
        self, generator: Optional[Callable] = None, 
        store_results: Optional[bool] = None,
        copy_results: Optional[bool] = False, results: Optional[Any] = None,
        *args, **kwargs
    ):
        if generator is None:
            generator = self.generator
        if store_results is None:
            store_results = self.store_results
        if copy_results and results is None:
            results = self.results
        return self._copy(
           *args, generator=generator, store_results=store_results, 
           results=results, **kwargs
        )        
