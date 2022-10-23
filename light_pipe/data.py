from typing import Any, Callable, Optional


class Data:
    def __init__(
        self, generator: Optional[Callable] = None, 
        store_results: Optional[bool] = False, results: Optional[Any] = None,
        *args, **kwargs
    ):
        self.generator = generator
        self.store_results = store_results
        self.results = results
        self.args = args
        self.kwargs = kwargs


    def wrap_generator(self, wrapper_fn: Callable, *args, **kwargs):
        self.generator = wrapper_fn(self.generator, *args, **kwargs)


    def generate(self, *args, **kwargs):
        args = (*args, *self.args)
        kwargs = {**kwargs, **self.kwargs}
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


    def block(self, *args, no_return: Optional[bool] = False, **kwargs):
        if no_return:
            for _ in self.generate(*args, **kwargs):
                pass
            return
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


    def __enter__(
        self, generator: Optional[Callable] = None, 
        store_results: Optional[bool] = None, results: Optional[Any] = None,
        *args, **kwargs
    ):
        if generator is not None:
            self.generator = generator
        if store_results is not None:
            self.store_results = store_results
        if results is not None:
            self.results = results
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
        copy_results: Optional[bool] = True, results: Optional[Any] = None,
        *args, **kwargs
    ):
        if generator is None:
            generator = self.generator
        if store_results is None:
            store_results = self.store_results
        if copy_results and results is None:
            results = self.results
        args = (*args, *self.args)
        kwargs = {**kwargs, **self.kwargs}
        return self._copy(
           *args, generator=generator, store_results=store_results, 
           results=results, **kwargs
        )        
