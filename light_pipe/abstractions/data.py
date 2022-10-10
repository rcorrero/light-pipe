from typing import Callable, Optional


class Data:
    def __init__(
        self, generator: Optional[Callable] = None, 
        store_results: Optional[bool] = False
    ):
        self.generator = generator
        self.store_results = store_results

        self.results = None


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


    def __call__(self, *args, **kwargs):
        return self.generate(*args, **kwargs)


    def __iter__(self):
        if self.store_results and self.results is not None:
            yield from self.results
        else:
            return self()


    def block(self, *args, **kwargs):
        results = list(self.generate(*args, **kwargs))
        return results
