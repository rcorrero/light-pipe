Module light_pipe.transformer
=============================

Classes
-------

`Transformer(transform_item: Optional[Callable] = None, join_fn: Optional[Callable] = None, parallelizer: Optional[light_pipe.parallelizer.Parallelizer] = <light_pipe.parallelizer.Parallelizer object>, *args, **kwargs)`
:   

    ### Static methods

    `fork(f: Callable, iterable: Iterable, *args, recurse: Optional[bool] = True, **kwargs) ‑> Generator`
    :

    `join(iterable: Iterable, recurse: Optional[bool] = True) ‑> Generator`
    :

    `transform_item(*args, **kwargs)`
    :

    ### Methods

    `transform(self, data: light_pipe.data.Data, *args, return_copy: Optional[bool] = True, block: Optional[bool] = False, **kwargs) ‑> light_pipe.data.Data`
    :