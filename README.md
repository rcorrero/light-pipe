# Light-Pipe

---

## Overview

Light-Pipe is a open source library that facilitates the development of highly-efficient, massively-scalable geospatial data pipelines for use in machine-learning applications. Command-line utilities are provided for all of the core library functions, along with a Python API for developers.

Light-Pipe has one non-standard Python dependency, the [`osgeo`](https://gdal.org/) library, which is released under an MIT style Open Source License by [The Open Source Geospatial Foundation](https://www.osgeo.org/), to whom I owe a massive debt of gratitude.

## Python API Guidelines

The following is a list of guidelines which this package follows:

1. Light-Pipe presents an abstract data model to the user, the `LightPipeSample`, along with `LightPipeline`, a class which is used to produce `LightPipeSample` instances from user-provided geospatial data. `LightPipeline` preprocesses the supplied data to prepare `LightPipeSamples`, instances which are analysis-ready.

2. Light-Pipe is designed from the ground-up to support concurrency in the form of multi-threading, parallelism in the form of multi-processing, and parallelism across multiple machines. This means that all data preprocessing may be scaled arbitrarily to meet the needs of users. The software guarantees that all multi-threaded operations are thread-safe. All supported concurrency modes may be used with each sample type.

3. Concurrency is controlled by `ConcurrencyHandler` instances which may be created by the user. The `ConcurrencyHandler` interface consists of two methods, `fork` and `join`, names which coincide with their traditional interpretation in [parallel programming contexts](https://en.wikipedia.org/wiki/Fork%E2%80%93join_model). `fork` operations may be nested recursively, and instances of different `ConcurrencyHandler` subclasses may be mixed and matched to achieve the desired approach to concurrency (for example, calls to the `fork` method of a `ThreadPoolHandler` instance may be nested within calls to the `fork` method of a `ProcessPoolHandler` instance). To do so, the user may define his or her own custom subclasses of `ConcurrencyHandler`.

4. `LightPipeline` is sample-type agnostic, and can operate on samples in a concurrent and/or parallel fashion. Each sample type is associated with a subclass of `LightPipeSample` and a subclass of `SampleHandler`. `SampleHandler` defines the sequence of operations necessary to produce analysis-ready samples from input files in a concurrency-type agnostic manner. 

5. All operations are idempotent and do not modify any of the input data. References to generated files, such as the unique identifier associated with the file, are invariant across operations to allow for idempotence (e.g. filepaths are georeferenced or referenced in some other unique manner so that multiple calls to `make_data` will produce the same result as a single call).

6. All Light-Pipe operations may be performed in memory, with no writing to disk necessary. Generated objects can be written to disk (subject to constraints imposed by the `osgeo` package).

7. Every operation which can be performed [lazily](https://en.wikipedia.org/wiki/Lazy_evaluation) without violating the other guidelines listed here is done so.

9. `LightPipeSample` supports tile generation, which allows for the reading of subarrays of larger raster data arrays. It also allows for the reading of entire sample arrays. Each tile is associated with a `Tile` instance from which NumPy arrays may be accessed for training or in deployment.

10. The user may provide his or her own thread pool or process pool objects where necessary.

11. Threading may be accomplished using `concurrent.futures.ThreadPoolExecutor` instances or `queue` instances.

12. Every IO operation or calculation is done only once in data processing. There are no duplicate operations, and no data is written to disk except by the user's calling a `LightPipeSample` instance's `save()` method.

13. Full support for the `osgeo` package's virtual file system tools is provided.

---

Copyright 2020-2022 [Richard Correro](mailto:rcorrero@stanford.edu).