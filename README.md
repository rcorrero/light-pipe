# [Light-Pipe](https://github.com/rcorrero/light-pipe)

---

## Overview

[Light-Pipe](https://www.light-pipe.io/) is a open source library that facilitates the development of highly-efficient, massively-scalable geospatial data pipelines for use in machine-learning applications. Light-Pipe can be used during model development to produce and interact with training data in an efficient manner. It may also be used to deploy a trained model. A Python API is provided for developers, and command-line utilities for all of the core library operations will be available soon. Light-Pipe is released under a [BSD 3-Clause License](https://opensource.org/licenses/BSD-3-Clause).

Light-Pipe has one non-standard Python dependency, the [`osgeo`](https://gdal.org/) library, which is released under an MIT style open source license by [The Open Source Geospatial Foundation](https://www.osgeo.org/).

## Python API Guidelines

The following is a list of guidelines which this package follows:

1. Light-Pipe handles geospatial data processing and model deployment. It may be used to generate analysis-ready samples on-the-fly during both training and production. Core operations are provided, and abstractions are provided which allow the user to define custom operations.

2. Light-Pipe is geospatially-aware and abstracts away the minutiae of geospatial data, allowing the user to focus instead on model development, training, and evaluation.

3. Light-Pipe also handles [concurrency](https://en.wikipedia.org/wiki/Concurrency_(computer_science)). Light-Pipe is designed from the ground-up to support concurrency in the form of multi-threading, parallelism in the form of multi-processing, and parallelism across multiple machines. This means that all data processing may be scaled arbitrarily to meet the needs of users during both training and deployment.

4. All provided operations are [idempotent](https://en.wikipedia.org/wiki/Idempotence) and do not modify any of the input data. References to generated files, such as unique identifiers associated with them, are invariant across repeated operations.

5. All Light-Pipe operations may be performed in memory, with no writing to disk necessary. Generated objects can be written to disk (subject to constraints imposed by the `osgeo` package).

6. The key abstraction which Light-Pipe presents is the `LightPipeline` class. Each `LightPipeline` instance is associated with one or more `SampleProcessor` instances, each of which defines an operation to be performed on the provided data. The user provides a sequence of `SampleProcessor`s to a `LightPipeline` instance which will execute them in order.

7. `SampleProcessor` instances define the operations performed by a `LightPipeline` on user-provided data and are therefore the basic building blocks of `LightPipeline`s. Model training or deployment may be incorporated into a `LightPipeline` by passing a `Callable` to a `SampleProcessor` instance or by creating a user-defined subclass of `SampleProcessor` which performs the desired operations. `SampleProcessor` is used along with a `ConcurrencyHandler` instance to deploy a user-provided `Callable`, optionally wrapped with one or more user-provided wrapper `Callable`s, in a concurrent manner. Each `SampleProcessor` instance is itself associated with a `ConcurrencyHandler` which does exactly what its name suggests: it handles the technicalities of concurrency so that the user doesn't have to. This means that operations associated with `SampleProcessor`s may be scaled automatically, as required by the user.

8. The `ConcurrencyHandler` interface consists of two methods, `fork` and `join`, names which coincide with their traditional interpretation in [parallel programming contexts](https://en.wikipedia.org/wiki/Fork%E2%80%93join_model). `fork` operations may be nested recursively, and instances of different `ConcurrencyHandler` subclasses may be mixed and matched to achieve the desired approach to concurrency (for example, calls to the `fork` method of a `ThreadPoolHandler` instance may be nested within calls to the `fork` method of a `ProcessPoolHandler` instance). To do so, the user may define custom subclasses of `ConcurrencyHandler`.

9. `SampleMaker` (a subclass of `SampleProcessor`) and its subclasses may be used to create `LightPipeSample` instances. These support key operations such as automatic raster tiling and the saving of user-created data in a georeferenced manner. `SampleMaker` defines the sequence of operations necessary to produce samples from input files in a concurrent manner (as defined by the supplied `ConcurrencyHandler` instance).

10. `LightPipeSample` supports tile generation, which allows for the reading of subarrays of larger raster data arrays. It also allows for the reading of entire sample arrays. Each tile is associated with a `Tile` instance from which NumPy arrays may be accessed during training or deployment.

11. `SampleProcessor` instances are designed to ensure that samples are produced and operated on in a manner which is consistent with the requirements imposed by the provided `ConcurrencyHandler` instance. For example, `concurrent.futures` requires that objects passed to a `ProcessPoolExecutor` are [serializable](https://docs.python.org/3/library/pickle.html). In practice this means that generator objects and `osgeo.gdal.Dataset` instances cannot be passed into or out of a `ProcessPoolExecutor`, and therefore sample processors must ensure that such objects are neither passed nor returned from processing functions when the `ProcessPoolHandler` is used.

12. Every operation which can be performed [lazily](https://en.wikipedia.org/wiki/Lazy_evaluation) without violating the other guidelines listed here is done so.

13. The user may supply thread pool or process pool objects to `ConcurrencyHandler`s where necessary, thereby allowing for more control over the degree of concurrency.

14. Every IO operation or calculation is done only once in data processing. There are no duplicate operations, and no data is written to disk except by the user's calling a `LightPipeSample` instance's `save()` method. Every operation is designed to be as computationally-efficient as possible.

15. Full support for the `osgeo` package's virtual file system tools is provided.

---

Copyright 2020-2022 [Richard Correro](mailto:rcorrero@stanford.edu).