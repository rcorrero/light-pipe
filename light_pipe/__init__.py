__author__ = "Richard Correro(rcorrero@stanford.edu)"

__version__ = '0.1'

__doc__ = """

# [Light-Pipe](https://github.com/rcorrero/light-pipe)

---

## Overview

[Light-Pipe](https://www.light-pipe.io/) is a open source library that facilitates the development of highly-efficient, massively-scalable geospatial data pipelines for use in machine-learning applications. Light-Pipe can be used during model development to produce and interact with training data in an efficient manner. It may also be used to deploy trained models at scale. A Python API is provided for developers, and command-line utilities for all of the core library operations will be available soon. Light-Pipe is released under a [BSD 3-Clause License](https://opensource.org/licenses/BSD-3-Clause).

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

10. `LightPipeSample` supports tile generation, which allows for the reading of sub-arrays of larger raster data arrays. It also allows for the reading of entire sample arrays. Each tile is associated with a `Tile` instance from which NumPy arrays may be accessed during training or deployment.

11. `SampleProcessor` instances are designed to ensure that samples are produced and operated on in a manner which is consistent with the requirements imposed by the provided `ConcurrencyHandler` instance. For example, `concurrent.futures` requires that objects passed to a `ProcessPoolExecutor` are [serializable](https://docs.python.org/3/library/pickle.html). In practice this means that generator objects and `osgeo.gdal.Dataset` instances cannot be passed into or out of a `ProcessPoolExecutor`, and therefore sample processors must ensure that such objects are neither passed nor returned from processing functions when the `ProcessPoolHandler` is used.

12. Every operation which can be performed [lazily](https://en.wikipedia.org/wiki/Lazy_evaluation) without violating the other guidelines listed here is done so.

13. The user may supply thread pool or process pool objects to `ConcurrencyHandler`s where necessary, thereby allowing for more control over the degree of concurrency.

14. Every IO operation or calculation is done only once in data processing. There are no duplicate operations, and no data is written to disk except by the user's calling a `LightPipeSample` instance's `save()` method. Every operation is designed to be as computationally-efficient as possible.

15. Full support for the `osgeo` package's virtual file system tools is provided.

## More Information

- [GitHub](https://github.com/rcorrero/light-pipe)

- [Documentation](https://www.light-pipe.io/)

## Comparison With Existing Packages, or "How I Learned to Stop Worrying and Love [GDAL](https://gdal.org/)"

### Raster Tiling

Raster files retrieved from data providers, such as satellite images, are generally too-large to be input directly into deep-learning-based computer vision models. The [ResNet-50 model](https://arxiv.org/pdf/1512.03385.pdf), for example, requires inputs of 224 pixels in width and 224 pixels in height whereas a typical [Landsat 8-9 Operational Land Imagery (OLI) panchromatic image](https://www.usgs.gov/faqs/what-are-band-designations-landsat-satellites) is approximately 12200 pixels in width and 11300 pixels in height. Therefore, to use a ResNet-50 model with a Landsat image, one must first pre-process the image into over 2800 image tiles (also known as "chips"). Imagery sourced from other providers would generally yield a similar number of tiles.

This pre-processing step must be performed _every time_ a satellite image is fed into a model, both while training the model and during deployment. Therefore, to deploy a model to analyze a large number of satellite image, one must be able to tile images as quickly and efficiently as possible.

> Explain Space and time complexity

Light-Pipe is designed to produce image tiles as efficiently as possible. Although Light-Pipe allows the user to save tiles (i.e. write to disk), this is generally undesirable when working with large quantities of data, since doing so would at least double the disk space required to conduct an analysis (each pixel value in the input raster will be written to at least one output tile file, and metadata will be duplicated across output files).For this reason, Light-Pipe supports in-memory data transformations such as raster tiling. Such transformations can be performed "on-the-fly", meaning that Light-Pipe transforms input data as it is needed, no pre-processing step required.

To test Light-Pipe's performance on this task, let's compare it with existing tools. Although there may be other (possibly better) tools for the task of raster tiling, two of the best and most popular are [`rio-tiler`](https://github.com/cogeotiff/rio-tiler) and [`solaris`](https://github.com/CosmiQ/solaris). Both have large followings and feature contributions from skilled developers. Although there are similar tools (such as [`rastervision`](https://github.com/azavea/raster-vision)), they generally use similar implementation to either `rio-tiler` or `solaris`. `rastervision`, for example, produces image tiles by essentially the same process as `solaris`.

For this test, we'll use the GeoTiff image found [here](https://s3.amazonaws.com/spacenet-dataset/AOIs/AOI_1_Rio/PS-RGB/PS-RGB_mosaic_013022223133.tif) (__CAUTION__, clicking this link will automatically download a reasonably large file). We'll test two methods of tile generation. The first method involves simply reading sub-arrays from the input image of a specified width and height (in pixels). The second method requires extracting regions from the input image specified not in pixel coordinates, as in the first method, but in geographic coordinates (e.g. degrees of latitude/longitude). This method is more difficult, as the input image may not use the same coordinate reference system as used to specify the coordinates for sub-sampling, and therefore may need to be reprojected into the target coordinate reference system first. Also, there are no guarantees in general that the specified coordinates for a region align with the locations of the pixels even after the input image has been reprojected into the same coordinate reference system. Although it is more computationally-expensive to tile according to geographic coordinates rather than pixel coordinates, it is necessary to use geographic coordinates when making imagery sequences with imagery. This means that sequential models such as [Long short-term memory networks](https://en.wikipedia.org/wiki/Long_short-term_memory) require input tiles which have been subdivided using geographic coordinates, assuming the goal is to generate predictions corresponding to target locations. That is to say, there are very good reasons to use this approach besides making pretty time-lapses from satellite imagery.

#### Results

Below is a chart displaying the runtime (in seconds) for both Light-Pipe and `solaris` when extracting sub-regions of an image specified in _pixel coordinates_. As far as I'm aware it's impossible to extract regions specified in terms of pixel coordinates using `rio-tiler`.

> Insert pixel coordinate tasks chart

These results were generated using this script, which you should test out yourself so that you don't have to take my word for it. These results were obtained on my local machine, which has an AMD Ryzen 5 2600 processor with 16 GB of DDR4 SDRAM with a SDD. The results for each method were averaged across several trials.

Next, I tested the performance of the three methods when extracting sub-regions specified in _geographic coordinates_.

> Insert geographic coordinate tasks chart

Again, the script used to obtain these results is available (here), and again I performed this test on my local machine.

We see that Light-Pipe handily outperforms each on the alternatives of both tasks. This test is essentially a _worst-case_ test for Light-Pipe, which is designed with concurrency tools which are most useful when processing large numbers of samples. Here we only process a single sample, giving the other two methods the best possible chance. When training a model, and especially when running a model in production, one may need to process thousands or even millions of images, which is where Light-Pipe's concurrency tools make it extremely attractive. 

If you look at the code closely, you'll see that `solaris` writes/reads each tile to/from disk, whereas `rio-tiler` generates its tiles without writing to disk. Although there are situations in which it is useful to store the tiles for future use, writing tiles to disk can eat up space, prohibitively so if one wants to analyze a large number of images. For example, the size on disk of the sub-regions extracted by `solaris` using geographic coordinates is approximately 510 MB on my machine, whereas the original image was only 93 MB. That means that image tiling would require over six times as much storage using `solaris` than would be when using Light-Pipe. As mentioned previously, `rastervision` uses a similar process to `solaris` to generate tiles. Although `rio-tiler` does not need to write to disk to make tiles, we see that it is much slower than Light-Pipe on both tasks.

You shouldn't choose to use Light-Pipe solely because it's faster at these tasks, but it should definitely influence your decision-making. There are other reasons why you may want to use it: its clean, intuitive API, its native concurrency support, its extensibility, its lack of dependencies (except for GDAL), and so on. Importantly, Light-Pipe handles tasks that no existing tool can, such as automatic joining of image tiles obtained from input raster and vector files using geographic coordinates. So please give it a go, and if you have any suggestions, don't hesitate to [email](mailto:rcorrero@stanford.edu) me or submit a [pull request](https://github.com/rcorrero/light-pipe/pulls).



---

Copyright 2020-2022 [Richard Correro](mailto:rcorrero@stanford.edu).
"""
