Module light_pipe.parallelizer
==============================

Classes
-------

`AsyncGatherer()`
:   

    ### Ancestors (in MRO)

    * light_pipe.parallelizer.Parallelizer

`BlockingPooler(max_workers: Optional[int] = None, queue_size: Optional[int] = None, DefaultBlockingExecutor: Optional[type] = None, executor: Union[concurrent.futures.thread.ThreadPoolExecutor, concurrent.futures.process.ProcessPoolExecutor, ForwardRef(None)] = None)`
:   

    ### Ancestors (in MRO)

    * light_pipe.parallelizer.Parallelizer

    ### Descendants

    * light_pipe.parallelizer.BlockingProcessPooler
    * light_pipe.parallelizer.BlockingThreadPooler

`BlockingProcessPooler(*args, DefaultBlockingExecutor: Optional[type] = concurrent.futures.process.ProcessPoolExecutor, **kwargs)`
:   

    ### Ancestors (in MRO)

    * light_pipe.parallelizer.BlockingPooler
    * light_pipe.parallelizer.Parallelizer

`BlockingThreadPooler(*args, DefaultBlockingExecutor: Optional[type] = concurrent.futures.thread.ThreadPoolExecutor, **kwargs)`
:   

    ### Ancestors (in MRO)

    * light_pipe.parallelizer.BlockingPooler
    * light_pipe.parallelizer.Parallelizer

`Parallelizer()`
:   

    ### Descendants

    * light_pipe.parallelizer.AsyncGatherer
    * light_pipe.parallelizer.BlockingPooler
    * light_pipe.parallelizer.Pooler

`Pooler(max_workers: Optional[int] = None, DefaultExecutor: Optional[type] = None, executor: Union[concurrent.futures.thread.ThreadPoolExecutor, concurrent.futures.process.ProcessPoolExecutor, ForwardRef(None)] = None)`
:   

    ### Ancestors (in MRO)

    * light_pipe.parallelizer.Parallelizer

    ### Descendants

    * light_pipe.parallelizer.ProcessPooler
    * light_pipe.parallelizer.ThreadPooler

`ProcessPooler(*args, DefaultExecutor: Optional[type] = concurrent.futures.process.ProcessPoolExecutor, **kwargs)`
:   

    ### Ancestors (in MRO)

    * light_pipe.parallelizer.Pooler
    * light_pipe.parallelizer.Parallelizer

`TestParallelizers(methodName='runTest')`
:   A class whose instances are single test cases.
    
    By default, the test code itself should be placed in a method named
    'runTest'.
    
    If the fixture may be used for many test cases, create as
    many test methods as are needed. When instantiating such a TestCase
    subclass, specify in the constructor arguments the name of the test method
    that the instance is to execute.
    
    Test authors should subclass TestCase for their own tests. Construction
    and deconstruction of the test's environment ('fixture') can be
    implemented by overriding the 'setUp' and 'tearDown' methods respectively.
    
    If it is necessary to override the __init__ method, the base class
    __init__ method must always be called. It is important that subclasses
    should not change the signature of their __init__ method, since instances
    of the classes are instantiated automatically by parts of the framework
    in order to be run.
    
    When subclassing TestCase, you can set these attributes:
    * failureException: determines which exception will be raised when
        the instance's assertion methods fail; test methods raising this
        exception will be deemed to have 'failed' rather than 'errored'.
    * longMessage: determines whether long messages (including repr of
        objects used in assert methods) will be printed on failure in *addition*
        to any explicit message passed.
    * maxDiff: sets the maximum length of a diff in failure messages
        by assert methods using difflib. It is looked up as an instance
        attribute so can be configured by individual tests if required.
    
    Create an instance of the class that will use the named test
    method when executed. Raises a ValueError if the instance does
    not have a method with the specified name.

    ### Ancestors (in MRO)

    * unittest.case.TestCase

    ### Static methods

    `task(num_tasks_submitted: int)`
    :

    ### Methods

    `test_async_gatherer(self)`
    :

    `test_blocking_process_pooler(self)`
    :

    `test_blocking_thread_pooler(self)`
    :

`ThreadPooler(*args, DefaultExecutor: Optional[type] = concurrent.futures.thread.ThreadPoolExecutor, **kwargs)`
:   

    ### Ancestors (in MRO)

    * light_pipe.parallelizer.Pooler
    * light_pipe.parallelizer.Parallelizer