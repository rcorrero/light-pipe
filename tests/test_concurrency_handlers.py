import functools
import time

from light_pipe.concurrency import concurrency_handlers

NUM_TASKS = 111
SLEEP_TIME = 1
MAX_THREADS = 32
MAX_PROCESSES = 32
CHUNKSIZE = 64
ITERABLE = [SLEEP_TIME] * NUM_TASKS


def test_f(secs):
    time.sleep(secs)
    return "seconds", secs


def test_thread_handler():
    iterable = ITERABLE
    tph = concurrency_handlers.ThreadPoolHandler(max_workers=MAX_THREADS)

    items = tph.join(tph.fork(test_f, iterable))

    start = time.time()
    for item in items:
        print(item)
    end = time.time()
    print(f"Total time to process {NUM_TASKS} items: {end - start} seconds.")


def test_process_handler():
    iterable = ITERABLE
    pph = concurrency_handlers.ProcessPoolHandler(max_workers=MAX_PROCESSES)

    items = pph.join(pph.fork(test_f, iterable))

    start = time.time()
    for item in items:
        print(item)
    end = time.time()
    print(f"Total time to process {NUM_TASKS} items: {end - start} seconds.")


def fork_fn(in_fn, iterable, chunksize, n_threads, *args, **kwargs):
    chunks = [
        iterable[i:i+chunksize] for i in range(0, len(iterable), chunksize)
    ]
    tph = concurrency_handlers.ThreadPoolHandler(max_workers=n_threads)
    tph2 = concurrency_handlers.ThreadPoolHandler(max_workers=n_threads)

    fn = tph2.fork(functools.partial(tph.fork, in_fn), chunks)
    # fn = tph.fork(fn, chunks)
    return fn


def test_thread_handler_in_process_handler():
    iterable = ITERABLE
    chunksize = CHUNKSIZE
    fn = fork_fn(in_fn=test_f, iterable=iterable, chunksize=chunksize, 
        n_threads=MAX_THREADS
    )

    items = concurrency_handlers.ConcurrencyHandler.join(fn)

    start = time.time()
    for item in items:
        print(item)
    end = time.time()
    print(f"Total time to process {NUM_TASKS} items: {end - start} seconds.")


def main():
    test_thread_handler()
    test_process_handler()
    test_thread_handler_in_process_handler()


if __name__ == "__main__":
    main()
