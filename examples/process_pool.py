from light_pipe import concurrency, processing, pipeline


def fn(x, *args, **kwargs):
    return x, x * 100


def main():
    iterable = range(10)

    ch = concurrency.ProcessPoolHandler(max_workers=1)
    proc = processing.SampleProcessor(fn=fn, concurrency_handler=ch)

    # results = proc.run(iterable)

    # for result in results:
    #     print(result)

    pipe = pipeline.LightPipeline(iterable, proc)

    for result in pipe:
        print(result)


if __name__ == "__main__":
    main()
