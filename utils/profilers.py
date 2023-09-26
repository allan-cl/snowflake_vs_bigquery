import time
import functools


class BenchmarkProfiler:

    def __init__(self):
        self.start_time = None

    def start(self):
        self.start_time = time.perf_counter()

    def stop(self):
        if self.start_time is None:
            return None
        elapsed_time = time.perf_counter() - self.start_time
        self.start_time = None
        return elapsed_time


def exec_time_profiler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        profiler = BenchmarkProfiler()
        profiler.start()
        result = func(*args, **kwargs)
        duration = profiler.stop()

        print(f"\nExecute {func.__name__}() Time: {duration:>.3f} seconds")
        return result
    return wrapper
