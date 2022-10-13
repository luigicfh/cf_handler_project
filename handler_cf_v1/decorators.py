from functools import wraps
import time
import os


def func_exec_time(func):
    @wraps(func)
    def func_exec_time_wrapper(self, *args, **kwargs):
        start_time = time.perf_counter()
        result = func(self, *args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(
            f'Function {func.__name__} Took {total_time:.4f} seconds')
        return result
    return func_exec_time_wrapper
