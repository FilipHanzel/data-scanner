import os
from timeit import default_timer as timer

import line_profiler
import memory_profiler

from data_scanner import Processor

# TODO: Write unittests
# TODO: Write "load" tests (with simple data generator script for bigger datasets)
# TODO: Use enum to store all available types

if __name__ == "__main__":
    # Time comparisson between parallel and single-process execution
    print("[TEST] Running timeit test...")
    processor = Processor(os.path.join("data", "csv"))

    start = timer()
    processor.run()
    end = timer()
    print(f"[TIMEIT RUN] Single process run took ~{round(end - start, 5)} seconds")

    start = timer()
    processor.run_workers()
    end = timer()
    print(f"[TIMEIT RUN] Multiprocessing run took ~{round(end - start, 5)} seconds")

    # Line profiling only for single-process execution,
    # since all the workers are doing exactly the same thing
    # we can assume that performance per worker will be
    # the same as performance here
    print("[TEST] Running line-profilers...")

    profiler = line_profiler.LineProfiler()
    profiled_function = profiler(processor.run)
    profiled_function()

    print("[TEST] Ouput from processor.run profile:")
    profiler.print_stats()

    profiler = memory_profiler.LineProfiler()
    profiled_function = profiler(processor.run)
    profiled_function()

    print("[TEST] Ouput from processor.run profile:")
    memory_profiler.show_results(profiler)
