import os
import sys

import line_profiler
import memory_profiler

data_scanner_path = os.path.join(os.path.dirname(__file__), "..")
sys.path.append(data_scanner_path)

from data_scanner import Processor
from data_scanner.loader import CSVLoader
from data_scanner.scanner import Scanner


def main():
    # Point processor to a single file, since line profiling
    # doesn't work well with multiprocessing
    script_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_path, "data")
    file_path = os.path.join(data_path, os.listdir(data_path)[0])

    print("[INFO] Running line-profilers on Processor (run)...")
    processor = Processor(file_path)

    profiler = line_profiler.LineProfiler()
    profiled_function = profiler(processor.run)
    profiled_function()
    profiler.print_stats()

    profiler = memory_profiler.LineProfiler()
    profiled_function = profiler(processor.run)
    profiled_function()
    memory_profiler.show_results(profiler)

    print("[INFO] Running line profilers on scanner (get_dtype)...")
    loader = CSVLoader()
    scanner = Scanner(loader.load(file_path))
    _scanner_get_dtype = scanner.get_dtype

    profiler = line_profiler.LineProfiler()
    scanner.get_dtype = profiler(_scanner_get_dtype)
    scanner.get_schema()
    profiler.print_stats()

    profiler = memory_profiler.LineProfiler()
    scanner.get_dtype = profiler(_scanner_get_dtype)
    scanner.get_schema()
    memory_profiler.show_results(profiler)


if __name__ == "__main__":
    main()
