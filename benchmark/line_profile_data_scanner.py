import os
import sys

import line_profiler
import memory_profiler

data_scanner_path = os.path.join(os.path.dirname(__file__), "..")
sys.path.append(data_scanner_path)

from data_scanner import Processor
from data_scanner.loader import CSVLoader
from data_scanner.scanner import CSVScanner


def main():
    # Point processor to a single file, since line profiling
    # doesn't work well with multiprocessing
    script_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_path, "data", "csv")
    file_path = os.path.join(data_path, os.listdir(data_path)[0])

    print("[INFO] Running line-profilers on Processor (run)...")
    processor = Processor(file_path, "csv")

    profiler = line_profiler.LineProfiler()
    profiled_function = profiler(processor.run)
    profiled_function()
    profiler.print_stats()

    profiler = memory_profiler.LineProfiler()
    profiled_function = profiler(processor.run)
    profiled_function()
    memory_profiler.show_results(profiler)

    print("[INFO] Running line profiler on scanner (_get_dtype)...")
    with CSVLoader(file_path) as loader:
        scanner = CSVScanner(loader)
        profiler = line_profiler.LineProfiler()
        scanner._get_dtype = profiler(scanner._get_dtype)
        scanner.get_schema()
        profiler.print_stats()

    print("[INFO] Running line profilers on scanner (get_schema)...")
    with CSVLoader(file_path) as loader:
        scanner = CSVScanner(loader)
        profiler = line_profiler.LineProfiler()
        scanner.get_schema = profiler(scanner.get_schema)
        scanner.get_schema()
        profiler.print_stats()


if __name__ == "__main__":
    main()
