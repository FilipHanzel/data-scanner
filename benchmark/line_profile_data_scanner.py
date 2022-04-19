import os
import sys
import argparse

import line_profiler
import memory_profiler

data_scanner_path = os.path.join(os.path.dirname(__file__), "..")
sys.path.append(data_scanner_path)

from data_scanner import Processor
from data_scanner.loader import CSVLoader, JSONLoader
from data_scanner.scanner import CSVScanner, JSONScanner


def main():
    parser = argparse.ArgumentParser(
        description="Benchmarks for data discovery with pandas."
    )
    parser.add_argument(
        "--type",
        "-t",
        action="store",
        default="all",
        choices=["all", "csv", "json"],
        dest="type",
    )
    args = parser.parse_args()

    if args.type == "all":
        types = ["csv", "json"]
    else:
        types = [args.type]

    script_path = os.path.dirname(os.path.abspath(__file__))

    for type_ in types:
        # Point processor to a single file, since line profiling
        # doesn't work well with multiprocessing
        data_path = os.path.join(script_path, "data", type_)
        file_path = os.path.join(data_path, os.listdir(data_path)[0])

        print(f"[INFO] Running line-profilers on Processor for {type_} (run)...")
        processor = Processor(file_path, type_)

        profiler = line_profiler.LineProfiler()
        profiled_function = profiler(processor.run)
        profiled_function()
        profiler.print_stats()

        profiler = memory_profiler.LineProfiler()
        profiled_function = profiler(processor.run)
        profiled_function()
        memory_profiler.show_results(profiler)

        if type_ == "csv":
            loaderClass = CSVLoader
            scannerClass = CSVScanner
        if type_ == "json":
            loaderClass = JSONLoader
            scannerClass = JSONScanner

        print(f"[INFO] Running line profiler on {type_.upper()}Scanner (_get_dtype)...")
        with loaderClass(file_path) as loader:
            scanner = scannerClass(loader)
            profiler = line_profiler.LineProfiler()
            scanner._get_dtype = profiler(scanner._get_dtype)
            scanner.get_schema()
            profiler.print_stats()

        print(
            f"[INFO] Running line profilers on {type_.upper()}Scanner (get_schema)..."
        )
        with loaderClass(file_path) as loader:
            scanner = scannerClass(loader)
            profiler = line_profiler.LineProfiler()
            scanner.get_schema = profiler(scanner.get_schema)
            scanner.get_schema()
            profiler.print_stats()


if __name__ == "__main__":
    main()
