import os
import sys
import logging
import argparse
from timeit import default_timer as timer
from pprint import pprint

data_scanner_path = os.path.join(os.path.dirname(__file__), "..")
sys.path.append(data_scanner_path)

from data_scanner import Processor
from data_scanner import setLoggingLevel


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

    setLoggingLevel(logging.DEBUG)

    script_path = os.path.dirname(os.path.abspath(__file__))

    for type_ in types:
        data_path = os.path.join(script_path, "data", type_)

        print(f"[INFO] Running data_scanner {type_} benchmark...")

        start = timer()
        processor = Processor(data_path, type_)
        output = processor.run_workers()
        end = timer()

        print(
            f"[INFO] Data scanner {type_} multiprocess run took ~{round(end - start, 5)}s "
            f"for {len(os.listdir(data_path))} files ({round((end - start) / len(os.listdir(data_path)), 5)}s per file)"
        )


if __name__ == "__main__":
    main()
