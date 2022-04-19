import os
import json
import argparse
from timeit import default_timer as timer

import pandas as pd


def pandas_csv_benchmark(file_names):
    print("[INFO] Running pandas csv benchmark...")

    start = timer()
    for file_name in file_names:
        pd.read_csv(file_name)
    end = timer()

    print(
        f"[INFO] Pandas read_csv without convert_dtypes run took ~{round(end - start, 5)}s "
        f"for {len(file_names)} files ({round((end - start) / len(file_names), 5)}s per file)"
    )

    start = timer()
    for file_name in file_names:
        pd.read_csv(file_name).convert_dtypes()
    end = timer()

    print(
        f"[INFO] Pandas read_csv with convert_dtypes run took ~{round(end - start, 5)}s "
        f"for {len(file_names)} files ({round((end - start) / len(file_names), 5)}s per file)"
    )


def pandas_json_benchmark(file_names):
    print("[INFO] Running pandas json benchmark...")

    start = timer()
    for file_name in file_names:
        with open(file_name, "rt") as f:
            pd.json_normalize(json.load(f))
    end = timer()

    print(
        f"[INFO] Pandas json_normalize/json.load without convert_dtypes run took ~{round(end - start, 5)}s "
        f"for {len(file_names)} files ({round((end - start) / len(file_names), 5)}s per file)"
    )

    start = timer()
    for file_name in file_names:
        with open(file_name, "rt") as f:
            pd.json_normalize(json.load(f)).convert_dtypes()
    end = timer()

    print(
        f"[INFO] Pandas json_normalize/json.load with convert_dtypes run took ~{round(end - start, 5)}s "
        f"for {len(file_names)} files ({round((end - start) / len(file_names), 5)}s per file)"
    )


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
        data_path = os.path.join(script_path, "data", type_)

        file_names = [
            os.path.join(data_path, file_name) for file_name in os.listdir(data_path)
        ]

        if type_ == "csv":
            pandas_csv_benchmark(file_names)
        if type_ == "json":
            pandas_json_benchmark(file_names)


if __name__ == "__main__":
    main()
