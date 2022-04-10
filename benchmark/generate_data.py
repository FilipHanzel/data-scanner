"""Generate fake data for benchmarking"""

import os
import csv
import json

from faker import Faker


def main():
    script_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_path, "data")

    if not os.path.exists(data_path):
        os.makedirs(data_path)

    files_count = 15
    # To speed up generation, fake one batch and write it multiple times
    rows_count = 1_000_000
    rows_batch = 1_000
    n_batches = rows_count // rows_batch

    fake = Faker()

    for idx in range(files_count):

        file_name = f"data_file_{idx:02}.csv"
        print(f"[INFO] Faking file {file_name}...")

        with open(os.path.join(data_path, file_name), "w+", newline="") as f:
            writer = csv.writer(f)

            writer.writerow(
                [
                    "c_integer",
                    "c_float",
                    "c_boolean",
                    "c_string",
                    "c_date",
                    "c_timestamp",
                    "c_json",
                ]
            )

            batch = [
                [
                    fake.pyint(min_value=-10000, max_value=10000),
                    fake.pyfloat(min_value=-10000, max_value=10000),
                    fake.boolean(),
                    fake.pystr(min_chars=0, max_chars=25),
                    fake.date(),
                    fake.date_time(),
                    json.dumps(fake.pydict(), default=str),
                ]
                for _ in range(rows_batch)
            ]

            for _ in range(n_batches):
                writer.writerows(batch)

    print("[INFO] CSV files faked")


if __name__ == "__main__":
    main()
