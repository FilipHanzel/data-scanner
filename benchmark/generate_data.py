import os
import csv
import json
import argparse

from faker import Faker


def fake_csv_file(file_name, dest_path, batches, rows_per_batch):
    fake = Faker()
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
        for _ in range(rows_per_batch)
    ]

    with open(os.path.join(dest_path, file_name), "w+", newline="") as f:
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

        for _ in range(batches):
            writer.writerows(batch)


def fake_json_file(file_name, dest_path, batches, rows_per_batch):
    fake = Faker()
    batch = ",\n".join(
        [
            json.dumps(
                dict(
                    c_int=fake.pyint(min_value=-10000, max_value=10000),
                    c_float=fake.pyfloat(min_value=-10000, max_value=10000),
                    c_boolean=fake.boolean(),
                    c_str=fake.pystr(min_chars=0, max_chars=25),
                    c_date=fake.date(),
                    c_timestamp=fake.date_time().strftime(r"%Y-%m-%d %H:%M:%S"),
                    c_nested=dict(
                        list=[1, 2, 3],
                        string=fake.pystr(min_chars=0, max_chars=15),
                        float=fake.pyfloat(min_value=-10000, max_value=10000),
                        boolean=fake.boolean(),
                        deeply_nested=dict(string=fake.pystr(min_chars=0, max_chars=5)),
                    ),
                )
            )
            for _ in range(rows_per_batch)
        ]
    )

    with open(os.path.join(dest_path, file_name), "w+") as f:
        f.write("[")
        for _ in range(batches - 1):
            f.write(batch)
            f.write(",\n")
        f.write(batch)
        f.write("]")


def main():
    parser = argparse.ArgumentParser(description="Generate fake data for benchmarking")
    parser.add_argument(
        "--files", "-f", action="store", type=int, default=15, dest="files"
    )
    parser.add_argument(
        "--rows", "-r", action="store", type=int, default=1_000_000, dest="rows"
    )
    parser.add_argument(
        "--rows_per_batch",
        "-b",
        action="store",
        type=int,
        default=1_000,
        dest="rows_per_batch",
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

    # To speed up generation, fake one batch and write it multiple times
    # If rows_per_batch > rows_count, set rows_per_batch to rows_count
    rows_count = args.rows
    rows_per_batch = min(args.rows_per_batch, rows_count)
    n_batches = rows_count // rows_per_batch
    files_count = args.files
    if args.type == "all":
        types = ["csv", "json"]
    else:
        types = [args.type]

    script_path = os.path.dirname(os.path.abspath(__file__))

    for type_ in types:
        data_path = os.path.join(script_path, "data", type_)

        if not os.path.exists(data_path):
            os.makedirs(data_path)

        for idx in range(files_count):
            file_name = f"data_file_{idx:02}.{type_}"
            print(f"[INFO] Faking file {file_name}...")

            if type_ == "csv":
                fake_csv_file(file_name, data_path, n_batches, rows_per_batch)
            if type_ == "json":
                fake_json_file(file_name, data_path, n_batches, rows_per_batch)

        print(f"[INFO] {type_} files faked")


if __name__ == "__main__":
    main()
