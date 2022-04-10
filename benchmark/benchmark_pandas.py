import os
from timeit import default_timer as timer

import pandas as pd


def main():
    script_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_path, "data")

    file_names = [
        os.path.join(data_path, file_name) for file_name in os.listdir(data_path)
    ]

    print("[INFO] Running pandas benchmark...")
    start = timer()
    for file_name in file_names:
        pd.read_csv(file_name)
    end = timer()

    print(
        f"[INFO] Pandas read_csv without convert_dtypes run took ~{round(end - start, 5)}s for {len(file_names)} files "
        f"({round((end - start) / len(file_names), 5)}s per file)"
    )

    start = timer()
    for file_name in file_names:
        pd.read_csv(file_name).convert_dtypes()
    end = timer()

    print(
        f"[INFO] Pandas read_csv with convert_dtypes run took ~{round(end - start, 5)}s for {len(file_names)} files "
        f"({round((end - start) / len(file_names), 5)}s per file)"
    )


if __name__ == "__main__":
    main()
