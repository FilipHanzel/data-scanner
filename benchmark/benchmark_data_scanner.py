import os
import sys
from timeit import default_timer as timer

data_scanner_path = os.path.join(os.path.dirname(__file__), "..")
sys.path.append(data_scanner_path)

from data_scanner import Processor


def main():
    script_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(script_path, "data")

    print("[INFO] Running data_scanner benchmark...")
    start = timer()
    processor = Processor(data_path)
    processor.run_workers()
    end = timer()

    print(
        f"[INFO] Data scanner multiprocess run took ~{round(end - start, 5)}s for {len(os.listdir(data_path))} files "
        f"({round((end - start) / len(os.listdir(data_path)), 5)}s per file)"
    )


if __name__ == "__main__":
    main()
