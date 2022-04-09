import os
import re
import csv
import abc
import time
from typing import List, Dict, Union
import multiprocessing as mp
import queue

import ujson
import pendulum

from pprint import pprint


# TODO: Restructure file into package
# TODO: Write unittests
# TODO: Write "load" tests (with simple data generator script for bigger datasets)
# TODO: Use enum to store all available types


class Processor:
    """Main class. Puts everyting together and implements multiprocessing."""

    def __init__(self, root_folder):
        self.cores = mp.cpu_count()
        self.file_list = [
            file_path
            for file_path in [
                root_folder + os.sep + file_name
                for file_name in os.listdir(root_folder)
            ]
            if os.path.isfile(file_path)
        ]

    def run_workers(self):
        """Allows for utilizing"""
        input_queue = mp.Queue(maxsize=len(self.file_list) + self.cores)
        output_queue = mp.Queue(maxsize=len(self.file_list))
        error_queue = mp.Queue(maxsize=len(self.file_list))

        workers = [
            mp.Process(
                target=self._worker_target,
                args=(input_queue, output_queue, error_queue),
            )
            for _ in range(self.cores)
        ]

        # Queue all the tasks
        for file_name in self.file_list:
            input_queue.put(file_name)

        # Queue end messages
        for _ in range(self.cores):
            input_queue.put(None)

        # Run workers
        for process in workers:
            process.start()

        # Wait for results
        schemas = []
        while True:
            if not workers:
                break

            # Health check
            _workers = []
            for process in workers:
                if not process.is_alive():
                    print(
                        f"[INFO] Process {process.name} exited with code {process.exitcode}"
                    )
                else:
                    _workers.append(process)
            workers = _workers

            # Empty output queue
            try:
                while True:
                    out = output_queue.get_nowait()
                    print("[WORKER OUTPUT]", out)
                    schemas.append(out)
            except queue.Empty as e:
                pass

            # Empty error_queue
            try:
                while True:
                    err = error_queue.get_nowait()
                    print("[WORKER ERROR]", err)
            except queue.Empty as e:
                pass

            # Wait
            time.sleep(0.5)

        return schemas

    @staticmethod
    def _worker_target(input_queue, output_queue, error_queue):
        while True:
            file_name = input_queue.get()
            if file_name is None:
                break
            try:
                frame = CSVLoader().load(file_name)
                schema = Scanner(frame).get_schema()
                output_queue.put(schema)
            except Exception as e:
                error_queue.put(e)

    def run(self):
        schemas = []
        for file_name in self.file_list:
            try:
                frame = CSVLoader().load(file_name)
                schema = Scanner(frame).get_schema()
                schemas.append(schema)
            except Exception as e:
                print("[RUN]", e)
                schemas.append({})
        return schemas


class Loader(abc.ABC):
    @abc.abstractmethod
    def load(self, file_path: Union[str, os.PathLike]) -> Dict[str, List[str]]:
        pass


class CSVLoader(Loader):
    def load(self, file_path: Union[str, os.PathLike]) -> Dict[str, List[str]]:
        with open(file_path, "rt") as f:
            csv_reader = csv.reader(f)
            try:
                self.head = next(csv_reader)
            except StopIteration:
                self.head = []
                self.records = []
                return {}
            self.records = list(csv_reader)

        # Assert rows length
        for row in self.records:
            if len(row) != len(self.head):
                raise ValueError("malformed csv, invalid row length")

        return {
            name: [record[idx] for record in self.records]
            for idx, name in enumerate(self.head)
        }


class Scanner:
    def __init__(self, frame: Dict[str, List[str]]):
        self.frame = frame
        self.nulls = ["", "NULL", "Null", "null", "None", "NA", "N/A"]
        self.booleans = ["True", "False", "true", "false", "t", "f", "T", "F", "1", "0"]

    def _reduce_nulls(self, column: List[str]) -> List[str]:
        return [
            value for value in column if value is not None and value not in self.nulls
        ]

    @staticmethod
    def _is_float(column: List[str]) -> bool:
        pattern = re.compile(r"[+-]?((\d+\.\d*)|(\.\d+)|(\d+))([eE][+-]?\d+)?$")
        for value in column:
            if not pattern.match(value):
                return False
        return True

    @staticmethod
    def _is_integer(column: List[str]) -> bool:
        pattern = re.compile(r"(\d+)(\.0*)?$")
        for value in column:
            if not pattern.match(value):
                return False
        return True

    def _is_boolean(self, column: List[str]) -> bool:
        for value in column:
            if not value in self.booleans:
                return False
        return True

    @staticmethod
    def _is_date_or_timestamp(column: List[str]) -> Union[bool, str]:
        try:
            parsed = [pendulum.parse(value) for value in column]
            for timestamp in parsed:
                if (
                    timestamp.hour != 0
                    or timestamp.minute != 0
                    or timestamp.second != 0
                    or timestamp.microsecond != 0
                ):
                    return "timestamp"
            return "date"
        except:
            return False

    @staticmethod
    def _is_json(column):
        try:
            for value in column:
                if not value.startswith("{") and not value.startswith("["):
                    return False
                ujson.loads(value)
        except:
            return False
        return True

    def get_dtype(self, column):
        column = self._reduce_nulls(column)
        if not column:
            return "unknown"
        if self._is_float(column):
            if self._is_integer(column):
                return "integer"
            return "float"
        if self._is_boolean(column):
            return "boolean"
        date_type = self._is_date_or_timestamp(column)
        if date_type:
            return date_type
        if self._is_json(column):
            return "json"
        return "string"

    def get_schema(self):
        return {name: self.get_dtype(column) for name, column in self.frame.items()}


if __name__ == "__main__":
    # Time comparisson between parallel and single-process execution
    from timeit import default_timer as timer

    print("[TEST] Running timeit test...")
    processor = Processor("data")

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

    import line_profiler
    import memory_profiler

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
