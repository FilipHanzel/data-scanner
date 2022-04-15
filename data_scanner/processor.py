import os
import time
import multiprocessing as mp
import queue
from typing import List, Dict, Union
from pprint import pformat

from .loader import CSVLoader, JSONLoader
from .scanner import CSVScanner, JSONScanner
from .logger import logger, traceback_format


class Processor:
    """Main Data Scanner class.

    Processor is responsible for putting Data Scanner together.
    It's a main entry point to the package. Allows for sequential
    and parallel scan of multiple files.
    """

    def __init__(self, path: Union[str, os.PathLike], type_: str):
        assert type_ in ("csv", "json"), "Only json or csv files are supported"

        self.cores = mp.cpu_count()

        if type_ == "csv":
            self.loader = CSVLoader
            self.scanner = CSVScanner
        elif type_ == "json":
            self.loader = JSONLoader
            self.scanner = JSONScanner

        if os.path.isfile(path):
            self.file_list = [path]
        elif os.path.isdir(path):
            self.file_list = [
                file_path
                for file_path in [
                    path + os.sep + file_name for file_name in os.listdir(path)
                ]
                if os.path.isfile(file_path)
            ]
        else:
            self.file_list = []

        if not len(self.file_list) > 0:
            logger.error(f"No files found for path: '{path}'")

    def run_workers(self) -> List[Dict[str, str]]:
        """Scan multiple files in parallel.

        This method allows running multiple python processes to scan
        multiple files in parallel. It does not split one file between
        processes, so it won't improve performance for single file datasets.
        """
        input_queue = mp.Queue(maxsize=len(self.file_list) + self.cores)
        output_queue = mp.Queue(maxsize=len(self.file_list))
        error_queue = mp.Queue(maxsize=len(self.file_list))

        workers = [
            mp.Process(
                target=self._worker_target,
                args=(
                    input_queue,
                    output_queue,
                    error_queue,
                    self.loader,
                    self.scanner,
                ),
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
                    log = f"Process {process.name} exited with code {process.exitcode}"
                    if process.exitcode == 0:
                        logger.debug(log)
                    else:
                        logger.error(log)
                else:
                    _workers.append(process)
            workers = _workers

            # Empty output queue
            try:
                while True:
                    out = output_queue.get_nowait()
                    schemas.append(out)
            except queue.Empty as e:
                pass

            # Empty error_queue
            try:
                while True:
                    err = error_queue.get_nowait()
                    logger.error(err)
                    logger.debug("Exception traceback:\n" + traceback_format(err))
                    schemas.append({})
            except queue.Empty as e:
                pass

            # Wait
            time.sleep(0.5)

        return schemas

    @staticmethod
    def _worker_target(
        input_queue: mp.Queue,
        output_queue: mp.Queue,
        error_queue: mp.Queue,
        loader: Union[CSVLoader, JSONLoader],
        scanner: Union[CSVScanner, JSONScanner],
    ) -> None:
        while True:
            file_name = input_queue.get()
            if file_name is None:
                break
            try:
                with loader(file_name) as loader:
                    schema = scanner(loader).get_schema()
                output_queue.put(schema)
            except Exception as e:
                error_queue.put(e)

    def run(self) -> List[Dict[str, str]]:
        """Run sequential scan over a list of files.

        Scans a list of files one by one. Without the overhead
        os spawning multiple processes, it's better for smaller
        datasets.
        """
        schemas = []
        for file_name in self.file_list:
            try:
                with self.loader(file_name) as loader:
                    schema = self.scanner(loader).get_schema()
                schemas.append(schema)
            except Exception as e:
                logger.error(e)
                logger.debug("Exception traceback:\n" + traceback_format(e))
                schemas.append({})
        return schemas
