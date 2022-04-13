import os
import time
import multiprocessing as mp
import queue
from typing import List, Dict, Union
from pprint import pformat

from .loader import CSVLoader
from .scanner import Scanner
from .logger import logger


class Processor:
    """Main class. Puts everyting together and implements multiprocessing."""

    def __init__(self, path: Union[str, os.PathLike]):
        assert path is not None, "path must be specified"

        self.cores = mp.cpu_count()

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
                    schemas.append({})
            except queue.Empty as e:
                pass

            # Wait
            time.sleep(0.5)

        return schemas

    @staticmethod
    def _worker_target(
        input_queue: mp.Queue, output_queue: mp.Queue, error_queue: mp.Queue
    ) -> None:
        while True:
            file_name = input_queue.get()
            if file_name is None:
                break
            try:
                with CSVLoader(file_name) as loader:
                    schema = Scanner(loader).get_schema()
                output_queue.put(schema)
            except Exception as e:
                error_queue.put(e)

    def run(self) -> List[Dict[str, str]]:
        schemas = []
        for file_name in self.file_list:
            try:
                with CSVLoader(file_name) as loader:
                    schema = Scanner(loader).get_schema()
                schemas.append(schema)
            except Exception as e:
                logger.error(e)
                schemas.append({})
        return schemas
