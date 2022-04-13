import os
import abc
import csv
from typing import Union, Iterable


class Loader(abc.ABC):
    @abc.abstractmethod
    def __enter__(self):
        pass

    @abc.abstractmethod
    def __exit__(self):
        pass


class CSVLoader(Loader):
    def __init__(self, file_path: Union[str, os.PathLike]):
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: '{file_path}'")

        self.file_path = file_path

        self._file = None
        self._reader = None

    def open(self) -> Iterable:
        self._file = open(self.file_path, "rt")
        self._reader = csv.reader(self._file)
        return self._reader

    def close(self):
        self._reader = None
        self._file.close()
        self._file = None

    def __enter__(self) -> Iterable:
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
