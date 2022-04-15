import os
import sys
import abc
import csv
import ijson
from typing import Union, Iterable, Dict, BinaryIO


class Loader(abc.ABC):
    @abc.abstractmethod
    def open(self) -> Iterable:
        pass

    @abc.abstractmethod
    def close(self):
        pass

    def __enter__(self) -> Iterable:
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


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


class JSONReader:
    """Read and flatten json file iteratively.

    This class is meant to be created by JSONLoader.
    """

    def __init__(self, file: BinaryIO):
        self.type = self.peek_type(file)
        if self.type == "list":
            self.json_file = ijson.items(file, "item")
        else:
            self.json_file = ijson.items(file, "")

    @staticmethod
    def peek_type(json_file: BinaryIO) -> Union[str, None]:
        type_ = None

        while True:
            char = json_file.read(1)
            if not char:
                type_ = "empty"
                break
            if char == b"{":
                type_ = "object"
                break
            if char == b"[":
                type_ = "list"
                break
            if char.isspace():
                continue
            break

        json_file.seek(0)
        return type_

    @staticmethod
    def flatten(
        json: Dict, sep: str = "_", max_level: int = sys.getrecursionlimit()
    ) -> Dict:
        result = {}

        def _recurse(json: Dict, parent_key: str = "", level: int = 0):
            for key, value in json.items():
                new_key = parent_key + sep + key if parent_key else key
                if (level < max_level) and isinstance(value, dict):
                    _recurse(value, parent_key=new_key, level=level + 1)
                else:
                    result[new_key] = value

        _recurse(json)

        return result

    def __next__(self):
        return self.flatten(next(self.json_file))

    def __iter__(self):
        return self


class JSONLoader(Loader):
    def __init__(self, file_path: Union[str, os.PathLike]):
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"File not found: '{file_path}'")

        self.file_path = file_path

    def open(self) -> Iterable:
        self._file = open(self.file_path, "rb")
        self._reader = JSONReader(self._file)
        return self._reader

    def close(self):
        self._reader = None
        self._file.close()
        self._file = None
