import os
import abc
import csv
from typing import List, Dict, Union


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
