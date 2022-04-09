import re
from typing import List, Dict, Union

import ujson
import pendulum


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
