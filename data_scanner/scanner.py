import re
from typing import Dict, Union, Iterable
from enum import Enum

import ujson
import pendulum


class Scanner:
    def __init__(self, frame: Iterable):
        self.frame = frame
        self._nulls = ["", "NULL", "Null", "null", "None", "none", "NA", "N/A"]
        self._booleans = [
            "True",
            "False",
            "true",
            "false",
            "t",
            "f",
            "T",
            "F",
            "1",
            "0",
        ]

        self._re_pattern_float = re.compile(
            r"[+-]?((\d+\.\d*)|(\.\d+)|(\d+))([eE][+-]?\d+)?$"
        )
        self._re_pattern_int = re.compile(r"(\d+)(\.0*)?$")

    def _is_null(self, value: str) -> bool:
        return value in self._nulls

    def _is_float(self, value: str) -> bool:
        return self._re_pattern_float.match(value)

    def _is_integer(self, value: str) -> bool:
        return self._re_pattern_int.match(value)

    def _is_boolean(self, value: str) -> bool:
        return value in self._booleans

    @staticmethod
    def _is_date_or_timestamp(value: str) -> Union[bool, str]:
        try:
            parsed = pendulum.parse(value)
            if (
                parsed.hour != 0
                or parsed.minute != 0
                or parsed.second != 0
                or parsed.microsecond != 0
            ):
                return "timestamp"
            return "date"
        except:
            return False

    @staticmethod
    def _is_json(value: str) -> bool:
        try:
            ujson.loads(value)
        except:
            return False
        return True

    def _get_dtype(self, value: str, dtype: str = "unknown") -> str:

        # Get type of first non-null value
        if dtype == "unknown":
            if self._is_null(value):
                return "unknown"
            if self._is_float(value):
                if self._is_integer(value):
                    return "integer"
                return "float"
            date_type = self._is_date_or_timestamp(value)
            if date_type:
                return date_type
            if self._is_json(value):
                return "json"
            # If defining values parsed as booleans
            # is allowed, it has to be checked last
            if self._is_boolean(value):
                return "boolean"
            return "string"

        # Based on first non-null value, either stick
        # with the type or change it to more generic
        # avoiding checks for impossible types

        # If value is null, then keep the dtype
        if self._is_null(value):
            return dtype

        if dtype == "integer":
            if self._is_integer(value):
                return "integer"
            dtype = "float"

        if dtype == "float":
            if self._is_float(value):
                return "float"
            dtype = "boolean"

        if dtype in ("date", "timestamp"):
            date_type = self._is_date_or_timestamp(value)
            if date_type:
                return date_type
            dtype = "boolean"

        if dtype == "json":
            if self._is_json(value):
                return "json"
            dtype = "boolean"

        if dtype == "boolean":
            if self._is_boolean(value):
                return "boolean"

        return "string"

    def get_schema(self) -> Dict:
        head = next(self.frame)
        types = ["unknown"] * len(head)

        for row in self.frame:
            if len(row) != len(head):
                raise ValueError("malformed csv, invalid row length")

            for idx, value in enumerate(row):
                types[idx] = self._get_dtype(value, types[idx])

        return dict(zip(head, types))
