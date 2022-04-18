import re
from decimal import Decimal  # ijson uses decimal
from typing import Dict, Union, Iterable, Any
import abc

import ujson
import pendulum


class Scanner(abc.ABC):
    def __init__(self, frame: Iterable):
        self.frame = frame

    @abc.abstractmethod
    def _is_null(self, value):
        pass

    @abc.abstractmethod
    def _is_float(self, value):
        pass

    @abc.abstractmethod
    def _is_integer(self, value):
        pass

    @abc.abstractmethod
    def _is_boolean(self, value):
        pass

    @abc.abstractmethod
    def _is_date_or_timestamp(self, value):
        pass

    @abc.abstractmethod
    def _is_json(self, value):
        pass

    @abc.abstractmethod
    def _get_dtype(self, value):
        """Defines how _is_* methods work with each other."""
        pass

    @abc.abstractmethod
    def get_schema(self) -> Dict:
        """Entry point for scanner. Uses _get_dtype to scan an iterable (self.frame)."""
        pass


class CSVScanner(Scanner):
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

        # If dtype is a string or value is null, then keep the dtype
        if dtype == "string" or self._is_null(value):
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
        try:
            head = next(self.frame)
        except:
            raise ValueError("Failed to read header, empty file")

        types = ["unknown"] * len(head)

        for row in self.frame:
            if len(row) != len(head):
                raise ValueError("Malformed data, invalid row length")

            for idx, value in enumerate(row):
                types[idx] = self._get_dtype(value, types[idx])

        return dict(zip(head, types))


class JSONScanner(Scanner):
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

    def _is_null(self, value: Any) -> bool:
        return value is None or value in self._nulls

    @staticmethod
    def _is_float(value: Any) -> bool:
        return (
            isinstance(value, Decimal)
            or isinstance(value, float)
            or isinstance(value, int)
        )

    @staticmethod
    def _is_integer(value: Any) -> bool:
        if isinstance(value, int):
            return True
        if isinstance(value, Decimal) or isinstance(value, float):
            return value % 1 == 0
        return False

    def _is_boolean(self, value: Any) -> bool:
        return isinstance(value, bool) or value in self._booleans

    @staticmethod
    def _is_date_or_timestamp(value: Any) -> Union[bool, str]:
        if not isinstance(value, str):
            return False
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
    def _is_json(value: Any) -> bool:
        return isinstance(value, list) or isinstance(value, dict)

    def _get_dtype(self, value: Any, dtype: str = "unknown") -> str:

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

        # If dtype is a string or value is null, then keep the dtype
        if dtype == "string":
            return "string"

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

        types = {}

        for row in self.frame:
            for column_name, value in row.items():
                types[column_name] = self._get_dtype(
                    value, types[column_name] if column_name in types else "unknown"
                )

        return types
