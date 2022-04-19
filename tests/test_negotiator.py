import os
import unittest

from data_scanner.negotiator import Negotiator


class TestNegotiator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script_path = os.path.dirname(os.path.abspath(__file__))
        cls.data_path = os.path.join(cls.script_path, "data", "csv")

    def test_negotiator(self):
        schemas = [
            {},
            {
                "unknown_01": "unknown",
                "unknown_02": "unknown",
                "unknown_03": "unknown",
                "unknown_04": "unknown",
                "unknown_05": "unknown",
                "unknown_06": "unknown",
                "unknown_07": "unknown",
                "unknown_08": "unknown",
                "integer_01": "integer",
                "integer_02": "integer",
                "integer_03": "integer",
                "integer_04": "integer",
                "integer_05": "integer",
                "integer_06": "integer",
                "integer_07": "integer",
                "integer_08": "integer",
                "float_01": "float",
                "float_02": "float",
                "float_03": "float",
                "float_04": "float",
                "float_05": "float",
                "float_06": "float",
                "float_07": "float",
                "float_08": "float",
                "string_01": "string",
                "string_02": "string",
                "string_03": "string",
                "string_04": "string",
                "string_05": "string",
                "string_06": "string",
                "string_07": "string",
                "string_08": "string",
                "boolean_01": "boolean",
                "boolean_02": "boolean",
                "boolean_03": "boolean",
                "boolean_04": "boolean",
                "boolean_05": "boolean",
                "boolean_06": "boolean",
                "boolean_07": "boolean",
                "boolean_08": "boolean",
                "json_01": "json",
                "json_02": "json",
                "json_03": "json",
                "json_04": "json",
                "json_05": "json",
                "json_06": "json",
                "json_07": "json",
                "json_08": "json",
                "date_01": "date",
                "date_02": "date",
                "date_03": "date",
                "date_04": "date",
                "date_05": "date",
                "date_06": "date",
                "date_07": "date",
                "date_08": "date",
                "timestamp_01": "timestamp",
                "timestamp_02": "timestamp",
                "timestamp_03": "timestamp",
                "timestamp_04": "timestamp",
                "timestamp_05": "timestamp",
                "timestamp_06": "timestamp",
                "timestamp_07": "timestamp",
                "timestamp_08": "timestamp",
            },
            {
                "unknown_01": "unknown",
                "unknown_02": "integer",
                "unknown_03": "float",
                "unknown_04": "string",
                "unknown_05": "boolean",
                "unknown_06": "json",
                "unknown_07": "date",
                "unknown_08": "timestamp",
                "integer_01": "unknown",
                "integer_02": "integer",
                "integer_03": "float",
                "integer_04": "string",
                "integer_05": "boolean",
                "integer_06": "json",
                "integer_07": "date",
                "integer_08": "timestamp",
                "float_01": "unknown",
                "float_02": "integer",
                "float_03": "float",
                "float_04": "string",
                "float_05": "boolean",
                "float_06": "json",
                "float_07": "date",
                "float_08": "timestamp",
                "string_01": "unknown",
                "string_02": "integer",
                "string_03": "float",
                "string_04": "string",
                "string_05": "boolean",
                "string_06": "json",
                "string_07": "date",
                "string_08": "timestamp",
                "boolean_01": "unknown",
                "boolean_02": "integer",
                "boolean_03": "float",
                "boolean_04": "string",
                "boolean_05": "boolean",
                "boolean_06": "json",
                "boolean_07": "date",
                "boolean_08": "timestamp",
                "json_01": "unknown",
                "json_02": "integer",
                "json_03": "float",
                "json_04": "string",
                "json_05": "boolean",
                "json_06": "json",
                "json_07": "date",
                "json_08": "timestamp",
                "date_01": "unknown",
                "date_02": "integer",
                "date_03": "float",
                "date_04": "string",
                "date_05": "boolean",
                "date_06": "json",
                "date_07": "date",
                "date_08": "timestamp",
                "timestamp_01": "unknown",
                "timestamp_02": "integer",
                "timestamp_03": "float",
                "timestamp_04": "string",
                "timestamp_05": "boolean",
                "timestamp_06": "json",
                "timestamp_07": "date",
                "timestamp_08": "timestamp",
            },
        ]

        expected = {
            "unknown_01": "unknown",  # unknown + unknown
            "unknown_02": "integer",  # unknown + integer
            "unknown_03": "float",  # unknown + float
            "unknown_04": "string",  # unknown + string
            "unknown_05": "boolean",  # unknown + boolean
            "unknown_06": "json",  # unknown + json
            "unknown_07": "date",  # unknown + date
            "unknown_08": "timestamp",  # unknown + timestamp
            "integer_01": "integer",  # integer + unknown
            "integer_02": "integer",  # integer + integer
            "integer_03": "float",  # integer + float
            "integer_04": "string",  # integer + string
            "integer_05": "string",  # integer + boolean
            "integer_06": "string",  # integer + json
            "integer_07": "string",  # integer + date
            "integer_08": "string",  # integer + timestamp
            "float_01": "float",  # float + unknown
            "float_02": "float",  # float + integer
            "float_03": "float",  # float + float
            "float_04": "string",  # float + string
            "float_05": "string",  # float + boolean
            "float_06": "string",  # float + json
            "float_07": "string",  # float + date
            "float_08": "string",  # float + timestamp
            "string_01": "string",  # string + unknown
            "string_02": "string",  # string + integer
            "string_03": "string",  # string + float
            "string_04": "string",  # string + string
            "string_05": "string",  # string + boolean
            "string_06": "string",  # string + json
            "string_07": "string",  # string + date
            "string_08": "string",  # string + timestamp
            "boolean_01": "boolean",  # boolean + unknown
            "boolean_02": "string",  # boolean + integer
            "boolean_03": "string",  # boolean + float
            "boolean_04": "string",  # boolean + string
            "boolean_05": "boolean",  # boolean + boolean
            "boolean_06": "string",  # boolean + json
            "boolean_07": "string",  # boolean + date
            "boolean_08": "string",  # boolean + timestamp
            "json_01": "json",  # json + unknown
            "json_02": "string",  # json + integer
            "json_03": "string",  # json + float
            "json_04": "string",  # json + string
            "json_05": "string",  # json + boolean
            "json_06": "json",  # json + json
            "json_07": "string",  # json + date
            "json_08": "string",  # json + timestamp
            "date_01": "date",  # date + unknown
            "date_02": "string",  # date + integer
            "date_03": "string",  # date + float
            "date_04": "string",  # date + string
            "date_05": "string",  # date + boolean
            "date_06": "string",  # date + json
            "date_07": "date",  # date + date
            "date_08": "timestamp",  # date + timestamp
            "timestamp_01": "timestamp",  # timestamp + unknown
            "timestamp_02": "string",  # timestamp + integer
            "timestamp_03": "string",  # timestamp + float
            "timestamp_04": "string",  # timestamp + string
            "timestamp_05": "string",  # timestamp + boolean
            "timestamp_06": "string",  # timestamp + json
            "timestamp_07": "timestamp",  # timestamp + date
            "timestamp_08": "timestamp",  # timestamp + timestamp
        }

        result = Negotiator.negotiate(schemas)

        self.assertEqual(result, expected)
