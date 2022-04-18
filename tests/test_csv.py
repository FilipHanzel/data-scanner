import os
import unittest

from data_scanner import Processor


class TestCSVScanner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script_path = os.path.dirname(os.path.abspath(__file__))
        cls.data_path = os.path.join(cls.script_path, "data", "csv")

    def test_empty_file(self):
        data_path = os.path.join(self.data_path, "empty_file")
        expected_schemas = [{}]

        print("[TEST] Running test_empty_file...")

        processor = Processor(data_path, "csv")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "csv")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)

    def test_header_only(self):
        data_path = os.path.join(self.data_path, "header_only.csv")
        expected_schemas = [
            {
                "this": "unknown",
                "file": "unknown",
                "has": "unknown",
                "only": "unknown",
                "columns": "unknown",
            }
        ]

        print("[TEST] Running test_header_only...")

        processor = Processor(data_path, "csv")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "csv")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)

    def test_malformed_columns(self):
        data_path = os.path.join(self.data_path, "malformed_columns.csv")
        expected_schemas = [{}]

        print("[TEST] Running test_malformed_columns...")

        processor = Processor(data_path, "csv")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "csv")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)

    def test_malformed_empty_lines(self):
        # First lines are empty, but exception is suppressed
        data_path = os.path.join(self.data_path, "malformed_empty_lines.csv")
        expected_schemas = [{}]

        print("[TEST] Running test_malformed_empty_lines...")

        processor = Processor(data_path, "csv")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "csv")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)

    def test_malformed_whitespace(self):
        # Whitespaces are not ignored, but it's safe to say, file shouldn't look like that
        data_path = os.path.join(self.data_path, "malformed_whitespace.csv")
        expected_schemas = [{"  column_1": "string", "column_2      ": "string"}]

        print("[TEST] Running test_malformed_whitespace...")

        processor = Processor(data_path, "csv")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "csv")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)

    def test_valid_file(self):
        data_path = os.path.join(self.data_path, "valid_file.csv")
        expected_schemas = [
            {
                "c_string": "string",
                "c_integer": "integer",
                "c_float": "float",
                "c_boolean": "boolean",
                "c_date": "date",
                "c_timestamp": "timestamp",
                "c_json": "json",
            }
        ]

        print("[TEST] Running test_valid_file...")

        processor = Processor(data_path, "csv")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "csv")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)
