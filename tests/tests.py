import os
import unittest

from data_scanner import Processor


class TestScannerRun(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script_path = os.path.dirname(os.path.abspath(__file__))
        cls.data_path = os.path.join(cls.script_path, "data")

    def test_empty_file(self):
        processor = Processor(os.path.join(self.data_path, "empty_file"))
        schemas = processor.run_workers()
        self.assertEqual(schemas, [{}])

    def test_header_only(self):
        processor = Processor(os.path.join(self.data_path, "header_only.csv"))
        schemas = processor.run()
        self.assertEqual(
            schemas,
            [
                {
                    "this": "unknown",
                    "file": "unknown",
                    "has": "unknown",
                    "only": "unknown",
                    "columns": "unknown",
                }
            ],
        )

    def test_malformed_columns(self):
        processor = Processor(os.path.join(self.data_path, "malformed_columns.csv"))
        schemas = processor.run()
        self.assertEqual(schemas, [{}])

    def test_malformed_empty_lines(self):
        # First lines are empty, but exception is suppressed
        processor = Processor(os.path.join(self.data_path, "malformed_empty_lines.csv"))
        schemas = processor.run()
        self.assertEqual(schemas, [{}])

    def test_malformed_whitespace(self):
        # Whitespaces are not ignored, but it's safe to say, file shouldn't look like that
        processor = Processor(os.path.join(self.data_path, "malformed_whitespace.csv"))
        schemas = processor.run()
        self.assertEqual(
            schemas, [{"  column_1": "string", "column_2      ": "string"}]
        )

    def test_valid_file(self):
        processor = Processor(os.path.join(self.data_path, "valid_file.csv"))
        schemas = processor.run()
        self.assertEqual(
            schemas,
            [
                {
                    "c_string": "string",
                    "c_integer": "integer",
                    "c_float": "float",
                    "c_boolean": "boolean",
                    "c_date": "date",
                    "c_timestamp": "timestamp",
                    "c_json": "json",
                }
            ],
        )


class TestScannerRunWorkers(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script_path = os.path.dirname(os.path.abspath(__file__))
        cls.data_path = os.path.join(cls.script_path, "data")

    def test_empty_file(self):
        processor = Processor(os.path.join(self.data_path, "empty_file"))
        schemas = processor.run_workers()
        self.assertEqual(schemas, [{}])

    def test_header_only(self):
        processor = Processor(os.path.join(self.data_path, "header_only.csv"))
        schemas = processor.run_workers()
        self.assertEqual(
            schemas,
            [
                {
                    "this": "unknown",
                    "file": "unknown",
                    "has": "unknown",
                    "only": "unknown",
                    "columns": "unknown",
                }
            ],
        )

    def test_malformed_columns(self):
        processor = Processor(os.path.join(self.data_path, "malformed_columns.csv"))
        schemas = processor.run_workers()
        self.assertEqual(schemas, [{}])

    def test_malformed_empty_lines(self):
        # First lines are empty, but exception is suppressed
        processor = Processor(os.path.join(self.data_path, "malformed_empty_lines.csv"))
        schemas = processor.run_workers()
        self.assertEqual(schemas, [{}])

    def test_malformed_whitespace(self):
        # Whitespaces are not ignored, but it's safe to say, file shouldn't look like that
        processor = Processor(os.path.join(self.data_path, "malformed_whitespace.csv"))
        schemas = processor.run_workers()
        self.assertEqual(
            schemas, [{"  column_1": "string", "column_2      ": "string"}]
        )

    def test_valid_file(self):
        processor = Processor(os.path.join(self.data_path, "valid_file.csv"))
        schemas = processor.run_workers()
        self.assertEqual(
            schemas,
            [
                {
                    "c_string": "string",
                    "c_integer": "integer",
                    "c_float": "float",
                    "c_boolean": "boolean",
                    "c_date": "date",
                    "c_timestamp": "timestamp",
                    "c_json": "json",
                }
            ],
        )
