import os
import unittest

from data_scanner import Processor


class TestJSONScannerRun(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script_path = os.path.dirname(os.path.abspath(__file__))
        cls.data_path = os.path.join(cls.script_path, "data", "json")

    def test_empty_file(self):
        processor = Processor(os.path.join(self.data_path, "empty_file"), "json")
        schemas = processor.run()
        self.assertEqual(schemas, [{}])

    def test_header_only(self):
        processor = Processor(
            os.path.join(self.data_path, "single_record_header_only.json"), "json"
        )
        schemas = processor.run()
        self.assertEqual(
            schemas,
            [
                {
                    "this": "unknown",
                    "file": "unknown",
                    "has": "unknown",
                    "only": "unknown",
                    "nulls": "unknown",
                }
            ],
        )

    def test_header_only_trailing_whitespace(self):
        processor = Processor(
            os.path.join(self.data_path, "single_record_header_only_trailing_whitespace.json"), "json"
        )
        schemas = processor.run()
        self.assertEqual(
            schemas,
            [
                {
                    "this": "unknown",
                    "file": "unknown",
                    "has": "unknown",
                    "only": "unknown",
                    "nulls": "unknown",
                }
            ],
        )

    def test_valid_json_list(self):
        processor = Processor(
            os.path.join(self.data_path, "valid_json_list.json"), "json"
        )
        schemas = processor.run()
        print(schemas)
        self.assertEqual(
            schemas,
            [
                {
                    "a": "string",
                    "c": "unknown", 
                    "c_f": "json",
                    "c_g": "string",
                    "d": "json",
                    "e": "json",
                    "f": "integer",
                    "g": "integer",
                    "h": "float",
                }
            ],
        )

    def test_valid_json_single_record(self):
        processor = Processor(
            os.path.join(self.data_path, "valid_json_single_record.json"), "json"
        )
        schemas = processor.run()
        self.assertEqual(
            schemas,
            [
                {
                    "a": "string",
                    "c_f": "json",
                    "c_g": "string",
                    "d": "unknown",
                    "f": "integer",
                    "g": "integer",
                    "h": "float",
                }
            ],
        )
