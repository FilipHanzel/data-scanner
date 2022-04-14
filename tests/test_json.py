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
