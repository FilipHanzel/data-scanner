import os
import unittest

from data_scanner import Processor


class TestJSONProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.script_path = os.path.dirname(os.path.abspath(__file__))
        cls.data_path = os.path.join(cls.script_path, "data", "json")

    def test_empty_file(self):
        data_path = os.path.join(self.data_path, "empty_file")
        expected_schemas = [{}]

        print("[TEST] Running test_empty_file...")

        processor = Processor(data_path, "json")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "json", negotiate_schema=True)
        schema = processor.run()
        self.assertEqual(schema, *expected_schemas)

        processor = Processor(data_path, "json")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)

    def test_header_only(self):
        data_path = os.path.join(self.data_path, "single_record_header_only.json")
        expected_schemas = [
            {
                "this": "unknown",
                "file": "unknown",
                "has": "unknown",
                "only": "unknown",
                "nulls": "unknown",
            }
        ]

        print("[TEST] Running test_header_only...")

        processor = Processor(data_path, "json")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "json", negotiate_schema=True)
        schema = processor.run()
        self.assertEqual(schema, *expected_schemas)

        processor = Processor(data_path, "json")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)

    def test_header_only_trailing_whitespace(self):
        data_path = os.path.join(
            self.data_path, "single_record_header_only_trailing_whitespace.json"
        )
        expected_schemas = [
            {
                "this": "unknown",
                "file": "unknown",
                "has": "unknown",
                "only": "unknown",
                "nulls": "unknown",
            }
        ]

        print("[TEST] Running test_header_only_trailing_whitespace...")

        processor = Processor(data_path, "json")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "json", negotiate_schema=True)
        schema = processor.run()
        self.assertEqual(schema, *expected_schemas)

        processor = Processor(data_path, "json")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)

    def test_valid_json_list(self):
        data_path = os.path.join(self.data_path, "valid_json_list.json")
        expected_schemas = [
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
        ]

        print("[TEST] Running test_valid_json_list...")

        processor = Processor(data_path, "json")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "json", negotiate_schema=True)
        schema = processor.run()
        self.assertEqual(schema, *expected_schemas)

        processor = Processor(data_path, "json")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)

    def test_valid_json_single_record(self):
        data_path = os.path.join(self.data_path, "valid_json_single_record.json")
        expected_schemas = [
            {
                "a": "string",
                "c_f": "json",
                "c_g": "string",
                "d": "unknown",
                "f": "integer",
                "g": "integer",
                "h": "float",
            }
        ]

        print("[TEST] Running test_valid_json_single_record...")

        processor = Processor(data_path, "json")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "json", negotiate_schema=True)
        schema = processor.run()
        self.assertEqual(schema, *expected_schemas)

        processor = Processor(data_path, "json")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)

    def test_malformed_json_list(self):
        data_path = os.path.join(self.data_path, "malformed_json_list")
        expected_schemas = [{}]

        print("[TEST] Running malformed_json_list...")

        processor = Processor(data_path, "json")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "json", negotiate_schema=True)
        schema = processor.run()
        self.assertEqual(schema, *expected_schemas)

        processor = Processor(data_path, "json")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)

    def test_malformed_json_single_record(self):
        data_path = os.path.join(self.data_path, "malformed_json_single_record")
        expected_schemas = [{}]

        print("[TEST] Running malformed_json_single_record...")

        processor = Processor(data_path, "json")
        schemas = processor.run()
        self.assertEqual(schemas, expected_schemas)

        processor = Processor(data_path, "json", negotiate_schema=True)
        schema = processor.run()
        self.assertEqual(schema, *expected_schemas)

        processor = Processor(data_path, "json")
        schemas = processor.run_workers()
        self.assertEqual(schemas, expected_schemas)
