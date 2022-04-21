import os
import sys
import logging
from pprint import pprint

script_path = os.path.dirname(os.path.abspath(__file__))
data_scanner_path = os.path.join(script_path, "..")
sys.path.append(data_scanner_path)

from data_scanner import Processor
from data_scanner import setLoggingLevel


if __name__ == "__main__":
    # On windows run_workers has to be invoked from if __main__ == "__main__" block,
    # to avoid recursively spawning processes, since there is no fork and the code
    # has to be rerun for every process

    example_file_csv = os.path.join(script_path, "example.csv")
    example_file_json = os.path.join(script_path, "example.json")

    setLoggingLevel(logging.DEBUG)

    # CSV

    schemas = Processor(example_file_csv, "csv").run()
    print("Here is a result of scanning a single csv file:")
    pprint(schemas)
    # [{'column_a': 'integer',
    # 'column_b': 'float',  
    # 'column_c': 'boolean',
    # 'column_d': 'string',
    # 'column_e': 'date',
    # 'column_f': 'timestamp',
    # 'column_g': 'unknown',
    # 'column_h': 'json'}]

    schemas = Processor(example_file_csv, "csv").run_workers()
    print("Here is the same schema, but found using multiprocessing:")
    pprint(schemas)

    schemas = Processor(example_file_csv, "csv", negotiate_schema=True).run()
    print(
        "Here is the result from before, but this time,\n"
        "reduced to a single schema instead of a list of schemas:"
    )
    pprint(schemas)

    # JSON

    schemas = Processor(example_file_json, "json").run()
    print("Here is a result of scanning a single json file:")
    pprint(schemas)
    # [{'column_a': 'string',
    # 'column_b': 'string',
    # 'column_c': 'integer',
    # 'column_d': 'float',
    # 'column_e': 'unknown',
    # 'column_f': 'date',
    # 'column_g': 'timestamp',
    # 'column_h_nested_a': 'string',
    # 'column_h_nested_b': 'string',
    # 'column_h_nested_c': 'json',
    # 'column_i': 'json'}]

    schemas = Processor(example_file_json, "json").run_workers()
    print("Here is the same schema, but found using multiprocessing:")
    pprint(schemas)

    schemas = Processor(example_file_json, "json", negotiate_schema=True).run_workers()
    print(
        "Here is the result from before, but this time,\n"
        "reduced to a single schema instead of a list of schemas:"
    )
    pprint(schemas)
