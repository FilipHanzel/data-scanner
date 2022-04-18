import os
import logging
from pprint import pprint

from data_scanner import Processor
from data_scanner import setLoggingLevel

if __name__ == "__main__":
    # On windows run_workers has to be invoked
    # from if __main__ == "__main__" block, to avoid
    # issues with spawning processes without fork
    
    script_path = os.path.dirname(os.path.abspath(__file__))
    test_csv_data_path = os.path.join(script_path, "tests", "data", "csv")
    test_json_data_path = os.path.join(script_path, "tests", "data", "json")

    setLoggingLevel(logging.DEBUG)

    schemas = Processor(test_csv_data_path, "csv").run()
    print("Detected schemas:")
    pprint(schemas)

    schemas = Processor(test_csv_data_path, "csv").run_workers()
    print("Detected schemas:")
    pprint(schemas)

    schemas = Processor(test_json_data_path, "json").run()
    print("Detected schemas:")
    pprint(schemas)

    schemas = Processor(test_json_data_path, "json").run_workers()
    print("Detected schemas:")
    pprint(schemas)
