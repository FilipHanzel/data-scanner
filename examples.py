import os
import logging
from pprint import pprint

from data_scanner import Processor
from data_scanner import setLoggingLevel

if __name__ == "__main__":
    script_path = os.path.dirname(os.path.abspath(__file__))
    test_data_path = os.path.join(script_path, "tests", "data", 'essa')

    setLoggingLevel(logging.DEBUG)

    schemas = Processor(test_data_path).run()
    pprint(schemas)

    # On windows run_workers has to be invoked
    # from if __main__ == "__main__" block, to avoid
    # issues with spawning processes without fork
    schemas = Processor(test_data_path).run_workers()
    pprint(schemas)
