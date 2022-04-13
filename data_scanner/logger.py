import sys
import logging

logger = logging.getLogger(__name__)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
logger.addHandler(handler)

logger.setLevel(logging.INFO)


def setLoggingLevel(level):
    logger.setLevel(level)
