import sys
import logging
import traceback

logger = logging.getLogger(__name__)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
logger.addHandler(handler)

logger.setLevel(logging.INFO)


def setLoggingLevel(level) -> None:
    logger.setLevel(level)


def traceback_format(exception) -> str:
    return "\n".join(traceback.format_tb(exception.__traceback__))
