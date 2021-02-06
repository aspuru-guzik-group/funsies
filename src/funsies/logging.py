"""Logging and log levels."""
# std
import sys

# external
import loguru

# logger = logging.getLogger("funsies")
logger = loguru.logger
logger.remove()
logger.add(sys.stderr, filter="funsies", level="INFO")
