"""Logging and log levels."""
# std
import sys

# external
import loguru

# logger = logging.getLogger("funsies")
logger = loguru.logger
logger.remove()
log_format = (
    "<green>{time:YYYYMMDD}</green>|<green>{time:HH:mm:ss}</green>|"
    + " <level>{level: <5}</level> |"
    + " <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)

logger.add(sys.stderr, format=log_format, filter="funsies", level="INFO")
