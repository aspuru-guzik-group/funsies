"""Logging and log levels."""
# std
import sys

# external
import loguru

# logger = logging.getLogger("funsies")
logger = loguru.logger
logger.remove()


# Standard logger for errors etc
def set_level(level: str) -> None:
    """Set logging level."""
    log_format = (
        "<green>{time:YYYYMMDD} {time:HH:mm:ss.S}</green>|"
        + " <level>{level: <8}</level> |"
        + " <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan>"
        + " <level>{message}</level>"
    )
    logger.remove()
    logger.add(
        sys.stderr,
        format=log_format,
        level=level,
        filter=lambda record: "op" not in record["extra"],
    )


# Default level
set_level("INFO")

# Error logger for operations
worker_format = " op:<green>{extra[op]}</green> |" + " <level>{message}</level>"
logger.add(
    sys.stderr,
    format=worker_format,
    filter=lambda record: "op" in record["extra"],
    level="INFO",
    backtrace=False,
)
