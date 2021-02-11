#!/usr/bin/env python
"""Cli utilities."""
import sys
from typing import Optional

# external
import click
from redis import Redis
from rq import Connection, Worker

# required funsies libraries loaded in advance
import funsies, subprocess, msgpack, hashlib, loguru  # noqa

# Local
from . import __version__
from .logging import logger


# This is the main funsies command
@click.group()
@click.version_option(__version__)
def main() -> None:
    """Command-line tools for funsies."""
    pass


@main.command()
@click.option(
    "--url",
    "-u",
    default="redis://localhost:6379",
    help="URL describing Redis connection details.",
)
@click.option(
    "--burst",
    "-b",
    is_flag=True,
    help="Run in burst mode (quit after all work is done)",
)
@click.option("--rq_logging", type=str, default="WARNING", help="Set logging for RQ")
@click.option(
    "--logging-level", type=str, default="INFO", help="Set logging level for funsies"
)
@click.argument("queues", nargs=-1)
def worker(url, queues, burst, rq_logging, logging_level):  # noqa:ANN001,ANN201
    """Starts an RQ worker for funsies."""
    logger.debug(f"connecting to {url}")
    db = Redis.from_url(url)
    try:
        db.ping()
    except Exception:
        logger.critical("could not connect to server! exiting")
        sys.exit(-1)
    logger.debug("connection sucessful")

    with Connection(db):
        queues = queues or ["default"]
        if burst:
            burst_mode = " in burst mode"
        else:
            burst_mode = ""
        logger.success(f"url={url} queues={', '.join(queues)}{burst_mode}")
        w = Worker(queues, log_job_description=False)
        w.work(burst=burst, logging_level=rq_logging)


@main.command()
@click.option(
    "--url",
    "-u",
    default="redis://localhost:6379",
    help="URL describing Redis connection details.",
)
def clean(url):  # noqa:ANN001,ANN201
    """Reset job queues and DAGs."""
    with Connection(Redis.from_url(url)):
        logger.info(f"connected to {url}")
        logger.info("cleaning up")
        funsies.context.cleanup_funsies(Redis.from_url(url))
        logger.info("done")


@main.command()
@click.option(
    "--url",
    "-u",
    default="redis://localhost:6379",
    help="URL describing Redis connection details.",
)
@click.option(
    "--output",
    "-o",
    type=str,
    help="Folder were to save the output hash.",
)
@click.argument(
    "hash",
    type=str,
)
def get(hash, url, output: Optional[str]):  # noqa:ANN001,ANN201
    """Extract data related to a given hash value."""
    with funsies.context.Fun(Redis.from_url(url)):
        logger.info(f"connected to {url}")


if __name__ == "__main__":
    main()
