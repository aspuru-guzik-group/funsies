#!/usr/bin/env python
"""Custom worker class for RQ."""
import sys

from redis import Redis
from rq import Connection, Worker
import click

# required funsies libraries loaded in advance
import funsies, subprocess, msgpack, hashlib, loguru
from . import __version__
from .logging import logger


# This is the main funsies command
@click.group()
@click.version_option(__version__)
def main():
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
def worker(url, queues, burst, rq_logging, logging_level):
    """Starts an RQ worker for funsies."""
    with Connection(Redis.from_url(url)):
        logger.info(f"connected to {url}")
        queues = queues or ["default"]
        if burst:
            burst_mode = " in burst mode"
        else:
            burst_mode = ""
        logger.info(f"working on {', '.join(queues)}{burst_mode}")
        w = Worker(queues, log_job_description=False)
        w.work(burst=burst, logging_level=rq_logging)


@main.command()
@click.option(
    "--url",
    "-u",
    default="redis://localhost:6379",
    help="URL describing Redis connection details.",
)
def clean(url):
    """Reset job queues and DAGs."""
    with Connection(Redis.from_url(url)):
        logger.info(f"connected to {url}")
        logger.info("cleaning up")
        funsies.context.cleanup(Redis.from_url(url))
        logger.info("done")


if __name__ == "__main__":
    main()
