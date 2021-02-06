#!/usr/bin/env python
"""Custom worker class for RQ."""
import sys

from rq import Connection, Worker
import click

# required funsies libraries loaded in advance
import funsies, subprocess, msgpack, hashlib, loguru
from . import __version__


# This is the main funsies command
@click.group()
@click.version_option(__version__)
def main():
    """Command-line tools for funsies."""
    pass


@main.command()
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
def worker(queues, burst, rq_logging, logging_level):
    """Starts an RQ worker for funsies."""
    with Connection():
        queues = queues or ["default"]
        w = Worker(queues, log_job_description=False)
        w.work(burst=burst, logging_level=rq_logging)


@main.command()
def clean():
    """Reset job queues and DAGs."""
    pass


if __name__ == "__main__":
    worker()
