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
from .constants import hash_t, short_hash
from .logging import logger


# This is the main funsies command
@click.group()
@click.version_option(__version__)
@click.option(
    "--url",
    "-u",
    type=str,
    default="redis://localhost:6379",
    help="URL describing Redis connection details.",
)
@click.pass_context
def main(ctx: click.Context, url: str) -> None:
    """Command-line tools for funsies."""
    logger.debug(f"connecting to {url}")
    db = Redis.from_url(url)
    try:
        db.ping()
    except Exception:
        logger.critical("could not connect to server! exiting")
        sys.exit(-1)
    logger.debug("connection sucessful")
    ctx.obj = db
    logger.info(f"connected to {url}")


@main.command()
@click.option(
    "--burst",
    "-b",
    is_flag=True,
    help="Run in burst mode (quit after all work is done)",
)
@click.option(
    "--rq-log-level",
    type=str,
    default="WARNING",
    help="Set logging for RQ",
    show_default=True,
)
@click.argument("queues", nargs=-1)
@click.pass_context
def worker(ctx: click.Context, queues, burst, rq_log_level):  # noqa:ANN001,ANN201
    """Starts an RQ worker for funsies."""
    db: Redis = ctx.obj
    with Connection(db):
        queues = queues or ["default"]
        if burst:
            burst_mode = " in burst mode"
        else:
            burst_mode = ""
        logger.success(f"working on queues={', '.join(queues)}{burst_mode}")
        w = Worker(queues, log_job_description=False)
        w.work(burst=burst, logging_level=rq_log_level)


@main.command()
@click.pass_context
def clean(ctx: click.Context):  # noqa:ANN001,ANN201
    """Reset job queues and DAGs."""
    db = ctx.obj
    logger.info("cleaning up")
    funsies.context.cleanup_funsies(db)
    logger.info("done")


# @main.command()
# @click.option(
#     "--output",
#     "-o",
#     type=click.Path(exists=False, writable=True),
#     help="Output location (defaults to first characters of hash).",
# )
# @click.argument(
#     "hash",
#     type=str,
# )
# @click.pass_context
# def get(ctx: click.Context, hash: str, output: Optional[str]) -> None:
#     """Extract data related to a given hash value."""
#     db = ctx.obj
#     if output is None:
#         output = short_hash(hash)

#     with funsies.context.Fun(db):
#         funsies.take(hash_t(hash))


if __name__ == "__main__":
    main()
