#!/usr/bin/env python
"""Cli utilities."""
from __future__ import annotations

# std
import json
import sys
import time
import os
from typing import Optional
import subprocess
import secrets
import socket

# external
import click
from redis import Redis

# funsies
import funsies

# module
from . import __version__, types  # noqa
from ._constants import hash_t
from ._graph import get_status
from ._logging import logger
from .config import Server

# required funsies libraries loaded in advance
import hashlib, subprocess, loguru  # noqa isort:skip

def step_arg(step: str, arg: str) -> None:
    """Print a step to output."""
    click.echo(f"{click.style(step, bold=True):30}{arg:62}", nl=False, err=True)
def status_done() -> None:
    """Print DONE."""
    click.echo(click.style("DONE", fg="green", bold=True), err=True)


def status_fail() -> None:
    """Print FAIL."""
    click.echo(click.style("FAIL", fg="red", bold=True), err=True)


def status_skip() -> None:
    """Print FAIL."""
    click.echo(click.style("SKIP", fg="blue", bold=True), err=True)

def info_print(term: str, details: str) -> None:
    """Print formatted funsies hash."""
    click.echo(f"{click.style(term, bold=True):>26} | {details}", err=True)


# This is the main command
@click.command()
@click.version_option(__version__)
@click.option(
    "--dir",
    "-d",
    type=click.Path(),
    nargs=1,
    default=None,
    help="Data connection URL.",
)

@click.option(
    "--disk/--no-disk",
    default=True,
    help="Save data to disk or within the Redis instance.",
)
def main(dir:Optional[str], disk:bool) -> None:
    """Easy initialization of a funsies environment.

    This command condenses the steps required to setup a funsies environment.
    It starts a password protected redis server, sets up the appropriate
    environment variables, initialize the data directory etc.
    """

    # Set directory
    if dir is None:
        dir = os.getcwd()

    # make global path
    dir = os.path.abspath(dir)

    # redis parameters
    redis_password = secrets.token_hex(12)
    redis_port = 16379
    redis_hostname = socket.gethostname()
    redis_url = f"redis://:{redis_password}@{redis_hostname}:{redis_port}"
    info_print("jobs server", redis_url)

    # data parameters
    if disk:
        data_url = f"file://{os.path.join(dir,'data')}"
    else:
        data_url = redis_url
    info_print("data", data_url)

    # setup the redis settings
    redis_conf = os.path.join(dir, "redis.conf")
    step_arg("redis config", redis_conf)
    with open(redis_conf, "w") as f:
        f.write(f"dir {dir}\n")
        f.write(f"logfile redis.log\n")
        f.write(f"requirepass {redis_password}\n")
        f.write(f"port {redis_port}\n")
        f.write("save 300 1\n")
    status_done()

    # setup the env variables
    step_arg("env variables","")
    os.environ['FUNSIES_JOBS'] = redis_url
    os.environ['FUNSIES_DATA'] = data_url
    env_file = dict(FUNSIES_JOBS=redis_url, FUNSIES_DATA=data_url)
    status_done()

    # starting the server
    step_arg("redis","starting the server")
    redis_server = subprocess.Popen(
        ["redis-server", redis_conf],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(0.1)
    stat = redis_server.poll()
    if stat is not None:
        status_fail()
        assert redis_server.stderr is not None
        stdout = redis_server.stderr.read().decode()
        raise RuntimeError(f"Redis server failed to start, errcode={stat}\n{stdout}")
    status_done()

    # wait for server to start
    timeout = None # change?
    if timeout is not None:
        tmax = time.time() + timeout

    # wait for the server to load
    step_arg("redis", "connecting to server...")
    while True:
        try:
            db = Redis.from_url(redis_url, decode_responses=False)
            db.ping()
            break
        except Exception:
            time.sleep(1.0)

        if timeout is not None:
            t1 = time.time()
            if t1 > tmax:
                status_fail()
                logger.error("timeout exceeded")
                raise SystemExit(2)
    status_done()








if __name__ == "__main__":
    main()
