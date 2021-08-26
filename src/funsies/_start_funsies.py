#!/usr/bin/env python
"""Cli utilities."""
from __future__ import annotations

# std
import os
import secrets
import socket
import subprocess
import time
from typing import Optional

# external
import click
from redis import Redis

# module
from . import __version__


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
@click.argument(
    "run_dir",
    type=click.Path(exists=True),
    default=".",
)
@click.option("--workers", nargs=1, type=int, help="Start INTEGER funsies workers.")
@click.option(
    "--disk/--no-disk",
    default=False,
    help="Save data to disk or within the Redis instance.",
)
@click.option(
    "--pw/--no-pw",
    default=True,
    help="Protect Redis server with a randomly generated password.",
)
@click.option(
    "--port",
    default=6379,
    type=int,
    help="Port for Redis server.",
)
@click.option(
    "--script",
    nargs=1,
    type=click.Path("r"),
    help="Run a python script after initialization.",
)
@click.option(
    "--stop",
    is_flag=True,
    default=False,
    help="Shutdown everything after the script ends.",
)
def main(
    run_dir: Optional[str],
    disk: bool,
    port: int,
    script: Optional[str],
    workers: Optional[int],
    pw: bool,
    stop: bool,
) -> None:
    """Easy initialization of a funsies environment.

    This command condenses the steps required to setup a funsies environment.
    It starts a password protected redis server, sets up the appropriate
    environment variables, initialize the data directory etc.

    To run a full funsies script in one go, do

    start-funsies run_dir --script workflow.py --workers 4 --stop

    In this case the workflow script should have both execute() and wait_for()
    steps, so that it blocks until the work is completed.
    """
    # Set run_directory
    if run_dir is None:
        run_dir = os.getcwd()

    # make global path
    run_dir = os.path.abspath(run_dir)

    # redis parameters
    redis_port = port
    redis_hostname = socket.gethostname()
    if pw:
        redis_password = secrets.token_hex(6)
        redis_url = f"redis://:{redis_password}@{redis_hostname}:{redis_port}"
    else:
        redis_password = ""
        redis_url = f"redis://{redis_hostname}:{redis_port}"

    # data parameters
    if disk:
        data_url = f"file://{os.path.join(run_dir,'funsies_data')}"
    else:
        data_url = redis_url

    # setup the redis settings
    redis_conf = os.path.join(run_dir, "redis.conf")
    env_file = os.path.join(run_dir, "funsies.env")

    info_print("jobs server", redis_url)
    info_print("redis conf", redis_conf)
    info_print("data", data_url)
    info_print("env", env_file)

    step_arg("writing", "redis config")
    with open(redis_conf, "w") as f:
        f.write(f"dir {run_dir}\n")
        f.write("logfile redis.log\n")
        if pw:
            f.write(f"requirepass {redis_password}\n")
        f.write(f"port {redis_port}\n")
        f.write("save 300 1\n")
    status_done()

    # writing env file
    step_arg("writing", "env file")
    with open(env_file, "w") as f:
        f.write(f"export FUNSIES_JOBS='{redis_url}'\n")
        os.environ["FUNSIES_JOBS"] = redis_url
        f.write(f"export FUNSIES_DATA='{data_url}'\n")
        os.environ["FUNSIES_DATA"] = data_url
    status_done()

    # starting the server
    step_arg("redis", "starting the server")
    redis_server = subprocess.Popen(
        ["redis-server", redis_conf],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    time.sleep(0.2)
    stat = redis_server.poll()
    if stat is not None:
        status_fail()
        assert redis_server.stderr is not None
        stdout = redis_server.stderr.read().decode()
        raise RuntimeError(f"Redis server failed to start, errcode={stat}\n{stdout}")
    status_done()

    # wait for server to start
    # timeout = None  # change?
    # if timeout is not None:
    #     tmax = time.time() + timeout

    # wait for the server to load
    step_arg("redis", "connecting to server...")
    while True:
        try:
            db = Redis.from_url(redis_url, decode_responses=False)
            db.ping()
            break
        except Exception:
            time.sleep(1.0)

        # if timeout is not None:
        #     t1 = time.time()
        #     if t1 > tmax:
        #         status_fail()
        #         logger.error("timeout exceeded")
        #         raise SystemExit(2)
    status_done()

    click.echo(
        f"""
Successful initialization!
--------------------------

To complete setup of the funsies environment, source the environment file

        source {env_file}

You should then be able to use funsies commands (funsies --help for a list of
them). You can shutdown the database and worker pool using

        funsies shutdown --all

"""
    )

    if workers is not None:
        if workers > 1:
            s = "s"
        else:
            s = ""
        step_arg("workers", f"starting pool of {workers} worker{s}")
        for i in range(workers):
            subprocess.Popen(
                [
                    "funsies",
                    "--jobs",
                    redis_url,
                    "--data",
                    data_url,
                    "--log",
                    os.path.join(run_dir, f"worker_log.{i}"),
                    "worker",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            time.sleep(0.1)
        status_done()

    if script is not None:
        step_arg("running script", script)
        out = subprocess.run(
            ["python", script], stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        if out.returncode != 0:
            status_fail()
            click.echo(f"Warning! error code = {out.returncode}")
        else:
            status_done()
        with open(os.path.join(run_dir, "script.out"), "wb") as fout:
            fout.write(out.stdout)
        with open(os.path.join(run_dir, "script.err"), "wb") as ferr:
            ferr.write(out.stderr)

    if stop:
        step_arg("shutdown", "pool and redis server")
        subprocess.Popen(
            ["funsies", "--jobs", redis_url, "--data", data_url, "shutdown", "--all"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        status_done()


if __name__ == "__main__":
    main()
