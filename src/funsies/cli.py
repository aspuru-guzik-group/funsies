#!/usr/bin/env python
"""Cli utilities."""
from __future__ import annotations

# std
import sys
import time
from typing import Optional

# external
import click
import redis
from redis import Redis
from rq import command, Connection, Worker

# required funsies libraries loaded in advance
import funsies, subprocess, hashlib, loguru  # noqa

# Local
from . import __version__, t
from ._graph import get_status
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
    except Exception as e:
        logger.error("could not connect to server! exiting")
        logger.error(str(e))
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
    db: Redis[bytes] = ctx.obj
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
def clean(ctx: click.Context) -> None:
    """Clean job queues and DAGs."""
    db = ctx.obj
    logger.info("cleaning up")
    funsies.context.cleanup_funsies(db)
    logger.success("done")


@main.command()
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Shutdown workers without finishing jobs.",
)
@click.pass_context
def shutdown(ctx: click.Context, force: bool) -> None:
    """Tell workers to shutdown."""
    db = ctx.obj
    workers = Worker.all(db)
    logger.info(f"shutting down {len(workers)} workers")
    for worker in workers:
        command.send_shutdown_command(db, worker.name)  # Tells worker to shutdown
        if force:
            command.send_kill_horse_command(db, worker.name)
    logger.success("done")


@main.command()
@click.argument("hashes", type=str, nargs=-1)
@click.pass_context
def cat(ctx: click.Context, hashes: tuple[str, ...]) -> None:
    """Print artefacts to stdout."""
    db = ctx.obj

    with funsies.context.Fun(db):
        for hash in hashes:
            logger.info(f"extracting {hash}")
            things = funsies.get(hash)
            if len(things) == 0:
                logger.error("hash does not correspond to anything!")
                raise SystemExit(2)

            if len(things) > 1:
                logger.error(f"hash resolves to {len(things)} things.")

            art = things[0]
            if isinstance(art, t.Artefact):
                res = funsies.take(art, strict=False)
                if isinstance(res, t.Error):
                    logger.warning(f"error at {hash}: {res.kind}")
                    if res.details is not None:
                        sys.stderr.buffer.write((res.details + "\n").encode())
                    logger.warning(f"error source: {res.source}")
                else:
                    sys.stdout.buffer.write(res)
                    logger.success(f"{hash} output to stdout")
            elif isinstance(art, t.Operation):
                logger.error("not an artefact")
                logger.info("did you mean...")
                sys.stderr.write("      INPUTS:\n")
                for key, val in art.inp.items():
                    sys.stderr.write(f"      {key:<30} -> {val[:8]}\n")
                sys.stderr.write("      OUTPUTS:\n")
                for key, val in art.out.items():
                    sys.stderr.write(f"      {key:<30} -> {val[:8]}\n")
            else:
                logger.error("not an artefact:")
                logger.error(f"{art}")


@main.command()
@click.option(
    "--output",
    "-o",
    type=click.Path(exists=False, writable=True),
    help="Output location (defaults to first characters of hash).",
)
@click.argument(
    "hash",
    type=str,
)
@click.pass_context
def debug(ctx: click.Context, hash: str, output: Optional[str]) -> None:
    """Extract all data related to a given hash value."""
    logger.info(f"extracting {hash}")
    db = ctx.obj
    if output is None:
        output = hash
        output2 = ""
    else:
        output2 = output + "_"

    with funsies.context.Fun(db):
        things = funsies.get(hash)
        if len(things) == 0:
            logger.error(f"{hash} does not correspond to anything!")
        elif len(things) == 1:
            logger.info(f"got {type(things[0])}")
            funsies.debug.anything(things[0], output)
            logger.success(f"saved to {output}")

        else:
            logger.warning(f"got {len(things)} objects")
            funsies.debug.anything(things[0], output)
            logger.success(f"{type(things[0])} -> {output}")
            for el in things[1:]:
                funsies.debug.anything(el, output2 + el.hash)
                logger.success(f"{type(el)} -> {output2 + el.hash}")


@main.command()
@click.argument(
    "hash",
    type=str,
)
@click.pass_context
def reset(ctx: click.Context, hash: str) -> None:
    """Reset operations and their dependents."""
    db = ctx.obj
    with funsies.context.Fun(db):
        things = funsies.get(hash)
        if len(things) == 0:
            logger.warning(f"no object with hash {hash}")
            raise SystemExit(2)
        if len(things) > 1:
            logger.error(f"more than object with hash starting with {hash}")
            logger.error("which one do you mean of :")
            for el in things:
                logger.error(f"\t{el.hash}")
            raise SystemExit(2)
        else:
            if isinstance(things[0], t.Artefact) or isinstance(things[0], t.Operation):
                funsies.ui.reset(things[0])
            else:
                logger.error(f"object {hash} is neither an operation or an artefact")


@main.command()
@click.argument(
    "hashes",
    type=str,
    nargs=-1,
)
@click.option("-t", "--timeout", type=float, help="Timeout in seconds.")
@click.pass_context
def wait(  # noqa:C901
    ctx: click.Context, hashes: tuple[str, ...], timeout: Optional[float]
) -> None:
    """Wait until redis database or certain hashes are ready."""
    db = ctx.obj
    if timeout is not None:
        tmax = time.time() + timeout

    while True:
        try:
            db.ping()
            break
        except redis.exceptions.BusyLoadingError:
            time.sleep(0.5)

        if timeout is not None:
            t1 = time.time()
            if t1 > tmax:
                logger.error("timeout exceeded")
                raise SystemExit(2)

    with funsies.context.Fun(db):
        h = []
        for hash in hashes:
            things = funsies.get(hash)
            if len(things) == 0:
                logger.warning(f"no object with hash {hash}")
            for el in things:
                if isinstance(el, t.Artefact):
                    h += [el.hash]
                    logger.info(f"waiting on artefact at {hash}")
                elif isinstance(el, t.Operation):
                    h += [next(iter(el.out.values()))]
                    logger.info(f"waiting on operation at {hash}")
                else:
                    logger.warning(f"ignoring {type(el)} at {el.hash}")

        while len(h) > 0:
            stat = get_status(db, h[0])
            if stat > 0:
                h.pop(0)
                logger.success(f"{len(h)} things left to wait for.")
            time.sleep(0.5)

            if timeout is not None:
                t1 = time.time()
                if t1 > tmax:
                    logger.error("timeout exceeded")
                    raise SystemExit(2)


@main.command()
@click.argument(
    "hashes",
    type=str,
    nargs=-1,
)
@click.pass_context
def graph(ctx: click.Context, hashes: tuple[str, ...]) -> None:
    """Print to stdout a DOT-formatted graph to visualize DAGs."""
    import funsies.graphviz

    db = ctx.obj
    with funsies.context.Fun(db):
        if len(hashes) == 0:
            # If no hashes are passed, we graph all the DAGs on index
            hashes = tuple(
                [dag.decode() for dag in db.smembers(funsies.constants.DAG_INDEX)]
            )

        all_data = []
        for hash in hashes:
            things = funsies.get(hash)
            if len(things) == 0:
                logger.warning(f"no object with hash {hash}")
            for el in things:
                if isinstance(el, t.Operation) or isinstance(el, t.Artefact):
                    all_data += [el.hash]

        if len(all_data):
            logger.info(f"writing graph for {len(all_data)} objects")
            out = funsies.graphviz.format_dot(
                *funsies.graphviz.export(db, all_data), targets=all_data
            )
            sys.stdout.write(out)
            logger.success("done")
        else:
            logger.error("No data points")
            raise SystemExit(2)


@main.command()
@click.argument(
    "hashes",
    type=str,
    nargs=-1,
)
@click.pass_context
def run(ctx: click.Context, hashes: tuple[str, ...]) -> None:
    """Enqueue execution of hashes."""
    db = ctx.obj
    with funsies.context.Fun(db):
        for hash in hashes:
            things = funsies.get(hash)
            if len(things) == 0:
                logger.warning(f"no object with hash {hash}")
            for el in things:
                if isinstance(el, t.Operation) or isinstance(el, t.Artefact):
                    funsies.execute(el)
                else:
                    logger.warning(f"object with hash {hash} of type {type(t)}")


if __name__ == "__main__":
    main()
