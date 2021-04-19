#!/usr/bin/env python
"""Cli utilities."""
from __future__ import annotations

# std
import json
import sys
import time
from typing import Optional

# external
import click
from redis import Redis
from rq import Connection, Worker

# funsies
import funsies

# module
from . import __version__, types  # noqa
from ._constants import hash_t
from ._graph import get_status
from ._logging import logger

# required funsies libraries loaded in advance
import hashlib, subprocess, loguru  # noqa isort:skip


# This is the main funsies command
@click.group()
@click.version_option(__version__)
@click.option(
    "--url",
    "-u",
    type=str,
    nargs=1,
    default=funsies.config._get_funsies_url(),
    show_default=True,
    help="Redis connection URL.",
)
@click.pass_context
def main(ctx: click.Context, url: str) -> None:
    """Command-line tools for funsies.

    The --url flag allows passing a custom url for the redis instance. This
    has to come before any of the subcommands (worker, cat etc.).
    Alternatively, the url can be set using the environment variables
    FUNSIES_URL environment variable.
    """
    hn = funsies.config._extract_hostname(url)
    logger.debug(f"connecting to {hn}")

    def connect2db(try_fail: bool = False) -> Redis[bytes]:
        db = Redis.from_url(url, decode_responses=False)
        try:
            db.ping()
        except Exception as e:
            if try_fail:
                raise e
            else:
                logger.error("could not connect to server! exiting")
                logger.error(str(e))
                sys.exit(-1)
        logger.debug("connection sucessful")
        logger.info(f"connected to {hn}")
        return db

    ctx.obj = connect2db


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
    db: Redis[bytes] = ctx.obj()
    with Connection(db):
        queues = queues or ["default"]
        if burst:
            burst_mode = " in burst mode"
        else:
            burst_mode = ""
        logger.success(f"working on queues={', '.join(queues)}{burst_mode}")
        w = Worker(
            queues,
            log_job_description=False,
            # TODO: make adjustable
            serializer="rq.serializers.JSONSerializer",
        )
        w.work(burst=burst, logging_level=rq_log_level)


@main.command()
@click.pass_context
def clean(ctx: click.Context) -> None:
    """Clean job queues and DAGs."""
    db = ctx.obj()
    logger.info("shutting down workers")
    funsies._context.shutdown_workers(db, True)
    logger.info("cleaning up")
    funsies._context.cleanup_funsies(db)
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
    db = ctx.obj()
    funsies._context.shutdown_workers(db, force)
    logger.success("done")


@main.command()
@click.argument("hashes", type=str, nargs=-1)
@click.pass_context
def cat(ctx: click.Context, hashes: tuple[str, ...]) -> None:
    """Print artefacts to stdout."""
    db = ctx.obj()

    with funsies._context.Fun(db):
        for hash in hashes:
            logger.info(f"extracting {hash}")
            things = funsies.get(hash)
            if len(things) == 0:
                logger.error("hash does not correspond to anything!")
                raise SystemExit(2)

            if len(things) > 1:
                logger.error(f"hash resolves to {len(things)} things.")

            art = things[0]
            if isinstance(art, types.Artefact):
                res = funsies.take(art, strict=False)
                if isinstance(res, types.Error):
                    logger.warning(f"error at {hash}: {res.kind}")
                    if res.details is not None:
                        sys.stderr.buffer.write((res.details + "\n").encode())
                    logger.warning(f"error source: {res.source}")
                elif isinstance(res, bytes):
                    sys.stdout.buffer.write(res)
                    logger.success(f"{hash} output to stdout")
                elif isinstance(res, str):
                    sys.stdout.buffer.write(res.encode())
                    logger.success(f"{hash} output to stdout")
                else:
                    sys.stdout.buffer.write(json.dumps(res).encode())
                    logger.success(f"{hash} output to stdout")

            elif isinstance(art, types.Operation):
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
    db = ctx.obj()
    if output is None:
        output = hash
        output2 = ""
    else:
        output2 = output + "_"

    with funsies._context.Fun(db):
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
    "hashes",
    type=str,
    nargs=-1,
)
@click.pass_context
def reset(ctx: click.Context, hashes: tuple[str, ...]) -> None:
    """Reset operations and their dependents."""
    db = ctx.obj()
    with funsies._context.Fun(db):
        for hash in hashes:
            things = funsies.get(hash)
            if len(things) == 0:
                logger.warning(f"no object with hash {hash}")
            if len(things) > 1:
                logger.error(f"more than one object with hash starting with {hash}")
                logger.info("which one do you mean of :")
                for t in things:
                    logger.info(f"\t{t.hash}")
                logger.info("none were reset")
            else:
                if isinstance(things[0], types.Artefact) or isinstance(
                    things[0], types.Operation
                ):
                    funsies.ui.reset(things[0])
                    logger.success(f"{hash} was reset")
                else:
                    logger.error(
                        f"object {hash} is neither an operation or an artefact"
                    )
                    logger.info(f"{hash} was not reset")


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
    if timeout is not None:
        tmax = time.time() + timeout

    while True:
        try:
            db = ctx.obj(try_fail=True)
            db.ping()
            break
        except Exception:
            time.sleep(1.0)

        if timeout is not None:
            t1 = time.time()
            if t1 > tmax:
                logger.error("timeout exceeded")
                raise SystemExit(2)

    with funsies._context.Fun(db):
        h = []
        for hash in hashes:
            things = funsies.get(hash)
            if len(things) == 0:
                logger.warning(f"no object with hash {hash}")
            for t in things:
                if isinstance(t, types.Artefact):
                    h += [t.hash]
                    logger.info(f"waiting on artefact at {hash}")
                elif isinstance(t, types.Operation):
                    h += [next(iter(t.out.values()))]
                    logger.info(f"waiting on operation at {hash}")
                else:
                    logger.warning(f"ignoring {type(t)} at {t.hash}")

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
@click.option(
    "-i",
    "--inputs",
    is_flag=True,
)
@click.pass_context
def graph(ctx: click.Context, hashes: tuple[str, ...], inputs: bool) -> None:
    """Print to stdout a DOT-formatted graph to visualize DAGs."""
    # funsies
    import funsies._graphviz

    db = ctx.obj()
    with funsies._context.Fun(db):
        if len(hashes) == 0:
            # If no hashes are passed, we graph all the DAGs on index
            hashes = tuple(
                [dag.decode() for dag in db.smembers(funsies._constants.DAG_INDEX)]
            )

        all_data: list[hash_t] = []
        for hash in hashes:
            things = funsies.get(hash.split("/")[-1])
            if len(things) == 0:
                logger.warning(f"no object with hash {hash}")
            for t in things:
                if isinstance(t, types.Operation) or isinstance(t, types.Artefact):
                    all_data += [hash_t(t.hash)]

        if len(all_data):
            logger.info(f"writing graph for {len(all_data)} objects")
            out = funsies._graphviz.format_dot(
                *funsies._graphviz.export(db, all_data),
                targets=all_data,
                show_inputs=inputs,
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
def execute(ctx: click.Context, hashes: tuple[str, ...]) -> None:
    """Enqueue execution of hashes."""
    db = ctx.obj()
    with funsies._context.Fun(db):
        exec_list = []
        for hash in hashes:
            things = funsies.get(hash)
            if len(things) == 0:
                logger.warning(f"no object with hash {hash}")
            for t in things:
                if isinstance(t, types.Operation) or isinstance(t, types.Artefact):
                    exec_list += [t]
                else:
                    logger.warning(f"object with hash {hash} of type {type(t)} skipped")

        funsies.execute(*exec_list)


if __name__ == "__main__":
    main()
