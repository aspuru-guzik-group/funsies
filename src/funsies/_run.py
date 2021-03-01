"""Functions for describing redis-backed DAGs."""
from __future__ import annotations

# std
from contextlib import ContextDecorator
from enum import IntEnum
import signal
import traceback
from types import FrameType
from typing import Any, Dict, Optional, Union

# external
from redis import Redis
import rq

# module
from . import _serdes
from ._constants import ARTEFACTS, hash_t, join
from ._context import _options_stack
from ._funsies import Funsie, FunsieHow
from ._graph import (
    Artefact,
    ArtefactStatus,
    create_link,
    get_bytes,
    get_status,
    mark_error,
    Operation,
    resolve_link,
    set_data,
)
from ._logging import logger
from ._pyfunc import run_python_funsie  # runner for python functions
from ._shell import run_shell_funsie  # runner for shell
from ._subdag import run_subdag_funsie  # runner for shell
from .errors import Error, ErrorKind, Result

# Dictionary of runners
RUNNERS = {
    FunsieHow.shell: run_shell_funsie,
    FunsieHow.python: run_python_funsie,
    FunsieHow.subdag: run_subdag_funsie,
}
out_data_t = Union[Dict[str, Optional[object]], Dict[str, Optional[Artefact[Any]]]]


# ----------------------------------------------------------------------------- #
# Context manager for signals
HANDLED_SIGNALS = [signal.SIGINT, signal.SIGTERM]


class SignalError(Exception):
    """Error raised by signal handler."""

    pass


def _signal_failure(signum: signal.Signals, frame: FrameType) -> None:
    raise SignalError(str(signum))


class catch_signals(ContextDecorator):
    """Decorator to catch termination signals."""

    def __init__(self: "catch_signals") -> None:
        """Start handler."""
        pass

    def __enter__(self: "catch_signals") -> None:
        """Enter signal handler."""
        self.original_handlers = dict(
            [(s, signal.getsignal(s)) for s in HANDLED_SIGNALS]
        )
        for s in HANDLED_SIGNALS:
            signal.signal(s, _signal_failure)

    def __exit__(self: "catch_signals", exc_type, exc, exc_tb):  # noqa
        """Exit signal handler."""
        for key, val in self.original_handlers.items():
            signal.signal(key, val)


# ----------------------------------------------------------------------------- #
class RunStatus(IntEnum):
    """Possible status of running an operation."""

    # <= 0 -> issue prevented running job.
    subdag_ready = -5
    delayed = -3
    unmet_dependencies = -2
    not_ready = -1
    # > 0 -> executed, can run dependents.
    executed = 1
    using_cached = 2
    input_error = 4


def is_it_cached(db: Redis[bytes], op: Operation) -> bool:
    """Check if an operation is fully cached and doesn't need to be recomputed."""
    # We do this by checking whether all of it's outputs are already saved.
    # This ensures that there is no mismatch between artefact statuses and the
    # status of generating operations.
    keys = [
        join(ARTEFACTS, resolve_link(db, address), "status")
        for address in op.out.values()
    ]

    def __status(p: Redis[bytes]) -> bool:
        for address in op.out.values():
            stat = get_status(p, address)
            if stat <= ArtefactStatus.no_data:
                return False
        return True

    answer: bool = db.transaction(  # type:ignore
        __status, *keys, watch_delay=0.5, value_from_callable=True
    )
    return answer


def dependencies_are_met(db: Redis[bytes], op: Operation) -> bool:
    """Check if all the dependencies of an operation are met."""
    for val in op.inp.values():
        stat = get_status(db, resolve_link(db, val))
        if stat <= ArtefactStatus.no_data:
            return False

    return True


@catch_signals()
def run_op(  # noqa:C901
    db: Redis[bytes], op: Union[Operation, hash_t], evaluate: bool = True
) -> RunStatus:
    """Run an Operation from its hash address."""
    # Compatibility feature
    if not isinstance(op, Operation):
        op = Operation.grab(db, op)

    logger.info(f"=== {op.hash} ===")
    logger.info("evaluating...")

    # Check if the current job needs to be done at all
    # ------------------------------------------------
    if is_it_cached(db, op):
        # All outputs are ok. We exit this run.
        logger.success("DONE: using cached data.")
        return RunStatus.using_cached

    if not evaluate:
        raise RuntimeError("Attempting to run an operation, but evaluate = False.")

    # # Then we check if all the inputs are ready to be processed.
    if not dependencies_are_met(db, op):
        logger.success("DONE: waiting on dependencies.")
        return RunStatus.unmet_dependencies

    # load the funsie
    funsie = Funsie.grab(db, op.funsie)

    # set options in case we need them in the funsie
    _options_stack.push(op.options)
    # we don't have to pop it later because this process is going to die

    # load input files
    input_data: dict[str, Result[bytes]] = {}
    for key, val in op.inp.items():
        artefact = Artefact[Any].grab(db, val)
        dat = get_bytes(db, artefact, carry_error=op.hash)
        if isinstance(dat, Error):
            if funsie.error_tolerant:
                logger.warning(f"error on input {key} (tolerated).")
                input_data[key] = dat
            else:
                # forward errors and stop
                for val in op.out.values():
                    mark_error(db, val, dat)
                logger.error(f"DONE: error on input {key} (fragile).")
                return RunStatus.input_error
        else:
            input_data[key] = dat

    logger.info("running...")
    try:
        runner = RUNNERS[funsie.how]
        out_data: out_data_t = runner(funsie, input_data)  # type:ignore

    # Timed out
    except rq.timeouts.JobTimeoutException as e:
        for val in op.out.values():
            mark_error(
                db,
                val,
                error=Error(
                    kind=ErrorKind.JobTimedOut,
                    source=op.hash,
                    details=e.args[0],
                ),
            )
        logger.error("DONE: runner timed out.")
        return RunStatus.executed

    # Killed by signal
    except SignalError as e:
        logger.exception("runner raised!")
        tb_exc = traceback.format_exc()
        # much trouble
        for val in op.out.values():
            mark_error(
                db,
                val,
                error=Error(
                    kind=ErrorKind.KilledBySignal,
                    source=op.hash,
                    details=f"signal={e}",
                ),
            )
        logger.error("DONE: runner killed by signal.")
        return RunStatus.executed

    # Anything else
    except Exception:
        logger.exception("runner raised!")
        tb_exc = traceback.format_exc()
        # much trouble
        for val in op.out.values():
            mark_error(
                db,
                val,
                error=Error(
                    kind=ErrorKind.ExceptionRaised,
                    source=op.hash,
                    details=tb_exc,
                ),
            )
        logger.error("DONE: runner raised exception.")
        return RunStatus.executed

    for key, val in out_data.items():
        if val is None:
            logger.warning(f"no output data for {key}")
            mark_error(
                db,
                op.out[key],
                error=Error(
                    kind=ErrorKind.MissingOutput,
                    source=op.hash,
                    details="output not returned by runner",
                ),
            )
            # not that in this case, the other outputs are not necesserarily
            # invalidated, only this one.
        elif funsie.how == FunsieHow.subdag:
            assert isinstance(val, Artefact)
            create_link(db, op.out[key], val.hash)
        else:
            assert not isinstance(val, Artefact)
            set_data(
                db,
                op.out[key],
                _serdes.encode(funsie.out[key], val),
                status=ArtefactStatus.done,
            )

    if funsie.how == FunsieHow.subdag:
        logger.success("DONE: subdag ready.")
        return RunStatus.subdag_ready
    else:
        logger.success("DONE: successful eval.")
        return RunStatus.executed
