"""Functions for describing redis-backed DAGs."""
# std
from enum import IntEnum
import logging
from typing import Dict

# external
from redis import Redis

# module
from ._funsies import FunsieHow, get_funsie
from ._graph import (
    ArtefactStatus,
    get_artefact,
    get_data,
    get_op,
    get_status,
    mark_error,
    set_data,
)
from ._pyfunc import run_python_funsie  # runner for python functions
from ._shell import run_shell_funsie  # runner for shell
from .constants import hash_t, SREADY, SRUNNING
from .errors import Error, ErrorKind, Option

# Dictionary of runners
RUNNERS = {FunsieHow.shell: run_shell_funsie, FunsieHow.python: run_python_funsie}


class RunStatus(IntEnum):
    """Possible status of running an operation."""

    # <= 0 -> issue prevented running job.
    unmet_dependencies = -2
    not_ready = -1
    # > 0 -> executed, can run dependents.
    executed = 1
    using_cached = 2


def __make_ready(db: Redis, address: hash_t) -> None:
    # Move back to the ready list
    val: int = db.smove(SRUNNING, SREADY, address)  # type:ignore
    if val != 1:
        raise RuntimeError(
            f"Critical error in running {address}!"
            + " Someone moved it out of SRUNNING while it was running!"  # noqa
        )


def run_op(db: Redis, address: hash_t) -> RunStatus:  # noqa:C901
    """Run an Operation from its hash address."""
    # Check if job is ready for execution
    # -----------------------------------
    # First, we check if this address is even ready to go. We do this by
    # moving the job from READY->DONE. We do it this way because this
    # operation is atomic. Thus, if any other worker is also attempting to
    # start this job, they'll know we currently are processing it.
    val: int = db.smove(SREADY, SRUNNING, address)  # type:ignore
    if val == 0:
        # job is NOT ready. return.
        logging.info(f"op skipped: {address} is being processed elsewhere")
        return RunStatus.not_ready

    # load the operation
    op = get_op(db, address)

    # Check if the current job needs to be done at all
    # ------------------------------------------------
    # We do this by checking whether all of it's outputs are already saved.
    # This ensures that there is no mismatch between artefact statuses and the
    # status of generating functions.
    for val in op.out.values():
        stat = get_status(db, val)
        if stat <= ArtefactStatus.no_data:
            break
    else:
        # All outputs are ok. We exit this run.
        logging.info(f"op skipped: {address} has cached results")
        __make_ready(db, address)
        return RunStatus.using_cached

    # # Then we check if all the inputs are ready to be processed.
    for val in op.inp.values():
        stat = get_status(db, val)
        if stat <= ArtefactStatus.no_data:
            # One of the inputs is not processed yet, we return.
            logging.info(f"op skipped: {address} has unmet dependencies.")
            __make_ready(db, address)
            return RunStatus.unmet_dependencies

    # load the funsie
    funsie = get_funsie(db, op.funsie)
    runner = RUNNERS[funsie.how]

    # load input files
    input_data: Dict[str, Option[bytes]] = {}
    for key, val in op.inp.items():
        artefact = get_artefact(db, val)
        dat = get_data(db, artefact)
        if isinstance(dat, Error):
            if funsie.options_ok:
                logging.warning(f"input {key} to error-tolerant funsie has errors.")
                input_data[key] = dat
            else:
                logging.error(f"input {key} to error-fragile funsie has errors.")
                # forward errors and stop
                for val in op.out.values():
                    mark_error(db, val, dat)
        else:
            input_data[key] = dat

    logging.info(f"op running: {address}")
    try:
        out_data = runner(funsie, input_data)
    except Exception as e:
        # much trouble
        for val in op.out.values():
            mark_error(
                db,
                val,
                error=Error(
                    kind=ErrorKind.ExceptionRaised, source=address, details=str(e)
                ),
            )
        return RunStatus.executed

    for key, val in out_data.items():
        artefact = get_artefact(db, op.out[key])
        if val is None:
            logging.warning(f"{address} -> missing output data for {key}")
            mark_error(
                db,
                artefact.hash,
                error=Error(
                    kind=ErrorKind.MissingOutput,
                    source=address,
                    details="output not returned by runner",
                ),
            )
            # not that in this case, the other outputs are not necesserarily
            # invalidated, only this one.
        else:
            set_data(db, artefact, val)

    __make_ready(db, address)
    return RunStatus.executed
