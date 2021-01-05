"""Functions for describing redis-backed DAGs."""
# std
import logging

# external
from redis import Redis

# module
from ._funsies import FunsieHow
from ._graph import get_artefact, get_data, get_op, set_data
from ._shell import run_shell_funsie  # runner for shell
from .constants import hash_t

# Dictionary of runners
RUNNERS = {FunsieHow.shell: run_shell_funsie}


def run_op(db: Redis, address: hash_t) -> bool:
    """Run a data operation from it's address."""
    # load the operation
    op = get_op(db, address)
    runner = RUNNERS[op.funsie.how]

    # load input files
    input_data = {}
    for key, val in op.inp.items():
        artefact = get_artefact(db, val)
        input_data[key] = get_data(db, artefact)

    out_data = runner(op.funsie, input_data)

    for key, val in out_data.items():
        if val is None:
            logging.warning(f"Missing output data for {key}")
        else:
            artefact = get_artefact(db, op.out[key])
            set_data(db, artefact, val)

    return True
