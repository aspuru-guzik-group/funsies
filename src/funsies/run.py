"""Functions for describing redis-backed DAGs."""
# std
import logging

# external
from redis import Redis

# module
from ._funsies import FunsieHow, get_funsie
from ._graph import get_artefact, get_data, get_op, set_data
from ._pyfunc import run_python_funsie  # runner for python functions
from ._shell import run_shell_funsie  # runner for shell
from .constants import hash_t

# Dictionary of runners
RUNNERS = {FunsieHow.shell: run_shell_funsie, FunsieHow.python: run_python_funsie}


def run_op(db: Redis, address: hash_t) -> bool:
    """Run an Operation from its hash address."""
    # load the operation
    op = get_op(db, address)
    # load the funsie
    funsie = get_funsie(db, op.funsie)
    runner = RUNNERS[funsie.how]

    # load input files
    input_data = {}
    for key, val in op.inp.items():
        artefact = get_artefact(db, val)
        input_data[key] = get_data(db, artefact)

    out_data = runner(funsie, input_data)

    for key, val in out_data.items():
        if val is None:
            logging.warning(f"Missing output data for {key}")
        else:
            artefact = get_artefact(db, op.out[key])
            set_data(db, artefact, val)

    return True
