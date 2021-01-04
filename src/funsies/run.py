"""Functions for describing redis-backed DAGs."""
# std
from dataclasses import asdict, dataclass
import hashlib
import logging
from typing import Dict, Optional, Type

# external
from msgpack import packb, unpackb
from redis import Redis

# module
from ._funsies import ART_TYPES, Funsie, FunsieHow
from ._locations import _ARTEFACTS, _OPERATIONS, _STORE
from ._graph import update_artefact, get_artefact, get_data, get_op, Operation

# runners
from ._shell import run_shell_funsie


RUNNERS = {FunsieHow.shell: run_shell_funsie}


def run(db: Redis, address: str) -> bool:
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
        artefact = get_artefact(db, op.out[key])
        update_artefact(db, artefact, val)

    return True
