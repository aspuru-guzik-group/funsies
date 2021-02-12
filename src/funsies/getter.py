"""Generic getter of stuff."""
from __future__ import annotations

# std
from typing import Optional, Union

# external
from redis import Redis

# module
from . import constants as c
from ._funsies import Funsie, get_funsie
from ._graph import Artefact, get_artefact, get_op, Operation
from ._short_hash import hash_load
from .context import get_db
from .logging import logger


def get(
    target: str,
    connection: Optional[Redis[bytes]] = None,
) -> list[Union[Artefact, Funsie, Operation]]:
    """Get the object returned by a given hash value."""
    db = get_db(connection)
    hashes = hash_load(db, target)
    out: list[Union[Artefact, Funsie, Operation]] = []
    for h in hashes:
        if db.hexists(c.ARTEFACTS, h):
            logger.debug(f"{h} is Artefact")
            # Load artefact
            out += [get_artefact(db, h)]
        elif db.hexists(c.FUNSIES, h):
            logger.debug(f"{h} is Funsie")
            out += [get_funsie(db, h)]
        elif db.hexists(c.OPERATIONS, h):
            logger.debug(f"{h} is Operation")
            out += [get_op(db, h)]
        else:
            logger.debug(f"{h} does not exist")
    return out