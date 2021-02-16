"""Generic getter of stuff."""
from __future__ import annotations

# std
from typing import Optional, Union

# external
from redis import Redis

# module
from . import constants as c
from ._funsies import Funsie
from ._graph import Artefact, Operation
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
        if db.exists(c.join(c.ARTEFACTS, h)):
            logger.debug(f"{h} is Artefact")
            out += [Artefact.grab(db, h)]

        elif db.exists(c.join(c.FUNSIES, h)):
            logger.debug(f"{h} is Funsie")
            out += [Funsie.grab(db, h)]

        elif db.exists(c.join(c.OPERATIONS, h)):
            logger.debug(f"{h} is Operation")
            out += [Operation.grab(db, h)]

        else:
            logger.debug(f"{h} does not exist")
    return out
