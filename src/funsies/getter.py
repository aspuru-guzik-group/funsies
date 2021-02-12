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
from .context import get_db
from .logging import logger


def get(
    target: c.hash_t,
    connection: Optional[Redis[bytes]] = None,
) -> Union[Artefact, Funsie, Operation, None]:
    """Get the object returned by a given hash value."""
    db = get_db(connection)

    if db.hexists(c.ARTEFACTS, target):
        logger.debug(f"{target} is Artefact")
        # Load artefact
        return get_artefact(db, target)

    elif db.hexists(c.FUNSIES, target):
        logger.debug(f"{target} is Funsie")
        return get_funsie(db, target)

    elif db.hexists(c.OPERATIONS, target):
        logger.debug(f"{target} is Operation")
        return get_op(db, target)

    else:
        logger.debug(f"{target} does not exist")
        return None
