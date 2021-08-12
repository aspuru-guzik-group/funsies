"""Generic getter of stuff."""
from __future__ import annotations

# std
from typing import Union

# module
from . import _constants as c
from ._context import Connection, get_connection
from ._funsies import Funsie
from ._graph import Artefact, Operation
from ._logging import logger
from ._short_hash import hash_load


def get(
    target: str,
    connection: Connection = None,
) -> list[Union[Artefact, Funsie, Operation]]:
    """Get object or objects that correspond to a given hash value.

    `get()` returns a list of objects (`Artefact`, `Operation` and `Funsie`
    instances) currently on the active Redis connection that have a hash
    address starting with `target`. This function allows programatically
    retrieving hashes like the ```funsies cat``` command does.

    Args:
        target: A hash or truncated hash value.
        connection (optional): An explicit Redis connection. Not required if
            called within a `Fun()` context.

    Returns:
        A list of objects with ids that start with `target`. Empty if no such
        objects exist.
    """
    db, store = get_connection(connection)
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
