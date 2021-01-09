"""Contextual DB usage."""
# std
from contextlib import contextmanager
from typing import Iterator, Optional

# external
from redis import Redis
import rq
from rq.local import LocalStack

# A thread local stack of connections (adapted from RQ)
_connect_stack = LocalStack()


# --------------------------------------------------------------------------------
# Main DB context manager
@contextmanager
def Fun(connection: Optional[Redis] = None) -> Iterator[Redis]:
    """Context manager for redis connections."""
    if connection is None:
        connection = Redis()

    _connect_stack.push(connection)

    # also push on rq
    # TODO maybe just use the RQ version of this?
    rq.connections.push_connection(connection)
    try:
        yield _connect_stack.top
    finally:
        popped = _connect_stack.pop()
        assert popped == connection, (
            "Unexpected Redis connection was popped off the stack. "
            "Check your Redis connection setup."
        )
        rq.connections.pop_connection()


def get_db(db: Optional[Redis] = None) -> Redis:
    """Get Redis instance."""
    if db is not None:
        # explicit redis instance
        return db
    else:
        if _connect_stack.top is not None:
            # try context instance
            out: Redis = _connect_stack.top
            return out
        elif (job := rq.get_current_job()) is not None:
            out2: Redis = job.connection
            return out2
        else:
            raise RuntimeError("No redis instance available.")
