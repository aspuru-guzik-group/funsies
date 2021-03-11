"""Test funsies cleaning."""
from signal import SIGKILL
import time

# module
import funsies as f


def test_cleanup() -> None:
    """Test truncation."""
    import os

    def kill_self(*inp: bytes) -> bytes:
        pid = os.getpid()
        os.kill(pid, SIGKILL)
        time.sleep(2.0)
        return b"what"

    with f.ManagedFun(nworkers=1) as db:
        inp = "\n".join([f"{k}" for k in range(10)]).encode()
        dat1 = f.put(inp)
        fun = f.reduce(kill_self, inp)
        f.execute(fun)
        time.sleep(0.5)
        key1 = db.get(f._constants.join(f._constants.OPERATIONS, fun.parent, "owner"))
        f._context.cleanup_funsies(db)
        key2 = db.get(f._constants.join(f._constants.OPERATIONS, fun.parent, "owner"))
        assert key1 is not None
        assert key2 is None
