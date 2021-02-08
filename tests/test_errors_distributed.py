"""Test errors in distributed context."""
# std
import time

# external
import pytest

# module
import funsies as f


def test_raising_funsie() -> None:
    """Test funsie that raises an error.

    This test is specifically designed to catch the bug fixed in fa9af6a4
    where funsies that raised did not release their locks, leading to a race
    condition.
    """

    def raising_fun(*inp: bytes) -> bytes:
        raise RuntimeError("this funsie raises.")

    with f.ManagedFun(nworkers=2):
        s0a = f.morph(lambda x: x, "bla blabla")
        s0b = f.morph(lambda x: x, "blala")
        s1 = f.reduce(raising_fun, "bla bla", s0a, s0b, strict=True)
        f.execute(s1)
        f.wait_for(s1, timeout=2)
        with pytest.raises(f.UnwrapError):
            _ = f.take(s1)

        s2 = f.morph(lambda x: x, s1)
        f.execute(s2)
        f.wait_for(s2, timeout=0.5)
