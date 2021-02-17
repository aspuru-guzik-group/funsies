"""Test errors in distributed context."""
# std
import time

# external
import pytest

# module
import funsies as f
from funsies.t import UnwrapError


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
        with pytest.raises(UnwrapError):
            _ = f.take(s1)

        s2 = f.morph(lambda x: x, s1)
        f.execute(s2)
        f.wait_for(s2, timeout=0.5)


def test_timeout_funsie() -> None:
    """Test funsie that timeout.

    This test is specifically designed to catch the deadlock produced when a
    funsie times out.

    """

    def timeout_fun(*inp: bytes) -> bytes:
        time.sleep(3.0)
        return b"what"

    with f.ManagedFun(nworkers=1):
        # Test when python function times out
        s1 = f.reduce(timeout_fun, "bla bla", "bla bla", opt=f.options(timeout=1))
        f.execute(s1)
        f.wait_for(s1, timeout=2)
        with pytest.raises(UnwrapError):
            _ = f.take(s1)

        # Test when shell function times out
        s1 = f.shell("sleep 20", opt=f.options(timeout=1))
        f.execute(s1)
        f.wait_for(s1, timeout=2)
        with pytest.raises(UnwrapError):
            _ = f.take(s1.stdout)


@pytest.mark.parametrize("nworkers", [1, 2, 8])
def test_data_race(nworkers: int) -> None:
    """Test a data race when execute calls are interleaved."""
    with f.ManagedFun(nworkers=nworkers):
        dat = f.put(b"bla bla")
        step1 = f.morph(lambda x: x.decode().upper().encode(), dat)
        step2 = f.shell(
            "cat file1 file2; grep 'bla' file2 file1 > file3; date >> file3",
            inp=dict(file1=step1, file2=dat),
            out=["file2", "file3"],
        )

        f.execute(step1)
        f.execute(step2)
        f.wait_for(step1, timeout=1.0)
        f.wait_for(step2, timeout=1.0)
