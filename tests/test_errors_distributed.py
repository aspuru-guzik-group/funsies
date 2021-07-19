"""Test errors in distributed context."""
from __future__ import annotations

# std
from signal import SIGINT, SIGKILL, SIGTERM
import time

# external
import pytest
from redis import Redis
from rq import Worker

# funsies
import funsies as f
from funsies.types import UnwrapError


# utility functions
def wait_for_workers(db: Redis[bytes], nworkers: int) -> None:
    """Wait till nworkers are connected."""
    while True:
        workers = Worker.all(connection=db)
        if len(workers) == nworkers:
            break
        else:
            time.sleep(0.1)


def test_raising_funsie() -> None:
    """Test funsie that raises an error.

    This test is specifically designed to catch the bug fixed in fa9af6a4
    where funsies that raised did not release their locks, leading to a race
    condition.
    """

    def raising_fun(*inp: str) -> bytes:
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


@pytest.mark.slow
def test_timeout_deadlock() -> None:
    """Test funsies that time out.

    Here we explicitly check if dependents are still enqueued or if the whole
    thing deadlocks.
    """

    def timeout_fun(*inp: str) -> bytes:
        time.sleep(3.0)
        return b"what"

    def cap(inp: bytes) -> bytes:
        return inp.capitalize()

    with f.ManagedFun(nworkers=2):
        # Test when python function times out
        s1 = f.reduce(timeout_fun, "bla bla", "bla bla", opt=f.options(timeout=1))
        s1b = f.morph(cap, s1)
        # Test when shell function times out
        s2 = f.shell("sleep 20", "echo 'bla bla'", opt=f.options(timeout=1))
        s2b = f.morph(cap, s2.stdouts[1])
        f.execute(s1b, s2b)

        # Check err for reduce
        f.wait_for(s1b, timeout=1.5)
        err = f.take(s1b, strict=False)
        assert isinstance(err, f.errors.Error)
        assert err.kind == f.errors.ErrorKind.JobTimedOut
        assert err.source == s1.parent

        # Check err for shell
        f.wait_for(s2b, timeout=1.5)
        err = f.take(s2b, strict=False)
        assert isinstance(err, f.errors.Error)
        assert err.kind == f.errors.ErrorKind.JobTimedOut
        assert err.source == s2.hash


@pytest.mark.slow
@pytest.mark.parametrize("nworkers", [1, 2])
@pytest.mark.parametrize("sig", [SIGTERM, SIGKILL])
def test_worker_killed(nworkers: int, sig: int) -> None:
    """Test what happens when 'funsies worker' gets killed."""
    # std
    import os

    def kill_funsies_worker(*inp: bytes) -> bytes:
        pid = os.getppid()
        os.kill(pid, sig)
        time.sleep(2.0)
        return b"what"

    def cap(inp: bytes) -> bytes:
        return inp.upper()

    with f.ManagedFun(nworkers=nworkers) as db:
        wait_for_workers(db, nworkers)
        s1 = f.reduce(
            kill_funsies_worker, b"bla bla", b"bla bla", opt=f.options(timeout=3)
        )
        s1b = f.morph(cap, s1)
        f.execute(s1b)

        if nworkers == 1:
            # no other workers to pick up the slack
            with pytest.raises(TimeoutError):
                f.wait_for(s1b, timeout=1)
        else:
            # everything is ok
            f.wait_for(s1b, timeout=1)
            assert f.take(s1b) == b"WHAT"


@pytest.mark.slow
@pytest.mark.parametrize("nworkers", [1, 2])
@pytest.mark.parametrize("sig", [SIGTERM, SIGINT])
def test_job_killed(nworkers: int, sig: int) -> None:
    """Test what happens when 'funsies worker' is ok but its job gets killed."""
    # std
    import os

    def kill_self(*inp: bytes) -> bytes:
        pid = os.getpid()
        os.kill(pid, sig)
        time.sleep(2.0)
        return b"what"

    def cap(inp: bytes) -> bytes:
        return inp.upper()

    with f.ManagedFun(nworkers=nworkers) as db:
        wait_for_workers(db, nworkers)
        s1 = f.reduce(kill_self, b"bla bla", b"bla bla", opt=f.options(timeout=3))
        s1b = f.morph(cap, s1)
        f.execute(s1b)

        # error
        f.wait_for(s1b, timeout=1)
        err = f.take(s1b, strict=False)
        assert isinstance(err, f.errors.Error)
        assert err.kind == f.errors.ErrorKind.KilledBySignal


@pytest.mark.slow
@pytest.mark.parametrize("nworkers", [1, 2, 4])
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
        f.wait_for(step1, timeout=20.0)
        f.wait_for(step2, timeout=20.0)


@pytest.mark.slow
@pytest.mark.parametrize("nworkers", [1, 2, 8])
def test_double_execution(nworkers: int) -> None:
    """Test multiple executions of the same task."""
    # This test will fail if a job is re-executed multiple times.
    # external
    from rq.job import get_current_job

    def track_runs(inp: bytes) -> bytes:
        job = get_current_job()
        db: Redis[bytes] = job.connection
        val = db.incrby("sentinel", 1)
        time.sleep(0.5)
        return str(val).encode()

    with f.ManagedFun(nworkers=nworkers):
        # wait_for_workers(db, nworkers)
        dat = f.put(b"bla bla")
        step1 = f.morph(track_runs, dat)

        step1a = f.shell(
            "cat file1",
            inp=dict(file1=step1),
        )

        step1b = f.shell(
            "cat file2",
            inp=dict(file2=step1),
        )

        f.execute(step1a)
        f.execute(step1b)
        f.wait_for(step1a, timeout=10.0)
        f.wait_for(step1b, timeout=10.0)
        assert f.take(step1a.stdout) == b"1"
