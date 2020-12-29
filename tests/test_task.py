"""Tests for commandline wrapper."""
# stdlib
import time
from typing import Optional, Tuple

# external
from fakeredis import FakeStrictRedis as Redis
from rq import Queue
from rq.job import Job

# module
from funsies import pull_file, pyfunc, run, shell
from funsies.types import FilePtr, pull, RTask, RTransformer


def assert_file(db: Redis, fid: Optional[FilePtr], equals: bytes) -> None:
    """Assert that a file exists and is equal to something."""
    assert fid is not None
    out = pull_file(db, fid)
    assert out is not None
    assert out == equals


def wait_for(job: Job) -> RTask:
    """Busily wait for a job to finish."""
    while True:
        if job.result is not None:
            t = pull(job.connection, job.result, "RTask")
            assert t is not None
            return t
        time.sleep(0.02)


def wait_for_transformer(job: Job) -> RTransformer:
    """Busily wait for a transformer to finish."""
    while True:
        if job.result is not None:
            t = pull(job.connection, job.result, "RTransformer")
            assert t is not None
            return t
        time.sleep(0.02)


def test_task() -> None:
    """Test that a task even runs."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)
    t = shell(db, "echo bla bla")
    job = q.enqueue(run, t.task_id)
    result = wait_for(job)
    print(shell)

    # read
    assert result.commands[0].stdout is not None
    assert result.commands[0].stderr is not None
    assert_file(db, result.commands[0].stdout, b"bla bla\n")
    assert_file(db, result.commands[0].stderr, b"")


def test_task_environ() -> None:
    """Test environment variable."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)
    t = shell(db, "env", env={"VARIABLE": "bla bla"})
    result = wait_for(q.enqueue(run, t.task_id))

    assert result.commands[0].stdout is not None
    out = pull_file(db, result.commands[0].stdout)
    assert out is not None
    assert out.splitlines()[0] == b"VARIABLE=bla bla"
    assert_file(db, result.commands[0].stderr, b"")


def test_task_file_in() -> None:
    """Test file input."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)
    t = shell(db, "cat file", inp={"file": b"bla bla\n"})
    result = wait_for(q.enqueue(run, t.task_id))
    assert_file(db, result.commands[0].stdout, b"bla bla\n")
    assert_file(db, result.commands[0].stderr, b"")


def test_task_identical_parameters() -> None:
    """Test task caching."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)
    t = shell(db, "cp file1 file2", inp={"file1": b"bla bla\n"}, out=["file2"])
    job = q.enqueue(run, t.task_id)
    result = wait_for(job)
    assert_file(db, t.out["file2"], b"bla bla\n")
    assert_file(db, result.commands[0].stderr, b"")
    id1 = job.result

    t = shell(db, "cp file1 file2", inp={"file1": b"bla bla\n"}, out=["file2"])
    job = q.enqueue(run, t.task_id)
    result = wait_for(job)
    id2 = job.result
    assert id1 == id2


def test_transformer() -> None:
    """Test transformer capabilities."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)
    t = shell(db, "cat file1", inp={"file1": b"bla bla\n"})

    def tfun(inp: bytes) -> bytes:
        return inp.decode().upper().encode()

    t2 = pyfunc(db, tfun, [t.commands[0].stdout])
    job = q.enqueue(run, t.task_id)
    job2 = q.enqueue(run, t2.task_id, depends_on=job)
    result = wait_for_transformer(job2)
    assert_file(db, result.out[0], b"BLA BLA\n")


def test_multitransformer() -> None:
    """Test transformer capabilities."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)
    t = shell(db, "cat file1", inp={"file1": b"bla bla\n"})

    def tfun(inp: bytes) -> Tuple[bytes, bytes]:
        return inp.decode().upper().encode(), "lol".encode()

    t2 = pyfunc(db, tfun, [t.commands[0].stdout], 2)
    job = q.enqueue(run, t.task_id)
    job = q.enqueue(run, t2.task_id, depends_on=job)
    result = wait_for_transformer(job)
    assert_file(db, result.out[0], b"BLA BLA\n")
    assert_file(db, result.out[1], b"lol")


def test_cached_transformer() -> None:
    """Test transformer cached results."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)

    def tfun(inp: bytes) -> bytes:
        return inp.decode().upper().encode()

    t = shell(db, "cat file1", inp={"file1": b"bla bla\n"})
    t1 = pyfunc(db, tfun, [t.commands[0].stdout])
    job = q.enqueue(run, t.task_id)
    job = q.enqueue(run, t1.task_id, depends_on=job)
    result1 = wait_for_transformer(job)
    assert_file(db, result1.out[0], b"BLA BLA\n")

    t = shell(db, "cat file1", inp={"file1": b"bla bla\n"})
    t2 = pyfunc(db, tfun, [t.commands[0].stdout])
    job = q.enqueue(run, t.task_id)
    job = q.enqueue(run, t2.task_id, depends_on=job)
    result2 = wait_for_transformer(job)
    assert_file(db, result2.out[0], b"BLA BLA\n")
    assert t2.task_id == t1.task_id
    assert result1 == result2


def test_notcached_transformer() -> None:
    """Test transformer uncached."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)

    def tfun(inp: bytes) -> bytes:
        return inp.decode().upper().encode()

    t = shell(db, "cat file1", inp={"file1": b"bla bla\n"})
    t1 = pyfunc(db, tfun, [t.commands[0].stdout])
    job = q.enqueue(run, t.task_id)
    job = q.enqueue(run, t1.task_id, depends_on=job)
    result1 = wait_for_transformer(job)
    assert_file(db, result1.out[0], b"BLA BLA\n")

    # the comment type:ignore is enough to get it to redefine as a new
    # transformer.

    def tfun(inp: bytes) -> bytes:  # type:ignore
        return inp.decode().upper().encode()

    t = shell(db, "cat file1", inp={"file1": b"bla bla\n"})
    t2 = pyfunc(db, tfun, [t.commands[0].stdout])
    job = q.enqueue(run, t.task_id)
    job = q.enqueue(run, t2.task_id, depends_on=job)
    result2 = wait_for_transformer(job)
    assert_file(db, result2.out[0], b"BLA BLA\n")
    assert t2.task_id != t1.task_id
    assert result1 != result2
