"""Tests for commandline wrapper."""
# stdlib
import time
from typing import Optional, Tuple

# external
from fakeredis import FakeStrictRedis as Redis
from rq import Queue
from rq.job import Job

# module
from funsies import pull_file, run, task, transformer
from funsies.cached import FilePtr
from funsies.rtask import pull_task, RTask
from funsies.rtransformer import pull_transformer, RTransformer


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
            t = pull_task(job.connection, job.result)
            assert t is not None
            return t
        time.sleep(0.02)


def wait_for_transformer(job: Job) -> RTransformer:
    """Busily wait for a transformer to finish."""
    while True:
        if job.result is not None:
            t = pull_transformer(job.connection, job.result)
            assert t is not None
            return t
        time.sleep(0.02)


def test_task() -> None:
    """Test that a task even runs."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)
    t = task(db, "echo bla bla")
    job = q.enqueue(run, t)
    result = wait_for(job)
    print(task)

    # read
    assert result.commands[0].stdout is not None
    assert result.commands[0].stderr is not None
    assert_file(db, result.commands[0].stdout, b"bla bla\n")
    assert_file(db, result.commands[0].stderr, b"")


def test_task_environ() -> None:
    """Test environment variable."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)
    t = task(db, "env", env={"VARIABLE": "bla bla"})
    result = wait_for(q.enqueue(run, t))
    assert_file(db, result.commands[0].stdout, b"VARIABLE=bla bla\n")
    assert_file(db, result.commands[0].stderr, b"")


def test_task_file_in() -> None:
    """Test environment variable."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)
    t = task(db, ["cat", "i am a file"], inp={"i am a file": b"bla bla\n"})
    result = wait_for(q.enqueue(run, t))
    assert_file(db, result.commands[0].stdout, b"bla bla\n")
    assert_file(db, result.commands[0].stderr, b"")


def test_task_identical_parameters() -> None:
    """Test environment variable."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)
    t = task(db, ["cat", "i am a file"], inp={"i am a file": b"bla bla\n"})
    job = q.enqueue(run, t)
    result = wait_for(job)
    assert_file(db, result.commands[0].stdout, b"bla bla\n")
    assert_file(db, result.commands[0].stderr, b"")
    id1 = job.result

    t = task(db, ["cat", "i am a file"], inp={"i am a file": b"bla bla\n"})
    job = q.enqueue(run, t)
    result = wait_for(job)
    assert_file(db, result.commands[0].stdout, b"bla bla\n")
    assert_file(db, result.commands[0].stderr, b"")
    id2 = job.result
    assert id1 == id2


def test_transformer() -> None:
    """Test transformer capabilities."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)
    t = task(db, ["cat", "i am a file"], inp={"i am a file": b"bla bla\n"})

    def tfun(inp: bytes) -> bytes:
        return inp.decode().upper().encode()

    t2 = transformer(db, tfun, [t.commands[0].stdout], ["whattt"])
    job = q.enqueue(run, t)
    job2 = q.enqueue(run, t2, depends_on=job)
    result = wait_for_transformer(job2)
    assert_file(db, result.outputs[0], b"BLA BLA\n")


def test_multitransformer() -> None:
    """Test transformer capabilities."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)
    t = task(db, ["cat", "i am a file"], inp={"i am a file": b"bla bla\n"})

    def tfun(inp: bytes) -> Tuple[bytes, bytes]:
        return inp.decode().upper().encode(), "lol".encode()

    t2 = transformer(db, tfun, [t.commands[0].stdout], ["whattt", "bruh"])
    job = q.enqueue(run, t)
    job = q.enqueue(run, t2, depends_on=job)
    result = wait_for_transformer(job)
    assert_file(db, result.outputs[0], b"BLA BLA\n")
    assert_file(db, result.outputs[1], b"lol")


def test_cached_transformer() -> None:
    """Test transformer capabilities."""
    db = Redis()
    q = Queue(connection=db, is_async=False, default_timeout=-1)

    def tfun(inp: bytes) -> bytes:
        return inp.decode().upper().encode()

    t = task(db, ["cat", "i am a file"], inp={"i am a file": b"bla bla\n"})
    t2 = transformer(db, tfun, [t.commands[0].stdout], ["whattt"])
    job = q.enqueue(run, t)
    job = q.enqueue(run, t2, depends_on=job)
    result = wait_for_transformer(job)
    assert_file(db, result.outputs[0], b"BLA BLA\n")

    t = task(db, ["cat", "i am a file"], inp={"i am a file": b"bla bla\n"})
    t2 = transformer(db, tfun, [t.commands[0].stdout], ["whattt"])
    job = q.enqueue(run, t)
    job = q.enqueue(run, t2, depends_on=job)
    result = wait_for_transformer(job)
    assert_file(db, result.outputs[0], b"BLA BLA\n")
