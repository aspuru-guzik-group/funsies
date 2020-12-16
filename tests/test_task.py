"""Tests for commandline wrapper."""
# stdlib
from io import StringIO
import time
from typing import Optional

# external
from fakeredis import FakeStrictRedis as Redis
from rq import Queue
from rq.job import Job

# module
from funsies import pull_file, run, task, transformer
from funsies.cached import CachedFile
from funsies.rtask import pull_task, RTask
from funsies.rtransformer import pull_transformer


def assert_file(db: Redis, fid: Optional[CachedFile], equals: bytes) -> None:
    """Assert that a file exists and is equal to something."""
    assert fid is not None
    out = pull_file(db, fid)
    assert out is not None
    assert out == equals


# Make a fake connection
db = Redis()
q = Queue(connection=db, is_async=False, default_timeout=-1)


def wait_for(job: Job) -> RTask:
    """Busily wait for a job to finish."""
    while True:
        if job.result is not None:
            t = pull_task(job.connection, job.result)
            assert t is not None
            return t
        time.sleep(0.02)


def wait_for_transformer(job: Job) -> RTask:
    """Busily wait for a transformer to finish."""
    while True:
        if job.result is not None:
            t = pull_transformer(job.connection, job.result)
            assert t is not None
            return t
        time.sleep(0.02)


def test_task() -> None:
    """Test that a task even runs."""
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
    t = task(db, "env", env={"VARIABLE": "bla bla"})
    result = wait_for(q.enqueue(run, t))
    assert_file(db, result.commands[0].stdout, b"VARIABLE=bla bla\n")
    assert_file(db, result.commands[0].stderr, b"")


def test_task_file_in() -> None:
    """Test environment variable."""
    t = task(db, ["cat", "i am a file"], inp={"i am a file": b"bla bla\n"})
    result = wait_for(q.enqueue(run, t))
    assert_file(db, result.commands[0].stdout, b"bla bla\n")
    assert_file(db, result.commands[0].stderr, b"")


def test_task_identical_parameters() -> None:
    """Test environment variable."""
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
    t = task(db, ["cat", "i am a file"], inp={"i am a file": b"bla bla\n"})

    def tfun(inp: StringIO, out: StringIO) -> None:
        out.write(inp.read().upper())

    t2 = transformer(db, tfun, [t.commands[0].stdout], ["whattt"])
    job = q.enqueue(run, t)
    job = q.enqueue(run, t2, depends_on=job)
    result = wait_for_transformer(job)
    assert_file(db, result.outputs[0], b"BLA BLA\n")


def test_cached_transformer() -> None:
    """Test transformer capabilities."""

    def tfun(inp: StringIO, out: StringIO) -> None:
        out.write(inp.read().upper())

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


# def test_task_file_inout() -> None:
#     """Test file input/output."""
#     cmd = Command(
#         executable="cp",
#         args=["file", "file2"],
#     )
#     with tempfile.TemporaryDirectory() as d:
#         cache_id = CacheSpec(d)
#         task = Task([cmd], {"file": b"12345\n"}, ["file2"])
#         results = run(cache_id, task)

#         # read
#         cache = open_cache(cache_id)
#         assert_file(get_file(cache, results.outputs["file2"]), b"12345\n")


# def test_task_command_sequence() -> None:
#     """Test file outputs."""
#     cmd1 = Command(executable="cp", args=["file", "file2"])
#     cmd2 = Command(executable="cp", args=["file2", "file3"])

#     with tempfile.TemporaryDirectory() as d:
#         cache_id = CacheSpec(d)
#         task = Task(
#             [cmd1, cmd2],
#             inputs={"file": b"12345"},
#             outputs=["file2", "file3"],
#         )
#         results = run(cache_id, task)

#         # read
#         cache = open_cache(cache_id)
#         assert_file(get_file(cache, results.outputs["file3"]), b"12345")


# def test_cliwrap_file_err() -> None:
#     """Test file errors."""
#     cmd = Command(executable="cp", args=["file", "file2"])
#     with tempfile.TemporaryDirectory() as d:
#         cache_id = CacheSpec(d)
#         task = Task(
#             [cmd],
#             inputs={"file": b"12345"},
#             outputs=["file3"],
#         )
#         results = run(cache_id, task)

#         # no problem
#         assert results.commands[0].raises is None

#         # but file not returned
#         assert results.outputs == {}


# def test_cliwrap_cli_err() -> None:
#     """Test command error."""
#     cmd = Command(executable="cp", args=["file2", "file3"])
#     with tempfile.TemporaryDirectory() as d:
#         cache_id = CacheSpec(d)
#         task = Task(
#             [cmd],
#             inputs={"file": b"12345"},
#         )
#         results = run(cache_id, task)

#         assert results.commands[0].raises is None
#         assert results.commands[0].returncode > 0
#         assert results.commands[0].stdout is not None
#         assert results.commands[0].stderr is not None
#         f = get_file(open_cache(cache_id), results.commands[0].stderr)
#         assert f is not None
#         assert f != b""


# def test_command_err() -> None:
#     """Test command error."""
#     cmd = Command(executable="what is this", args=["file2", "file3"])
#     with tempfile.TemporaryDirectory() as d:
#         cache_id = CacheSpec(d)
#         task = Task(
#             [cmd],
#             inputs={"file": b"12345"},
#         )
#         results = run(cache_id, task)

#         assert results.commands[0].raises is not None
