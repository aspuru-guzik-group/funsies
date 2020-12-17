"""Tests for task dependencies."""
# stdlib
import time
from typing import Optional, Union

# external
# from fakeredis import FakeStrictRedis as Redis
from redis import Redis
from rq import Queue
from rq.job import Job

# module
from funsies import pull, pull_file, runall, task, transformer
from funsies.core import get_dependencies
from funsies.types import FilePtr, RTask, RTransformer


# utils
def assert_file(db: Redis, fid: Optional[FilePtr], equals: bytes) -> None:
    """Assert that a file exists and is equal to something."""
    assert fid is not None
    out = pull_file(db, fid)
    assert out is not None
    assert out == equals


def wait_for(job: Job) -> Union[RTask, RTransformer, FilePtr]:
    """Busily wait for a job to finish."""
    while True:
        if job.result is not None:
            t = pull(job.connection, job.result)
            assert t is not None
            return t
        time.sleep(0.02)


def test_dependents() -> None:
    """Test that a task can find dependents properly."""
    db = Redis()
    t = task(db, "cat file", inp={"file": "blabla"})

    # depends on file
    depends = get_dependencies(db, t.task_id)
    assert len(depends) == 1

    # no dependencies
    depends = get_dependencies(db, depends[0])
    assert depends == []


def test_dependents_complex() -> None:
    """Test that a task can find dependents properly."""

    def do_something(x: bytes) -> bytes:
        return x

    db = Redis()
    t = task(db, "cat file", inp={"file": "blabla"}, out=["lol"])
    t2 = transformer(db, do_something, inp={t.commands[0].stdout})
    t3 = task(
        db,
        "cat file",
        inp={
            "bla": b"whatsup",
            "file": t2.outputs[0],
            "something else": t.outputs["lol"],
        },
    )

    # depends on file
    depends = get_dependencies(db, t3.task_id)
    print(depends)


def test_dag_execution() -> None:
    """Test that a simple DAG runs."""
    db = Redis()
    q = Queue(connection=db, default_timeout=-1, is_async=True)
    t = task(db, "cat file", inp={"file": b"1bla bla\n"})
    tr = transformer(
        db, lambda x: x.decode().upper().encode(), inp=[t.commands[0].stdout]
    )

    tr2 = transformer(db, lambda x, y: x + y, inp=[t.commands[0].stdout, tr.outputs[0]])
    job = runall(db, q, tr2.task_id)
    print("deferred:", q.deferred_job_registry.get_job_ids())

    # q.enqueue_job(job)

    # read
    result = wait_for(job)
    assert_file(db, result.outputs[0], b"1BLA BLA\n")
