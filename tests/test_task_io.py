"""Tests for commandline wrapper."""
from fakeredis import FakeStrictRedis as Redis
from rq import Queue

# module
from funsies import run
from funsies.cached import CachedFile
from funsies.command import Command
from funsies.rtask import pull_task, register, UnregisteredTask


def test_task_serialization() -> None:
    """Test that a task serializes ok."""
    cmd = Command(executable="echo", args=["bla", "bla"])

    task = UnregisteredTask([cmd])
    print(task.json())


def test_task_deserialization() -> None:
    """Test that a task deserializes ok."""
    cmd = Command(executable="echo", args=["bla", "bla"])
    task = UnregisteredTask([cmd, cmd], inputs={"what": CachedFile("bla")})
    print(task.json())

    task2 = UnregisteredTask.from_json(task.json())
    print(task2)

    # same classes
    assert task == task2
    # same serialization
    assert task.json() == task.json()


def test_taskoutput_serialization() -> None:
    """Test that a task even runs."""
    db = Redis()
    cmd = Command(executable="echo", args=["bla", "bla"])
    q = Queue(connection=db, is_async=False, default_timeout=-1)

    task = register(db, UnregisteredTask([cmd]))
    job = q.enqueue(run, task)
    result = pull_task(db, job.result)
    assert result is not None
    print(task)

    # read
    assert result.commands[0].stdout is not None
    assert result.commands[0].stderr is not None
