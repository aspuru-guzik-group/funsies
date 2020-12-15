"""Tests of user-friendly routines."""
# std
import os
import tempfile

# external
from fakeredis import FakeStrictRedis as Redis

# module
import funsies

db = Redis()


def test_simple_arg_parsing() -> None:
    """Test simple argument parsing."""
    task = funsies.task(db, "echo 123")
    assert len(task.commands) == 1
    assert task.commands[0].executable == "echo"
    assert task.commands[0].args == ["123"]

    task = funsies.task(db, "echo", "echo")
    assert len(task.commands) == 2
    assert task.commands[0].executable == "echo"
    assert task.commands[0].args == []


def test_complex_arg_parsing() -> None:
    """Test less simple argument parsing."""
    task = funsies.task(db, *["echo 123", ["echo", "22", "33"]])
    assert len(task.commands) == 2
    assert task.commands[0].executable == "echo"
    assert task.commands[1].executable == "echo"
    assert task.commands[1].args == ["22", "33"]

    task = funsies.task(db, *[["bla"], ["bla bla", "k"], ["bla"], "bla"])
    assert task.commands[0].executable == "bla"
    assert task.commands[1].executable == "bla bla"
    assert task.commands[1].args == ["k"]


def test_arg_errors() -> None:
    """Test for wrong args for task maker."""
    try:
        _ = funsies.task(db, 3)  # type:ignore
    except Exception as e:
        assert isinstance(e, TypeError)

    try:
        _ = funsies.task(db, "echo", input_files=3)  # type:ignore
    except Exception as e:
        assert isinstance(e, TypeError)

    try:
        _ = funsies.task(db, "echo", output_files=3)  # type:ignore
    except Exception as e:
        assert isinstance(e, TypeError)


def test_files_parsing() -> None:
    """Test input files parsing."""
    with tempfile.NamedTemporaryFile("wb") as f:
        f.write(b"bla bla")
        f.flush()
        task2 = funsies.task(db, "cat file", input_files=[f.name])
        tmp = funsies.pull_file(db, task2.inputs[os.path.basename(f.name)])
        assert tmp is not None
        assert tmp == b"bla bla"


def test_outfiles_parsing() -> None:
    """Test input files parsing."""
    task1 = funsies.task(
        db, "cp file1 file2", input_files={"file1": "bla"}, output_files=["file2"]
    )
    assert tuple(task1.outputs) == ("file2",)
