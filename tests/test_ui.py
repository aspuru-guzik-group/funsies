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
    task = funsies.shell(db, "echo 123")
    assert len(task.commands) == 1
    assert task.commands[0].executable == "echo"
    assert task.commands[0].args == ["123"]

    task = funsies.shell(db, "echo", "echo")
    assert len(task.commands) == 2
    assert task.commands[0].executable == "echo"
    assert task.commands[0].args == []


def test_complex_arg_parsing() -> None:
    """Test less simple argument parsing."""
    task = funsies.shell(db, *["echo 123", ["echo", "22", "33"]])
    assert len(task.commands) == 2
    assert task.commands[0].executable == "echo"
    assert task.commands[1].executable == "echo"
    assert task.commands[1].args == ["22", "33"]

    task = funsies.shell(db, *[["bla"], ["bla bla", "k"], ["bla"], "bla"])
    assert task.commands[0].executable == "bla"
    assert task.commands[1].executable == "bla bla"
    assert task.commands[1].args == ["k"]


def test_arg_errors() -> None:
    """Test for wrong args for task maker."""
    try:
        _ = funsies.shell(db, 3)  # type:ignore
    except Exception as e:
        assert isinstance(e, TypeError)

    try:
        _ = funsies.shell(db, "echo", inp=3)  # type:ignore
    except Exception as e:
        assert isinstance(e, TypeError)

    try:
        _ = funsies.shell(db, "echo", out=3)  # type:ignore
    except Exception as e:
        assert isinstance(e, TypeError)


def test_files_parsing() -> None:
    """Test input files parsing."""
    with tempfile.NamedTemporaryFile("wb") as f:
        f.write(b"bla bla")
        f.flush()
        task2 = funsies.shell(db, "cat file", inp=[f.name])
        tmp = funsies.pull_file(db, task2.inp[os.path.basename(f.name)])
        assert tmp is not None
        assert tmp == b"bla bla"


def test_outfiles_parsing() -> None:
    """Test input files parsing."""
    task1 = funsies.shell(db, "cp file1 file2", inp={"file1": "bla"}, out=["file2"])
    assert tuple(task1.out) == ("file2",)


# def test_concat() -> None:
#     """Test 'concat' functionality."""
#     db = Redis()
#     q = Queue(connection=db, is_async=False, default_timeout=-1)
#     t1 = funsies.task(db, ["cat", "i am a file"], inp={"i am a file": b"bla bla\n"})
#     t2 = funsies.task(db, ["echo", "lololol"])
#     t3 = funsies.concat(
#         db, [t1.commands[0].stdout, t2.commands[0].stdout, t1.inp["i am a file"]]
#     )

#     funsies.runall(q, t3)
