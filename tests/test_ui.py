"""Tests of user-friendly routines."""
# std
import os
import tempfile

# module
import funsies


def test_simple_arg_parsing() -> None:
    """Test simple argument parsing."""
    task = funsies.task("echo 123")
    assert len(task.commands) == 1
    assert task.commands[0].executable == "echo"
    assert task.commands[0].args == ("123",)

    task = funsies.task("echo", "echo")
    assert len(task.commands) == 2
    assert task.commands[0].executable == "echo"
    assert task.commands[0].args == ()


def test_complex_arg_parsing() -> None:
    """Test less simple argument parsing."""
    task = funsies.task(*["echo 123", ["echo", "22", "33"]])
    assert len(task.commands) == 2
    assert task.commands[0].executable == "echo"
    assert task.commands[1].executable == "echo"
    assert task.commands[1].args == ("22", "33")

    task = funsies.task(*[["bla"], ["bla bla", "k"], ["bla"], "bla"])
    assert task.commands[0].executable == "bla"
    assert task.commands[1].executable == "bla bla"
    assert task.commands[1].args == ("k",)


def test_arg_errors() -> None:
    """Test for wrong args for task maker."""
    try:
        _ = funsies.task(3)  # type:ignore
    except Exception as e:
        assert isinstance(e, TypeError)

    try:
        _ = funsies.task("echo", input_files=3)  # type:ignore
    except Exception as e:
        assert isinstance(e, TypeError)

    try:
        _ = funsies.task("echo", output_files=3)  # type:ignore
    except Exception as e:
        assert isinstance(e, TypeError)


def test_files_parsing() -> None:
    """Test input files parsing."""
    with tempfile.NamedTemporaryFile("wb") as f:
        f.write(b"bla bla")
        f.flush()
        name = os.path.basename(f.name)
        task1 = funsies.task("cat file", input_files={name: b"bla bla"})
        task2 = funsies.task("cat file", input_files=[f.name])

    assert task1 == task2


def test_outfiles_parsing() -> None:
    """Test input files parsing."""
    task1 = funsies.task(
        "cp file1 file2", input_files={"file1": "bla"}, output_files=["file2"]
    )
    assert tuple(task1.outputs) == ("file2",)


def test_cache_manager() -> None:
    """Test Cache."""
    cache = funsies.Cache("test")
    task = funsies.task(
        "echo bla bla bla", input_files={"file1": "bla"}, output_files=["file1"]
    )
    out = funsies.run(cache.spec, task)
    result = cache.unwrap_file(out.outputs["file1"])
    assert result == b"bla"

    result = cache.unwrap_command(out.commands[0])
    assert result.stdout == b"bla bla bla\n"
