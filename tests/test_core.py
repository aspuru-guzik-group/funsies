"""Tests for commandline wrapper."""
from funsies import Command, Context, run, run_command, Task


def test_command_echo() -> None:
    """Test running the wrapper."""
    cmd = Command(executable="echo")
    results = run_command(".", None, cmd)
    assert results.returncode == 0
    assert results.stdout == b"\n"
    assert results.stderr == b""
    assert results.exception is None


def test_command_echo2() -> None:
    """Test arguments."""
    cmd = Command(executable="echo", args=["bla", "bla"])
    results = run_command(".", None, cmd)
    assert results.stdout == b"bla bla\n"
    assert results.stderr == b""
    assert results.exception is None


def test_command_environ() -> None:
    """Test environment variable."""
    cmd = Command(executable="env")
    environ = {"VARIABLE": "bla bla"}
    results = run_command(".", environ, cmd)
    assert results.exception is None
    assert results.stdout == b"VARIABLE=bla bla\n"
    assert results.stderr == b""


def test_task_environ() -> None:
    """Test environment variable."""
    cmd = Command(executable="env")
    task = Task([cmd], env={"VARIABLE": "bla bla"})
    results = run(task)
    assert results.commands[0].stdout == b"VARIABLE=bla bla\n"
    assert results.commands[0].stderr == b""


def test_task_file_in() -> None:
    """Test file inputs."""
    cmd = Command(
        executable="cat",
        args=["file"],
    )
    task = Task([cmd], inputs={"file": b"12345"})
    results = run(task)
    assert results.commands[0].stdout == b"12345"


def test_task_file_out() -> None:
    """Test file outputs."""
    cmd = Command(executable="cp", args=["file", "file2"])
    task = Task(
        [cmd],
        inputs={"file": b"12345"},
        outputs=["file2"],
    )
    results = run(task)

    assert results.outputs["file2"] == b"12345"


def test_task_command_sequence() -> None:
    """Test file outputs."""
    cmd1 = Command(executable="cp", args=["file", "file2"])
    cmd2 = Command(executable="cp", args=["file2", "file3"])
    task = Task(
        [cmd1, cmd2],
        inputs={"file": b"12345"},
        outputs=["file2", "file3"],
    )
    results = run(task)

    assert results.outputs["file3"] == b"12345"
    assert results.outputs["file2"] == b"12345"


def test_cliwrap_file_err() -> None:
    """Test file errors."""
    cmd = Command(executable="cp", args=["file", "file2"])
    task = Task(
        [cmd],
        inputs={"file": b"12345"},
        outputs=["file3"],
    )
    results = run(task)
    # no problem
    assert results.commands[0].exception is None
    # but file not returned
    assert results.outputs == {}


def test_cliwrap_cli_err() -> None:
    """Test file errors."""
    cmd = Command(executable="cp", args=["file2", "file3"])
    task = Task(
        [cmd],
        inputs={"file": b"12345"},
        outputs=["file3"],
    )
    results = run(task)
    assert results.commands[0].exception is None
    assert results.commands[0].returncode > 0
    assert results.commands[0].stderr != b""


def test_cliwrap_nocmd_err() -> None:
    """Test file errors."""
    cmd = Command(executable="a bizarre command name", args=["file", "file2"])
    task = Task(
        [cmd],
        inputs={"file": b"12345"},
        outputs=["file3"],
    )
    results = run(task)
    assert isinstance(results.commands[0].exception, FileNotFoundError)


def test_no_dask() -> None:
    """Test functionality when dask is not present."""
    # set dask to "unavailable"
    import funsies.core as c

    old_DASK_AVAIL = c.__DASK_AVAIL
    c.__DASK_AVAIL = False

    cmd = Command(
        executable="cat",
        args=["file"],
    )
    task = Task([cmd], inputs={"file": b"12345"})
    results = run(task)
    assert results.commands[0].stdout == b"12345"

    # reset for future tests
    c.__DASK_AVAIL = old_DASK_AVAIL


def test_context() -> None:
    """Test context (without caching)."""
    cmd = Command(
        executable="cat",
        args=["file"],
    )
    task = Task([cmd], inputs={"file": b"12345"})
    context = Context(tmpdir=".")
    results = run(task, context)
    assert results.commands[0].stdout == b"12345"
