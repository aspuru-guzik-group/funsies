"""Tests for xtb commandline wrapper."""
from iacta import cliwrap


def test_command_echo() -> None:
    """Test running the wrapper."""
    cmd = cliwrap.Command(executable="echo")
    results = cliwrap.run_command(".", None, cmd)
    assert results.returncode == 0
    assert results.stdout == b"\n"
    assert results.stderr == b""
    assert results.exception is None


def test_command_echo2() -> None:
    """Test arguments."""
    cmd = cliwrap.Command(executable="echo", args=["bla", "bla"])
    results = cliwrap.run_command(".", None, cmd)
    assert results.stdout == b"bla bla\n"
    assert results.stderr == b""
    assert results.exception is None


def test_command_environ() -> None:
    """Test environment variable."""
    cmd = cliwrap.Command(executable="env")
    environ = {"VARIABLE": "bla bla"}
    results = cliwrap.run_command(".", environ, cmd)
    assert results.exception is None
    assert results.stdout == b"VARIABLE=bla bla\n"
    assert results.stderr == b""


def test_task_file_in() -> None:
    """Test file inputs."""
    cmd = cliwrap.Command(
        executable="cat",
        args=["file"],
    )
    task = cliwrap.Task([cmd], inputs={"file": b"12345"})
    results = cliwrap.run(task)
    assert results.commands[0].stdout == b"12345"


def test_task_file_out() -> None:
    """Test file outputs."""
    cmd = cliwrap.Command(executable="cp", args=["file", "file2"])
    task = cliwrap.Task(
        [cmd],
        inputs={"file": b"12345"},
        outputs=["file2"],
    )
    results = cliwrap.run(task)

    assert results.outputs["file2"] == b"12345"


def test_task_command_sequence() -> None:
    """Test file outputs."""
    cmd1 = cliwrap.Command(executable="cp", args=["file", "file2"])
    cmd2 = cliwrap.Command(executable="cp", args=["file2", "file3"])
    task = cliwrap.Task(
        [cmd1, cmd2],
        inputs={"file": b"12345"},
        outputs=["file2", "file3"],
    )
    results = cliwrap.run(task)

    assert results.outputs["file3"] == b"12345"
    assert results.outputs["file2"] == b"12345"


def test_cliwrap_file_err() -> None:
    """Test file errors."""
    cmd = cliwrap.Command(executable="cp", args=["file", "file2"])
    task = cliwrap.Task(
        [cmd],
        inputs={"file": b"12345"},
        outputs=["file3"],
    )
    results = cliwrap.run(task)
    # no problem
    assert results.commands[0].exception is None
    # but file not returned
    assert results.outputs == {}


def test_cliwrap_cli_err() -> None:
    """Test file errors."""
    cmd = cliwrap.Command(executable="cp", args=["file2", "file3"])
    task = cliwrap.Task(
        [cmd],
        inputs={"file": b"12345"},
        outputs=["file3"],
    )
    results = cliwrap.run(task)
    assert results.commands[0].exception is None
    assert results.commands[0].returncode > 0
    assert results.commands[0].stderr != b""


def test_cliwrap_nocmd_err() -> None:
    """Test file errors."""
    cmd = cliwrap.Command(executable="a bizarre command name", args=["file", "file2"])
    task = cliwrap.Task(
        [cmd],
        inputs={"file": b"12345"},
        outputs=["file3"],
    )
    results = cliwrap.run(task)
    assert isinstance(results.commands[0].exception, FileNotFoundError)
