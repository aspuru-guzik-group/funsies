"""Tests for commandline wrapper."""
# module
from funsies import Command, run_command


# test of just simple command runner
def test_command_echo() -> None:
    """Test running the wrapper."""
    cmd = Command(executable="echo")
    results = run_command(".", None, cmd)
    assert results.returncode == 0
    assert results.stdout == b"\n"
    assert results.stderr == b""
    assert results.raises is None


def test_command_echo2() -> None:
    """Test arguments."""
    cmd = Command(executable="echo", args=["bla", "bla"])
    results = run_command(".", None, cmd)
    assert results.stdout == b"bla bla\n"
    assert results.stderr == b""
    assert results.raises is None


def test_command_environ() -> None:
    """Test environment variable."""
    cmd = Command(executable="env")
    environ = {"VARIABLE": "bla bla"}
    results = run_command(".", environ, cmd)
    assert results.raises is None
    assert results.stdout == b"VARIABLE=bla bla\n"
    assert results.stderr == b""
