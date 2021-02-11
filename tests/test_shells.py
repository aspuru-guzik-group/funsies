"""Test of Funsies shell capabilities."""
# std
import os

# module
from funsies import _shell as s


def test_shell_wrap() -> None:
    """Test the instantiation of a shell Funsie."""
    out = s.shell_funsie(["cat file1"], ["file1"], [])
    assert out is not None


def test_shell_run() -> None:
    """Test runnign shell commands."""
    cmd = s.shell_funsie(["cat file1"], ["file1"], [])
    inp = {"file1": b"bla bla"}
    out = s.run_shell_funsie(cmd, inp)
    assert out[f"{s.STDOUT}0"] == b"bla bla"


def test_shell_cp() -> None:
    """Test runnign shell commands."""
    cmd = s.shell_funsie(["cp file1 file2"], ["file1"], ["file2"])
    inp = {"file1": b"bla bla"}
    out = s.run_shell_funsie(cmd, inp)
    assert out[f"{s.STDOUT}0"] == b""
    assert out["file2"] == b"bla bla"


def test_shell_env() -> None:
    """Test env variables in shell funsie."""
    cmd = s.shell_funsie(["echo $VARIABLE"], [], [], env={"VARIABLE": "bla"})
    out = s.run_shell_funsie(cmd, {})
    assert out[f"{s.STDOUT}0"] == b"bla\n"

    # Check that env variables don't propagate to other commands
    cmd = s.shell_funsie(["echo $VARIABLE"], [], [])
    out = s.run_shell_funsie(cmd, {})
    assert out[f"{s.STDOUT}0"] == b"\n"

    # Check that other env variables are not erased
    for key, value in os.environ.items():
        break
    cmd = s.shell_funsie([f"echo ${key}"], [], [])
    out = s.run_shell_funsie(cmd, {})
    assert out[f"{s.STDOUT}0"] == f"{value}\n".encode()

    cmd = s.shell_funsie([f"echo ${key} $VAR"], [], [], {"VAR": "bla"})
    out = s.run_shell_funsie(cmd, {})
    assert out[f"{s.STDOUT}0"] == f"{value} bla\n".encode()
