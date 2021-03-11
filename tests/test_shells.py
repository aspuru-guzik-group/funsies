"""Test of Funsies shell capabilities."""
# std
import os

# funsies
from funsies import _shell as s
from funsies.types import Encoding


def test_shell_wrap() -> None:
    """Test the instantiation of a shell Funsie."""
    out = s.shell_funsie(["cat file1"], {"file1": Encoding.blob}, [])
    assert out is not None


def test_shell_run() -> None:
    """Test runnign shell commands."""
    cmd = s.shell_funsie(["cat file1"], {"file1": Encoding.blob}, [])
    inp = {"file1": b"bla bla"}
    out = s.run_shell_funsie(cmd, inp)
    assert out[f"{s.STDOUT}0"] == b"bla bla"


def test_shell_cp() -> None:
    """Test runnign shell commands."""
    cmd = s.shell_funsie(["cp file1 file2"], {"file1": Encoding.json}, ["file2"])
    inp = {"file1": b"bla bla"}
    out = s.run_shell_funsie(cmd, inp)
    assert out[f"{s.STDOUT}0"] == b""
    assert out["file2"] == b"bla bla"


def test_shell_env() -> None:
    """Test env variables in shell funsie."""
    cmd = s.shell_funsie(["echo $VARIABLE"], {}, [], env={"VARIABLE": "bla"})
    out = s.run_shell_funsie(cmd, {})
    assert out[f"{s.STDOUT}0"] == b"bla\n"

    # Check that env variables don't propagate to other commands
    cmd = s.shell_funsie(["echo $VARIABLE"], {}, [])
    out = s.run_shell_funsie(cmd, {})
    assert out[f"{s.STDOUT}0"] == b"\n"

    k = "PATH"
    v = os.environ[k]

    cmd = s.shell_funsie([f"echo ${k}"], {}, [])
    out = s.run_shell_funsie(cmd, {})
    assert out[f"{s.STDOUT}0"] == f"{v}\n".encode()

    cmd = s.shell_funsie([f"echo ${k} $VAR"], {}, [], {"VAR": "bla"})
    out = s.run_shell_funsie(cmd, {})
    assert out[f"{s.STDOUT}0"] == f"{v} bla\n".encode()
