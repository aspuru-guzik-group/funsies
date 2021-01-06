"""Test of Funsies shell capabilities."""
# module
from funsies import _shell as s


def test_shell_wrap() -> None:
    """Test the instantiation of a Funsie class."""
    out = s.shell_funsie(["cat file1"], ["file1"], [])
    assert out is not None


def test_shell_run() -> None:
    """Test the instantiation of a Funsie class."""
    cmd = s.shell_funsie(["cat file1"], ["file1"], [])
    inp = {"file1": b"bla bla"}
    out = s.run_shell_funsie(cmd, inp)
    assert out[f"{s.STDOUT}0"] == b"bla bla"


def test_shell_cp() -> None:
    """Test the instantiation of a Funsie class."""
    cmd = s.shell_funsie(["cp file1 file2"], ["file1"], ["file2"])
    inp = {"file1": b"bla bla"}
    out = s.run_shell_funsie(cmd, inp)
    assert out[f"{s.STDOUT}0"] == b""
    assert out["file2"] == b"bla bla"
