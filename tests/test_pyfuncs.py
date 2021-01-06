"""Test of Funsies shell capabilities."""
# std
from typing import Dict

# module
from funsies import _pyfunc as p


def capitalize(inputs: Dict[str, bytes]) -> Dict[str, bytes]:
    """Capitalize artifacts."""
    out = {}
    for key, val in inputs.items():
        out[key] = val.decode().upper().encode()
    return out


def test_fun_wrap() -> None:
    """Test the instantiation of a Funsie class."""
    out = p.python_funsie(capitalize, inputs=["in"], outputs=["out"])
    assert out is not None


def test_fun_run() -> None:
    """Test the instantiation of a Funsie class."""
    cmd = p.python_funsie(capitalize, inputs=["in"], outputs=["out", "in"])
    inp = {"in": b"bla bla bla"}
    out = p.run_python_funsie(cmd, inp)
    assert out["in"] == b"BLA BLA BLA"
    assert out["out"] is None


# def test_shell_run() -> None:
#     """Test the instantiation of a Funsie class."""
#     cmd = s.shell_funsie(["cat file1"], ["file1"], [])
#     inp = {"file1": b"bla bla"}
#     out = s.run_shell_funsie(cmd, inp)
#     assert out[f"{s.STDOUT}0"] == b"bla bla"


# def test_shell_cp() -> None:
#     """Test the instantiation of a Funsie class."""
#     cmd = s.shell_funsie(["cp file1 file2"], ["file1"], ["file2"])
#     inp = {"file1": b"bla bla"}
#     out = s.run_shell_funsie(cmd, inp)
#     assert out[f"{s.STDOUT}0"] == b""
#     assert out["file2"] == b"bla bla"
