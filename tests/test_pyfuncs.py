"""Test of Funsies python functions capabilities."""
# std
from typing import Dict

# module
from funsies import _pyfunc as p
from funsies._constants import DataType


def capitalize(inputs: Dict[str, bytes]) -> Dict[str, bytes]:
    """Capitalize artifacts."""
    out = {}
    for key, val in inputs.items():
        out[key] = val.decode().upper().encode()
    return out


def capitalize2(inputs: Dict[str, bytes]) -> Dict[str, str]:
    """Capitalize artifacts."""
    out = {}
    for key, val in inputs.items():
        out[key] = val.decode().upper()
    return out


def test_fun_wrap() -> None:
    """Test the instantiation of a Funsie class."""
    out = p.python_funsie(
        capitalize, inputs={"in": DataType.blob}, outputs={"in": DataType.blob}
    )
    assert out is not None


def test_fun_run() -> None:
    """Test running python function."""
    cmd = p.python_funsie(
        capitalize, inputs={"in": DataType.blob}, outputs={"in": DataType.blob}
    )
    inp = {"in": b"bla bla bla"}
    out = p.run_python_funsie(cmd, inp)
    assert out["in"] == b"BLA BLA BLA"


def test_fun_run_json() -> None:
    """Test running python function that outputs a JSON."""
    cmd = p.python_funsie(
        capitalize2, inputs={"in": DataType.blob}, outputs={"in": DataType.json}
    )
    inp = {"in": b"bla bla bla"}
    out = p.run_python_funsie(cmd, inp)
    assert out["in"] == "BLA BLA BLA"
