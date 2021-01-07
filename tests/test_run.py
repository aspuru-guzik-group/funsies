"""Test running a funsie."""
from typing import Dict

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import _graph
from funsies import _pyfunc as p
from funsies import _shell as s
from funsies import run_op, RunStatus


def capitalize(inputs: Dict[str, bytes]) -> Dict[str, bytes]:
    """Capitalize artifacts."""
    out = {}
    for key, val in inputs.items():
        out[key] = val.decode().upper().encode()
    return out


def uncapitalize(inputs: Dict[str, bytes]) -> Dict[str, bytes]:
    """Uncapitalize artifacts."""
    out = {}
    for key, val in inputs.items():
        out[key] = val.decode().lower().encode()
    return out


def test_shell_run() -> None:
    """Test run on a shell command."""
    db = Redis()
    cmd = s.shell_funsie(["cat file1"], ["file1"], [])
    inp = {"file1": _graph.store_explicit_artefact(db, b"bla bla")}
    operation = _graph.make_op(db, cmd, inp)
    status = run_op(db, operation.hash)

    # test return values
    assert status == RunStatus.executed

    # check data is good
    dat = _graph.get_data(db, _graph.get_artefact(db, operation.inp["file1"]))
    assert dat == b"bla bla"

    dat = _graph.get_data(db, _graph.get_artefact(db, operation.out[f"{s.STDOUT}0"]))
    assert dat == b"bla bla"


def test_pyfunc_run() -> None:
    """Test run on a python function."""
    db = Redis()
    cmd = p.python_funsie(capitalize, ["inp"], ["inp"], name="capit")
    inp = {"inp": _graph.store_explicit_artefact(db, b"bla bla")}
    operation = _graph.make_op(db, cmd, inp)
    status = run_op(db, operation.hash)

    # test return values
    assert status == RunStatus.executed

    # check data is good
    dat = _graph.get_data(db, _graph.get_artefact(db, operation.inp["inp"]))
    assert dat == b"bla bla"

    dat = _graph.get_data(db, _graph.get_artefact(db, operation.out["inp"]))
    assert dat == b"BLA BLA"


def test_cached_run() -> None:
    """Test cached result."""
    db = Redis()
    cmd = p.python_funsie(capitalize, ["inp"], ["inp"], name="capit")
    inp = {"inp": _graph.store_explicit_artefact(db, b"bla bla")}
    operation = _graph.make_op(db, cmd, inp)
    status = run_op(db, operation.hash)

    # test return values
    assert status == RunStatus.executed
    status = run_op(db, operation.hash)
    assert status == RunStatus.using_cached


def test_dependencies() -> None:
    """Test cached result."""
    db = Redis()
    cmd = p.python_funsie(capitalize, ["inp"], ["inp"])
    cmd2 = p.python_funsie(uncapitalize, ["inp"], ["inp"])
    operation = _graph.make_op(
        db, cmd, inp={"inp": _graph.store_explicit_artefact(db, b"bla bla")}
    )
    operation2 = _graph.make_op(
        db, cmd2, inp={"inp": _graph.get_artefact(db, operation.out["inp"])}
    )

    status = run_op(db, operation2.hash)
    assert status == RunStatus.unmet_dependencies

    status = run_op(db, operation.hash)
    assert status == RunStatus.executed

    status = run_op(db, operation2.hash)
    assert status == RunStatus.executed