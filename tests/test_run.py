"""Test running a funsie."""
# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# module
from funsies import run
from funsies import _graph
from funsies import _shell as s


def test_shell_run() -> None:
    """Test run on a shell command."""
    db = Redis()
    cmd = s.shell_funsie(["cat file1"], ["file1"], [])
    inp = {"file1": _graph.store_explicit_artefact(db, b"bla bla")}
    operation = _graph.make_op(db, cmd, inp)
    status = run.run(db, operation.hash)
    # test return values
    assert status == True
    assert (
        _graph.get_data(db, _graph.get_artefact(db, operation.inp["file1"]))
        == b"bla bla"
    )
    assert (
        _graph.get_data(db, _graph.get_artefact(db, operation.out[f"{s.STDOUT}0"]))
        == b"bla bla"
    )
