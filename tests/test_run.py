"""Test running a funsie."""
# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import _graph
from funsies import _shell as s
from funsies import run_op


def test_shell_run() -> None:
    """Test run on a shell command."""
    db = Redis()
    cmd = s.shell_funsie(["cat file1"], ["file1"], [])
    inp = {"file1": _graph.store_explicit_artefact(db, b"bla bla")}
    operation = _graph.make_op(db, cmd, inp)
    status = run_op(db, operation.hash)
    # test return values
    assert status

    # check data is good
    dat = _graph.get_data(db, _graph.get_artefact(db, operation.inp["file1"]))
    assert dat == b"bla bla"

    dat = _graph.get_data(db, _graph.get_artefact(db, operation.out[f"{s.STDOUT}0"]))
    assert dat == b"bla bla"
