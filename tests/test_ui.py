"""Test of Funsies shell capabilities."""
# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import _graph, run_op, ui


def test_shell_run() -> None:
    """Test shell command."""
    db = Redis()
    s = ui.shell(db, "cp file1 file2", inp={"file1": "wawa"}, out=["file2"])
    run_op(db, s.hash)
    assert _graph.get_data(db, _graph.get_artefact(db, s.op.inp["file1"])) == b"wawa"
    assert _graph.get_data(db, _graph.get_artefact(db, s.stdout)) == b""
    assert ui.grab(db, s.out["file2"]) == b"wawa"
