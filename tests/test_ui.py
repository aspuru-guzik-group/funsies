"""Test of Funsies shell capabilities."""
# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import ui, run, _graph


def test_shell_wrap() -> None:
    """Test shell command."""
    db = Redis()
    s = ui.shell(db, "cp file1 file2", inp={"file1": "wawa"}, out=["file2"])
    run.run(db, s.hash)
    assert _graph.get_data(db, _graph.get_artefact(db, s.inp["file1"])) == b"wawa"
    assert _graph.get_data(db, _graph.get_artefact(db, s.out["stdout:0"])) == b""
    assert _graph.get_data(db, _graph.get_artefact(db, s.out["file2"])) == b"wawa"


test_shell_wrap()
