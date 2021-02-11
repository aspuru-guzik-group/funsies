"""Test of Funsies 'get'."""
# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import Artefact, Fun, getter, Operation, ui
from funsies._funsies import Funsie


def test_shell_run() -> None:
    """Test shell command."""
    with Fun(Redis()):
        s1 = ui.shell("cp file1 file2", inp={"file1": "wawa"}, out=["file2"])

        dat = getter.get(s1.hash)
        assert isinstance(dat, Operation)
        assert dat == s1.op

        dat = getter.get(s1.inp["file1"].hash)
        assert isinstance(dat, Artefact)
        assert dat == s1.inp["file1"]

        dat = getter.get(s1.op.funsie)
        assert isinstance(dat, Funsie)
