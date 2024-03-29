"""Test of Funsies 'get'."""

# funsies
from funsies import _getter, Fun, types, ui
from funsies.config import MockServer


def test_get() -> None:
    """Test get."""
    with Fun(MockServer()):
        s1 = ui.shell("cp file1 file2", inp={"file1": "wawa"}, out=["file2"])

        data = _getter.get(s1.hash)
        assert len(data) == 1
        dat = data[0]
        assert isinstance(dat, types.Operation)
        assert dat == s1.op

        data = _getter.get(s1.inp["file1"].hash)
        assert len(data) == 1
        dat = data[0]
        assert isinstance(dat, types.Artefact)
        assert dat == s1.inp["file1"]

        data = _getter.get(s1.op.funsie)
        assert len(data) == 1
        dat = data[0]
        assert isinstance(dat, types.Funsie)
