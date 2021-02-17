"""Test of Funsies 'get'."""
# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import Fun, getter, types, ui


def test_get() -> None:
    """Test get."""
    with Fun(Redis()):
        s1 = ui.shell("cp file1 file2", inp={"file1": "wawa"}, out=["file2"])

        data = getter.get(s1.hash)
        assert len(data) == 1
        dat = data[0]
        assert isinstance(dat, types.Operation)
        assert dat == s1.op

        data = getter.get(s1.inp["file1"].hash)
        assert len(data) == 1
        dat = data[0]
        assert isinstance(dat, types.Artefact)
        assert dat == s1.inp["file1"]

        data = getter.get(s1.op.funsie)
        assert len(data) == 1
        dat = data[0]
        assert isinstance(dat, types.Funsie)
