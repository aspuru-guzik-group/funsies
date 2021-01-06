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
    assert _graph.get_data(db, s.inp["file1"]) == b"wawa"
    assert _graph.get_data(db, s.stdout) == b""
    assert ui.take(db, s.out["file2"]) == b"wawa"


def test_store_cache() -> None:
    """Test store for caching."""
    db = Redis()
    s = ui.put(db, "bla bla")
    s2 = ui.put(db, b"bla bla")
    assert s == s2
    assert ui.take(db, s) == b"bla bla"


def test_morph() -> None:
    """Test store for caching."""
    db = Redis()
    dat = ui.put(db, "bla bla")
    morph = ui.morph(db, lambda x: x.decode().upper().encode(), dat)
    run_op(db, morph.parent)
    assert ui.take(db, morph) == b"BLA BLA"


def test_reduce() -> None:
    """Test store for caching."""
    db = Redis()
    dat = ui.put(db, "bla bla")
    morph = ui.morph(db, lambda x: x.decode().upper().encode(), dat)

    def join(x: bytes, y: bytes) -> bytes:
        return x + y

    red = ui.reduce(db, join, morph, dat)

    run_op(db, morph.parent)
    run_op(db, red.parent)
    assert ui.take(db, red) == b"BLA BLAbla bla"
