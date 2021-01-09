"""Test of Funsies shell capabilities."""
# std
import tempfile

# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# module
from funsies import _graph, Fun, run_op, ui


def test_shell_run() -> None:
    """Test shell command."""
    with Fun(Redis()) as db:
        s = ui.shell("cp file1 file2", inp={"file1": "wawa"}, out=["file2"])
        run_op(db, s.hash)
        assert _graph.get_data(db, s.stderr) == b""
        assert _graph.get_data(db, s.returncode) == b"0"
        assert _graph.get_data(db, s.inp["file1"]) == b"wawa"
        assert _graph.get_data(db, s.stdout) == b""
        assert ui.take(s.out["file2"]) == b"wawa"


def test_shell_run2() -> None:
    """Test shell command output side cases."""
    with Fun(Redis()) as db:
        s = ui.shell("cp file1 file2", "cat file2", inp={"file1": b"wawa"})
        run_op(db, s.hash)
        assert _graph.get_data(db, s.inp["file1"]) == b"wawa"
        with pytest.raises(Exception):
            _graph.get_data(db, s.stdout)
        with pytest.raises(Exception):
            _graph.get_data(db, s.stderr)
        with pytest.raises(Exception):
            _graph.get_data(db, s.returncode)

        assert ui.take(s.stdouts[1]) == b"wawa"


def test_store_cache() -> None:
    """Test store for caching."""
    with Fun(Redis()):
        s = ui.put("bla bla")
        s2 = ui.put(b"bla bla")
        assert s == s2
        assert ui.take(s) == b"bla bla"


def test_morph() -> None:
    """Test store for caching."""
    with Fun(Redis()) as db:
        dat = ui.put("bla bla")
        morph = ui.morph(lambda x: x.decode().upper().encode(), dat)
        run_op(db, morph.parent)
        assert ui.take(morph) == b"BLA BLA"


def test_reduce() -> None:
    """Test store for caching."""
    with Fun(Redis()) as db:
        dat = ui.put("bla bla")
        morph = ui.morph(lambda x: x.decode().upper().encode(), dat)

        def join(x: bytes, y: bytes) -> bytes:
            return x + y

        red = ui.reduce(join, morph, dat)

        run_op(db, morph.parent)
        run_op(db, red.parent)
        assert ui.take(red) == b"BLA BLAbla bla"


def test_multi_reduce() -> None:
    """Test store for caching."""
    with Fun(Redis()) as db:
        dat = ui.put("bla bla")
        morph = ui.morph(lambda x: x.decode().upper().encode(), dat)

        def join(*x: bytes) -> bytes:
            out = b""
            for el in x:
                out += el
            return out

        red = ui.reduce(join, morph, dat, b"|wat")

        run_op(db, morph.parent)
        run_op(db, red.parent)
        assert ui.take(red) == b"BLA BLAbla bla|wat"


def test_store_takeout() -> None:
    """Test store for caching."""
    with Fun(Redis()):
        s = ui.put("bla bla")
        with tempfile.NamedTemporaryFile() as f:
            ui.takeout(s, f.name)
            with open(f.name, "rb") as f2:
                assert f2.read() == b"bla bla"
