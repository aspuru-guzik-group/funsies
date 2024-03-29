"""Test of Funsies user routines."""
# std
import json
import tempfile
from typing import Tuple

# external
import pytest

# funsies
from funsies import _context, _graph, fp, Fun, options, ui
from funsies._run import run_op
from funsies.config import MockServer
from funsies.types import Encoding, UnwrapError


def test_shell_run() -> None:
    """Test shell command."""
    with Fun(MockServer()):
        db, store = _context.get_connection()
        s = ui.shell("cp file1 file2", inp={"file1": b"wawa"}, out=["file2"])
        run_op(db, store, s.hash)
        assert _graph.get_data(db, store, s.stderr) == b""
        assert _graph.get_data(db, store, s.returncode) == 0
        assert _graph.get_data(db, store, s.inp["file1"]) == b"wawa"
        assert _graph.get_data(db, store, s.stdout) == b""
        assert ui.take(s.out["file2"]) == b"wawa"


def test_shell_run2() -> None:
    """Test shell command output side cases."""
    with Fun(MockServer()):
        db, store = _context.get_connection()
        s = ui.shell("cp file1 file2", "cat file2", inp={"file1": b"wawa"})
        run_op(db, store, s.hash)
        assert _graph.get_data(db, store, s.inp["file1"]) == b"wawa"
        with pytest.raises(Exception):
            _graph.get_data(db, store, s.stdout)
        with pytest.raises(Exception):
            _graph.get_data(db, store, s.stderr)
        with pytest.raises(Exception):
            _graph.get_data(db, store, s.returncode)

        assert ui.take(s.stdouts[1]) == b"wawa"


def test_store_cache() -> None:
    """Test store for caching."""
    with Fun(MockServer()):
        s = ui.put("bla bla")
        s2 = ui.put(b"bla bla")
        assert s != s2  # type:ignore
        assert ui.take(s) == "bla bla"
        assert ui.take(s2) == b"bla bla"


def test_store_cache_jsons() -> None:
    """Test store for caching."""
    with Fun(MockServer()):
        # more complex data types
        li = ui.put([1, 2, 3])
        assert ui.take(li) == [1, 2, 3]

        di = ui.put({"fun": 3})
        assert ui.take(di) == {"fun": 3}


def test_rm() -> None:
    """Test rm."""
    with Fun(MockServer(), options(distributed=False)):
        dat = ui.put("bla bla")
        # removing const artefact raises
        with pytest.raises(AttributeError):
            ui.reset(dat)
        ui.take(dat)

        def upper(x: str) -> str:
            return x.upper()

        m1 = fp.morph(upper, dat)
        m2 = fp.morph(lambda x: x + x, m1)
        ui.execute(m2)
        assert ui.take(m2) == "BLA BLABLA BLA"

        ui.reset(m1)
        with pytest.raises(UnwrapError):
            # deletion works
            ui.take(m1)

        with pytest.raises(UnwrapError):
            # and it's recursive
            ui.take(m2)

        # re run
        ui.execute(m2)
        assert ui.take(m2) == "BLA BLABLA BLA"


def test_morph() -> None:
    """Test store for caching."""
    with Fun(MockServer()):
        db, store = _context.get_connection()
        dat = ui.put(b"bla bla")
        morph = fp.morph(lambda x: x.decode().upper().encode(), dat)
        run_op(db, store, morph.parent)
        assert ui.take(morph) == b"BLA BLA"

        dat = ui.put("bla bla")
        morph = fp.morph(lambda x: x.upper(), dat, name="CAPITALIZE_THIS")
        run_op(db, store, morph.parent)
        assert ui.take(morph) == "BLA BLA"


def test_py() -> None:
    """Test multiple output py()."""
    with Fun(MockServer()):
        db, store = _context.get_connection()
        dat = ui.put("Bla Bla")

        def fun(a: str) -> Tuple[str, str]:
            return a.upper(), a.lower()

        x1, x2 = fp.py(
            fun,
            dat,
            out=[Encoding.json, Encoding.json],
            strict=True,
        )
        run_op(db, store, x1.parent)
        assert ui.take(x1) == "BLA BLA"
        assert ui.take(x2) == "bla bla"


def test_reduce() -> None:
    """Test store for caching."""
    with Fun(MockServer()):
        db, store = _context.get_connection()
        dat = ui.put("bla bla")
        morph = fp.morph(lambda x: x.upper(), dat)

        def join(x: str, y: str) -> str:
            return x + y

        red = fp.reduce(join, morph, dat)

        run_op(db, store, morph.parent)
        run_op(db, store, red.parent)
        assert ui.take(red) == "BLA BLAbla bla"


def test_multi_reduce() -> None:
    """Test store for caching."""
    with Fun(MockServer()):
        db, store = _context.get_connection()
        dat = ui.put("bla bla")
        morph = fp.morph(lambda x: x.upper(), dat)

        def join(*x: str) -> str:
            out = ""
            for el in x:
                out += el
            return out

        red = fp.reduce(join, morph, dat, "|wat")

        run_op(db, store, morph.parent)
        run_op(db, store, red.parent)
        assert ui.take(red) == "BLA BLAbla bla|wat"


def test_store_takeout() -> None:
    """Test store for caching."""
    with Fun(MockServer()):
        s = ui.put(3)
        with tempfile.NamedTemporaryFile() as f:
            ui.takeout(s, f.name)
            with open(f.name, "r") as f2:
                assert json.loads(f2.read()) == 3


def test_wait() -> None:
    """Test waiting on things."""
    with Fun(MockServer()):
        db, store = _context.get_connection()
        s = ui.shell("cp file1 file2", inp={"file1": "wawa"}, out=["file2"])
        with pytest.raises(TimeoutError):
            ui.wait_for(s.stdout, timeout=0)
        run_op(db, store, s.op.hash)
        ui.wait_for(s.stdout, timeout=0)
        ui.wait_for(s, timeout=0)
