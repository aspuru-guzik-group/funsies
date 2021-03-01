"""Test of Funsies user routines."""
# std
import json
import tempfile
from typing import Tuple

# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# module
from funsies import _graph, Fun, options, ui
from funsies._run import run_op
from funsies.types import Encoding, UnwrapError


def test_shell_run() -> None:
    """Test shell command."""
    with Fun(Redis()) as db:
        s = ui.shell("cp file1 file2", inp={"file1": "wawa"}, out=["file2"])
        run_op(db, s.hash)
        assert _graph.get_data(db, s.stderr) == b""
        assert _graph.get_data(db, s.returncode) == 0
        assert _graph.get_data(db, s.inp["file1"]) == "wawa"
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
        assert s != s2  # type:ignore
        assert ui.take(s) == "bla bla"
        assert ui.take(s2) == b"bla bla"


def test_rm() -> None:
    """Test rm."""
    with Fun(Redis(), options(distributed=False)):
        dat = ui.put("bla bla")
        # removing const artefact raises
        with pytest.raises(AttributeError):
            ui.reset(dat)
        ui.take(dat)

        def upper(x: str) -> str:
            return x.upper()

        m1 = ui.morph(upper, dat)
        m2 = ui.morph(lambda x: x + x, m1)
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
    with Fun(Redis()) as db:
        dat = ui.put(b"bla bla")
        morph = ui.morph(lambda x: x.decode().upper().encode(), dat)
        run_op(db, morph.parent)
        assert ui.take(morph) == b"BLA BLA"

        dat = ui.put("bla bla")
        morph = ui.morph(lambda x: x.upper(), dat)
        run_op(db, morph.parent)
        assert ui.take(morph) == "BLA BLA"


def test_reduce() -> None:
    """Test store for caching."""
    with Fun(Redis()) as db:
        dat = ui.put("bla bla")
        morph = ui.morph(lambda x: x.upper(), dat)

        def join(x: str, y: str) -> str:
            return x + y

        red = ui.reduce(join, morph, dat)

        run_op(db, morph.parent)
        run_op(db, red.parent)
        assert ui.take(red) == "BLA BLAbla bla"


def test_multi_reduce() -> None:
    """Test store for caching."""
    with Fun(Redis()) as db:
        dat = ui.put("bla bla")
        morph = ui.morph(lambda x: x.upper(), dat)

        def join(*x: str) -> str:
            out = ""
            for el in x:
                out += el
            return out

        # with pytest.raises(TypeError):
        #     red = ui.reduce(join, morph, dat, b"|wat")

        red = ui.reduce(join, morph, dat, "|wat")

        run_op(db, morph.parent)
        run_op(db, red.parent)
        assert ui.take(red) == "BLA BLAbla bla|wat"


def test_store_takeout() -> None:
    """Test store for caching."""
    with Fun(Redis()):
        s = ui.put(3)
        with tempfile.NamedTemporaryFile() as f:
            ui.takeout(s, f.name)
            with open(f.name, "r") as f2:
                assert json.loads(f2.read()) == 3


def test_wait() -> None:
    """Test waiting on things."""
    with Fun(Redis()) as db:
        s = ui.shell("cp file1 file2", inp={"file1": "wawa"}, out=["file2"])
        with pytest.raises(TimeoutError):
            ui.wait_for(s.stdout, timeout=0)
        run_op(db, s.op.hash)
        ui.wait_for(s.stdout, timeout=0)
        ui.wait_for(s, timeout=0)
