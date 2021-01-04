"""Test of Funsies save / restore."""
# std

# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# module
from funsies import _funsies as f
from funsies import _graph


def test_artefact_add() -> None:
    """Test adding explicit artefacts."""
    store = Redis()
    a = _graph.store_explicit_artefact(store, "bla bla")
    b = _graph.get_artefact(store, a.hash)
    assert b is not None
    assert a == b


def test_artefact_add_implicit() -> None:
    """Test adding implicit artefacts."""
    store = Redis()
    art = _graph.store_generated_artefact(store, "1", "file", "str")
    assert _graph.get_data(store, art) is None


def test_artefact_errors() -> None:
    """Test adding explicit artefacts."""
    store = Redis()
    with pytest.raises(RuntimeError):
        _ = _graph.get_artefact(store, "bla")

    # TODO check that warnings are logged?
    _graph.store_explicit_artefact(store, "bla bla")
    _graph.store_explicit_artefact(store, "bla bla")

    _graph.store_generated_artefact(store, "1", "file", "str")
    _graph.store_generated_artefact(store, "1", "file", "str")


def test_artefact_update() -> None:
    """Test adding explicit artefacts."""
    store = Redis()
    art = _graph.store_explicit_artefact(store, "bla bla")
    _graph.update_artefact(store, art, "b")
    assert _graph.get_data(store, art) == "b"


def test_operation_pack() -> None:
    """Test packing and unpacking of operations."""
    store = Redis()
    a = _graph.store_explicit_artefact(store, b"bla bla")
    fun = f.Funsie(
        how=f.FunsieHow.shell,
        what=b"cat",
        inp={"infile": "bytes"},
        out={"out2": "bytes", "stdout": "bytes"},
    )
    op = _graph.make_op(store, fun, {"infile": a})
    op2 = _graph.get_op(store, op.hash)
    assert op == op2

    with pytest.raises(AttributeError):
        op = _graph.make_op(store, fun, {})

    b = _graph.store_explicit_artefact(store, "bla bla")
    with pytest.raises(TypeError):
        op = _graph.make_op(store, fun, {"infile": b})

    with pytest.raises(RuntimeError):
        op = _graph.get_op(store, "b")
