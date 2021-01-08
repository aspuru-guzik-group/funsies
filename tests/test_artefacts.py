"""Test of artefacts save / restore."""
# std

# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# module
from funsies import _funsies as f
from funsies import _graph, errors


def test_artefact_add() -> None:
    """Test adding const artefacts."""
    store = Redis()
    a = _graph.constant_artefact(store, b"bla bla")
    b = _graph.get_artefact(store, a.hash)
    assert b is not None
    assert a == b


def test_artefact_add_implicit() -> None:
    """Test adding implicit artefacts."""
    store = Redis()
    art = _graph.variable_artefact(store, "1", "file")
    out = _graph.get_data(store, art)
    assert isinstance(out, errors.Error)
    assert out.kind == errors.ErrorKind.NotFound


def test_operation_pack() -> None:
    """Test packing and unpacking of operations."""
    store = Redis()
    a = _graph.constant_artefact(store, b"bla bla")
    fun = f.Funsie(
        how=f.FunsieHow.shell,
        what=b"cat",
        inp=["infile"],
        out=["out"],
    )
    op = _graph.make_op(store, fun, {"infile": a})
    op2 = _graph.get_op(store, op.hash)
    assert op == op2

    with pytest.raises(AttributeError):
        op = _graph.make_op(store, fun, {})

    # b = _graph.store_explicit_artefact(store, b"bla bla")
    # with pytest.raises(TypeError):
    #     op = _graph.make_op(store, fun, {"infile": b})

    with pytest.raises(RuntimeError):
        op = _graph.get_op(store, "b")
