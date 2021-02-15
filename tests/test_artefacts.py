"""Test of artefacts save / restore."""
# std

# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# module
from funsies import _funsies as f
from funsies import _graph, errors, hash_t
from funsies import options


def test_artefact_add() -> None:
    """Test adding const artefacts."""
    options()
    store = Redis()
    a = _graph.constant_artefact(store, b"bla bla")
    b = _graph.get_artefact(store, a.hash)
    assert b is not None
    assert a == b


def test_artefact_add_implicit() -> None:
    """Test adding implicit artefacts."""
    options()
    store = Redis()
    art = _graph.variable_artefact(store, hash_t("1"), "file")
    out = _graph.get_data(store, art)
    assert isinstance(out, errors.Error)
    assert out.kind == errors.ErrorKind.NotFound


def test_operation_pack() -> None:
    """Test packing and unpacking of operations."""
    opt = options()
    store = Redis()
    a = _graph.constant_artefact(store, b"bla bla")
    b = _graph.constant_artefact(store, b"bla bla bla")
    fun = f.Funsie(
        how=f.FunsieHow.shell,
        what=b"cat",
        inp=["infile"],
        out=["out"],
    )
    op = _graph.make_op(store, fun, {"infile": a}, opt)
    op2 = _graph.Operation.grab(store, op.hash)
    assert op == op2

    with pytest.raises(AttributeError):
        op = _graph.make_op(store, fun, {}, opt)

    with pytest.raises(AttributeError):
        # no inputs
        op = _graph.make_op(store, fun, {}, opt)

    with pytest.raises(AttributeError):
        # too many inputs
        op = _graph.make_op(store, fun, {"infile": a, "infile2": b}, opt)

    with pytest.raises(RuntimeError):
        op = _graph.Operation.grab(store, hash_t("b"))
