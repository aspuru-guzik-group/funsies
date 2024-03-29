"""Test of artefacts save / restore."""
# std

# external
import pytest

# funsies
from funsies import _funsies as f
from funsies import _graph, _serdes, options
from funsies.config import MockServer
from funsies.types import Encoding, Error, ErrorKind, hash_t


def test_artefact_add() -> None:
    """Test adding const artefacts."""
    options()
    server = MockServer()
    db, store = server.new_connection()

    a = _graph.constant_artefact(db, store, b"bla bla")
    b = _graph.Artefact[bytes].grab(db, a.hash)
    c = _graph.get_data(db, store, a)
    assert b is not None
    assert a == b
    assert c == b"bla bla"


def test_artefact_add_implicit() -> None:
    """Test adding implicit artefacts."""
    options()
    server = MockServer()
    db, store = server.new_connection()

    art = _graph.variable_artefact(db, hash_t("1"), "file", Encoding.blob)
    out = _graph.get_data(db, store, art)
    assert isinstance(out, Error)
    assert out.kind == ErrorKind.NotFound


def test_operation_pack() -> None:
    """Test packing and unpacking of operations."""
    opt = options()
    server = MockServer()
    db, store = server.new_connection()

    a = _graph.constant_artefact(db, store, b"bla bla")
    b = _graph.constant_artefact(db, store, b"bla bla bla")
    fun = f.Funsie(
        how=f.FunsieHow.shell,
        what="cat infile",
        inp={"infile": Encoding.blob},
        out={"out": Encoding.json},
        extra={},
    )
    op = _graph.make_op(db, fun, {"infile": a}, opt)
    op2 = _graph.Operation.grab(db, op.hash)
    assert op == op2

    with pytest.raises(AttributeError):
        op = _graph.make_op(db, fun, {}, opt)

    with pytest.raises(AttributeError):
        # no inputs
        op = _graph.make_op(db, fun, {}, opt)

    with pytest.raises(AttributeError):
        # too many inputs
        op = _graph.make_op(db, fun, {"infile": a, "infile2": b}, opt)

    with pytest.raises(RuntimeError):
        op = _graph.Operation.grab(db, hash_t("b"))


def test_artefact_wrong_type() -> None:
    """Test storing non-bytes in implicit artefacts."""
    server = MockServer()
    db, store = server.new_connection()

    art = _graph.variable_artefact(db, hash_t("1"), "file", Encoding.blob)
    _graph.set_data(
        db,
        store,
        art.hash,
        _serdes.encode(art.kind, "what"),
        _graph.ArtefactStatus.done,
    )
    out = _graph.get_data(db, store, art)
    assert isinstance(out, Error)
    assert out.kind == ErrorKind.WrongType

    art = _graph.variable_artefact(db, hash_t("2"), "file", Encoding.json)
    _graph.set_data(
        db,
        store,
        art.hash,
        _serdes.encode(art.kind, ["what", 1]),
        _graph.ArtefactStatus.done,
    )
    out = _graph.get_data(db, store, art)
    assert out == ["what", 1]
