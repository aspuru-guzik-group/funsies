"""Test of artefacts save / restore."""
# std

# external
from fakeredis import FakeStrictRedis as Redis
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
    db = server.new_connection()
    store = server.new_storage()

    a = _graph.constant_artefact(db, store, b"bla bla")
    b = _graph.Artefact[bytes].grab(store, a.hash)
    assert b is not None
    assert a == b


# def test_artefact_add_implicit() -> None:
#     """Test adding implicit artefacts."""
#     options()
#     store = MockServer().runtime()
#     art = _graph.variable_artefact(store, hash_t("1"), "file", Encoding.blob)
#     out = _graph.get_data(store, art)
#     assert isinstance(out, Error)
#     assert out.kind == ErrorKind.NotFound


# def test_operation_pack() -> None:
#     """Test packing and unpacking of operations."""
#     opt = options()
#     store = MockServer().runtime()
#     a = _graph.constant_artefact(store, b"bla bla")
#     b = _graph.constant_artefact(store, b"bla bla bla")
#     fun = f.Funsie(
#         how=f.FunsieHow.shell,
#         what="cat infile",
#         inp={"infile": Encoding.blob},
#         out={"out": Encoding.json},
#         extra={},
#     )
#     op = _graph.make_op(store, fun, {"infile": a}, opt)
#     op2 = _graph.Operation.grab(store, op.hash)
#     assert op == op2

#     with pytest.raises(AttributeError):
#         op = _graph.make_op(store, fun, {}, opt)

#     with pytest.raises(AttributeError):
#         # no inputs
#         op = _graph.make_op(store, fun, {}, opt)

#     with pytest.raises(AttributeError):
#         # too many inputs
#         op = _graph.make_op(store, fun, {"infile": a, "infile2": b}, opt)

#     with pytest.raises(RuntimeError):
#         op = _graph.Operation.grab(store, hash_t("b"))


# def test_artefact_wrong_type() -> None:
#     """Test storing non-bytes in implicit artefacts."""
#     store = MockServer().runtime()
#     art = _graph.variable_artefact(store, hash_t("1"), "file", Encoding.blob)
#     _graph.set_data(
#         store,
#         art.hash,
#         _serdes.encode(art.kind, "what"),
#         _graph.ArtefactStatus.done,
#     )
#     out = _graph.get_data(store, art)
#     assert isinstance(out, Error)
#     assert out.kind == ErrorKind.WrongType

#     art = _graph.variable_artefact(store, hash_t("2"), "file", Encoding.json)
#     _graph.set_data(
#         store,
#         art.hash,
#         _serdes.encode(art.kind, ["what", 1]),
#         _graph.ArtefactStatus.done,
#     )
#     out = _graph.get_data(store, art)
#     assert out == ["what", 1]
