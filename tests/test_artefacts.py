"""Test of artefacts save / restore."""
# std

# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# module
from funsies import _funsies as f
from funsies import _graph, options
from funsies.types import Error, ErrorKind, hash_t


def test_artefact_add() -> None:
    """Test adding const artefacts."""
    options()
    store = Redis()
    a = _graph.constant_artefact(store, b"bla bla")
    b = _graph.Artefact.grab(store, a.hash)
    assert b is not None
    assert a == b


def test_artefact_add_implicit() -> None:
    """Test adding implicit artefacts."""
    options()
    store = Redis()
    art = _graph.variable_artefact(store, hash_t("1"), "file")
    out = _graph.get_data(store, art)
    assert isinstance(out, Error)
    assert out.kind == ErrorKind.NotFound


def test_operation_pack() -> None:
    """Test packing and unpacking of operations."""
    opt = options()
    store = Redis()
    a = _graph.constant_artefact(store, b"bla bla")
    b = _graph.constant_artefact(store, b"bla bla bla")
    fun = f.Funsie(
        how=f.FunsieHow.shell, what="cat infile", inp=["infile"], out=["out"], extra={}
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


def test_artefact_wrong_type() -> None:
    """Test storing non-bytes in implicit artefacts."""
    store = Redis()
    art = _graph.variable_artefact(store, hash_t("1"), "file")
    _graph.set_data(store, art.hash, "what", _graph.ArtefactStatus.done)  # type:ignore
    out = _graph.get_data(store, art)
    assert isinstance(out, Error)
    assert out.kind == ErrorKind.ExceptionRaised


def test_artefact_wrong_type2() -> None:
    """Test storing non-bytes in implicit artefacts."""
    import tempfile

    with tempfile.SpooledTemporaryFile(mode="w+") as f:
        for _ in range(10):
            f.write("12412413294230472839401923741209347219347293847")

        # rewind
        f.seek(0)

        store = Redis()
        art = _graph.variable_artefact(store, hash_t("1"), "file")
        _graph.set_data(store, art.hash, f, _graph.ArtefactStatus.done)  # type:ignore
        out = _graph.get_data(store, art)
        assert isinstance(out, Error)
        print(out)
        assert out.kind == ErrorKind.WrongType


def test_artefact_buffered() -> None:
    """Test storing non-bytes in implicit artefacts."""
    import tempfile

    _graph._set_block_size(10)
    data = 10 * b"1234512345111"

    with tempfile.SpooledTemporaryFile(mode="w+b") as f:
        f.write(data)

        # rewind
        f.seek(0)

        store = Redis()
        art = _graph.variable_artefact(store, hash_t("1"), "file")
        _graph.set_data(store, art.hash, f, _graph.ArtefactStatus.done)
        out = _graph.get_data(store, art.hash)
        assert out == data

    with tempfile.SpooledTemporaryFile(mode="w+b") as f:
        _graph.write_data(store, art, f)
        f.flush()

        # rewind
        f.seek(0)
        assert f.read() == data
