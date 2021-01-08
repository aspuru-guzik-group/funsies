"""Tests of error handling."""
# std

# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# module
import funsies
from funsies import _graph


def test_artefact_add() -> None:
    """Test adding const artefacts."""
    store = Redis()
    a = _graph.constant_artefact(store, b"bla bla")
    b = _graph.get_artefact(store, a.hash)
    assert b is not None
    assert a == b


def test_artefact_load_errors() -> None:
    """Test loading artefact errors."""
    store = Redis()
    with pytest.raises(RuntimeError):
        _ = _graph.get_artefact(store, "bla")

    # TODO check that warnings are logged?
    _graph.constant_artefact(store, b"bla bla")
    _graph.constant_artefact(store, b"bla bla")

    _graph.variable_artefact(store, "1", "file")
    _graph.variable_artefact(store, "1", "file")


def test_artefact_update() -> None:
    """Test updating a const artefact."""
    store = Redis()
    art = _graph.constant_artefact(store, b"bla bla")
    with pytest.raises(TypeError):
        _graph.set_data(store, art, b"b")


def test_not_generated() -> None:
    """What happens when an artefact is not generated?"""
    store = Redis()
    s = funsies.shell(store, "cp file1 file2", inp=dict(file1="bla"), out=["file3"])
    funsies.run_op(store, s.op.hash)
    assert funsies.take(store, s.returncode) == b"0"
    with pytest.raises(funsies.UnwrapError):
        funsies.take(store, s.out["file3"])


def test_error_propagation() -> None:
    """Test propagation of errors."""
    store = Redis()
    s1 = funsies.shell(store, "cp file1 file3", inp=dict(file1="bla"), out=["file2"])
    s2 = funsies.shell(
        store, "cat file1 file2", inp=dict(file1="a file", file2=s1.out["file2"])
    )
    funsies.run_op(store, s1.op.hash)
    funsies.run_op(store, s2.op.hash)
    out = funsies.take(store, s2.stdout, strict=False)
    assert isinstance(out, funsies.Error)
    assert out.source == s1.op.hash


def test_error_propagation_morph() -> None:
    """Test propagation of errors."""
    store = Redis()
    s1 = funsies.shell(store, "cp file1 file3", inp=dict(file1="bla"), out=["file2"])

    def fun_strict(inp: bytes) -> bytes:
        return inp

    def fun_lax(inp: funsies.Result[bytes]) -> bytes:
        return b"bla bla"

    s2 = funsies.morph(store, fun_strict, s1.out["file2"])
    s3 = funsies.morph(store, fun_lax, s1.out["file2"])
    s4 = funsies.morph(store, fun_lax, s1.out["file2"], strict=False)

    funsies.run_op(store, s1.op.hash)

    funsies.run_op(store, s2.parent)
    out = funsies.take(store, s2, strict=False)
    assert isinstance(out, funsies.Error)
    assert out.source == s1.op.hash

    funsies.run_op(store, s3.parent)
    out = funsies.take(store, s3, strict=False)
    assert isinstance(out, funsies.Error)
    assert out.source == s1.op.hash

    funsies.run_op(store, s4.parent)
    out = funsies.take(store, s4)
    assert out == b"bla bla"


def test_error_propagation_shell() -> None:
    """Test propagation of errors."""
    store = Redis()
    s1 = funsies.shell(store, "cp file1 file3", inp=dict(file1="bla"), out=["file2"])
    s2 = funsies.shell(store, "cat file2", inp=dict(file2=s1.out["file2"]))
    s3 = funsies.shell(
        store, "cat file2", inp=dict(file2=s1.out["file2"]), strict=False
    )

    funsies.run_op(store, s1.op.hash)
    funsies.run_op(store, s2.op.hash)
    with pytest.raises(funsies.UnwrapError):
        funsies.take(store, s2.stderr)

    funsies.run_op(store, s3.op.hash)
    assert funsies.take(store, s3.stderr) != b""
    assert funsies.take(store, s3.returncode) != b"0"
