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
