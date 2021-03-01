"""Test of large artefacts save / restore."""
# std

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import _graph
import funsies._constants as cons
from funsies.types import hash_t


def test_artefact_add_large() -> None:
    """Test adding large artefacts."""
    store = Redis()
    _graph._set_block_size(8)
    art = _graph.variable_artefact(store, hash_t("1"), "file", cons.Encoding.blob)
    data = b"12345" * 100
    _graph.set_data(store, art, data, _graph.ArtefactStatus.done)
    data2 = _graph.get_data(store, art)

    assert store.llen(cons.join(cons.ARTEFACTS, art.hash, "data")) == 63
    assert data == data2


def test_artefact_replace_large() -> None:
    """Test replacing large artefacts."""
    store = Redis()
    _graph._set_block_size(8)
    art = _graph.variable_artefact(store, hash_t("1"), "file", cons.Encoding.blob)
    data = b"12345" * 100
    _graph.set_data(store, art, data, _graph.ArtefactStatus.done)

    _graph._set_block_size(10000)
    _graph.set_data(store, art, data, _graph.ArtefactStatus.done)
    data2 = _graph.get_data(store, art)

    assert data == data2
