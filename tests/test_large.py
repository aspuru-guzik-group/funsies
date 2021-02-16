"""Test of large artefacts save / restore."""
# std

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import _graph, hash_t
import funsies.constants as cons


def test_artefact_add_large() -> None:
    """Test adding large artefacts."""
    store = Redis()
    _graph._set_block_size(5)
    art = _graph.variable_artefact(store, hash_t("1"), "file")
    data = b"12345" * 100
    _graph.set_data(store, art.hash, data, _graph.ArtefactStatus.done)
    data2 = _graph.get_data(store, art.hash)

    assert store.llen(cons.join(cons.ARTEFACTS, art.hash, "data")) == 100
    assert data == data2


def test_artefact_replace_large() -> None:
    """Test replacing large artefacts."""
    store = Redis()
    _graph._set_block_size(5)
    art = _graph.variable_artefact(store, hash_t("1"), "file")
    data = b"12345" * 100
    _graph.set_data(store, art.hash, data, _graph.ArtefactStatus.done)

    _graph._set_block_size(10000)
    _graph.set_data(store, art.hash, data, _graph.ArtefactStatus.done)
    data2 = _graph.get_data(store, art.hash)

    assert data == data2
