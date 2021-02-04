"""Test of large artefacts save / restore."""
# std

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import _graph, hash_t
import funsies.constants


def test_artefact_add_large() -> None:
    """Test adding large artefacts."""
    store = Redis()
    _graph._set_block_size(5)
    art = _graph.variable_artefact(store, hash_t("1"), "file")
    data = b"12345" * 100
    _graph.set_data(store, art, data)
    data2 = _graph.get_data(store, art)

    assert len(store.hkeys(funsies.constants.STORE)) == 100
    assert data == data2
