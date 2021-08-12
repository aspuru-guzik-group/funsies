"""Test of large artefacts save / restore."""
# std

# external
from fakeredis import FakeStrictRedis as Redis

# funsies
from funsies import _graph
import funsies._constants as cons
from funsies.config import RedisStorage
from funsies.types import hash_t


def test_artefact_add_large() -> None:
    """Test adding large artefacts to a Redis store."""
    db = Redis()
    store = RedisStorage(db, block_size=8)
    art = _graph.variable_artefact(db, hash_t("1"), "file", cons.Encoding.blob)
    data = b"12345" * 100
    _graph.set_data(db, store, art.hash, data, _graph.ArtefactStatus.done)
    data2 = _graph.get_data(db, store, art)
    assert db.llen(cons.join(cons.ARTEFACTS, art.hash, "data")) == 63
    assert data == data2


def test_artefact_replace_large() -> None:
    """Test replacing large artefacts."""
    db = Redis()
    store = RedisStorage(db, block_size=8)
    art = _graph.variable_artefact(db, hash_t("1"), "file", cons.Encoding.blob)
    data = b"12345" * 100
    _graph.set_data(db, store, art.hash, data, _graph.ArtefactStatus.done)

    store.block_size = 1000000
    _graph.set_data(db, store, art.hash, data, _graph.ArtefactStatus.done)
    data2 = _graph.get_data(db, store, art)

    assert data == data2
