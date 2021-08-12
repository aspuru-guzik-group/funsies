"""Test of artefacts save / restore on disk."""
# std
import tempfile

# external
from fakeredis import FakeStrictRedis as Redis

# funsies
from funsies import _graph
import funsies._constants as cons
from funsies.config import DiskStorage
from funsies.types import hash_t


def test_artefact_save() -> None:
    """Test saving an artefact to disk."""
    with tempfile.TemporaryDirectory() as td:
        db = Redis()
        store = DiskStorage(td)

        art = _graph.variable_artefact(db, hash_t("1"), "file", cons.Encoding.blob)
        data = b"12345" * 100
        _graph.set_data(db, store, art.hash, data, _graph.ArtefactStatus.done)
        data2 = _graph.get_data(db, store, art)
        assert data == data2


