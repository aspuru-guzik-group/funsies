"""Test of artefacts save / restore on disk."""
# std
import tempfile

# external
from fakeredis import FakeStrictRedis as Redis

# funsies
from funsies import _graph, _serdes
import funsies._constants as cons
from funsies._storage import DiskStorage
from funsies.types import hash_t


def test_artefact_disk() -> None:
    """Test saving an artefact to disk and restoring it."""
    with tempfile.TemporaryDirectory() as td:
        db = Redis()
        store = DiskStorage(td)

        art = _graph.variable_artefact(
            db, hash_t("12345678"), "file", cons.Encoding.blob
        )
        data = b"12345" * 100
        _graph.set_data(db, store, art.hash, data, _graph.ArtefactStatus.done)
        data2 = _graph.get_data(db, store, art)
        assert data == data2


def test_artefact_disk_json() -> None:
    """Test saving an artefact to disk and restoring it."""
    with tempfile.TemporaryDirectory() as td:
        db = Redis()
        store = DiskStorage(td)

        art = _graph.variable_artefact(
            db, hash_t("12345678"), "file", cons.Encoding.json
        )
        dat = [0, 1, 2, 3, "a string"]
        bdat = _serdes.encode(_serdes.kind(dat), dat)

        _graph.set_data(db, store, art.hash, bdat, _graph.ArtefactStatus.done)
        data2 = _graph.get_data(db, store, art)
        assert dat == data2


def test_artefact_disk_distributed() -> None:
    """Test whether artefacts on disk works on different nodes."""
    # funsies
    import funsies as f

    with tempfile.TemporaryDirectory() as td:
        with f.ManagedFun(nworkers=1, data_url=f"file://{td}"):
            dat = f.put(b"bla bla")
            step1 = f.morph(lambda x: x.decode().upper().encode(), dat)
            step2 = f.shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
            step2b = f.shell("cat file1", inp=dict(file1=step1))

            f.execute(step2)
            f.wait_for(step2, 1.0)
            out = f.take(step2.stdout)
            assert out == b"BLA BLAbla bla"

            f.execute(step2b)
            f.wait_for(step2b, 1.0)
            out = f.take(step2b.stdout)
            assert out == b"BLA BLA"
