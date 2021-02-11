"""Test debugging functions."""
import os.path
import tempfile

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import debug, Fun, run_op, shell


def test_shell_run() -> None:
    """Test run on a shell command."""
    db = Redis()
    with Fun(db):
        cmd = shell("cat file1", inp={"file1": b"bla bla"}, out=["bla"])
        _ = run_op(db, cmd.hash)

        with tempfile.TemporaryDirectory() as d:
            debug.shell(cmd, d)
            n = os.listdir(d)
            assert "stdout0" in n
            assert "stderr0" in n
            assert "input_files" in n
            assert "output_files" in n

            with open(os.path.join(d, "errors.json"), "r") as f:
                assert "MissingOutput" in f.read()


def test_shell_norun() -> None:
    """Test run on a shell command that didn't run."""
    db = Redis()
    with Fun(db):
        cmd = shell("cat file1", inp={"file1": b"bla bla"}, out=["bla"])

        with tempfile.TemporaryDirectory() as d:
            debug.shell(cmd, d)
            n = os.listdir(d)
            assert "input_files" in n
            assert "output_files" in n

            with open(os.path.join(d, "errors.json"), "r") as f:
                assert "NotFound" in f.read()


def test_artefact() -> None:
    """Test artefact debug."""
    db = Redis()
    with Fun(db):
        cmd = shell("cat file1", inp={"file1": b"bla bla"}, out=["bla"])

        with tempfile.TemporaryDirectory() as d:
            debug.artefact(cmd.stdout, d, connection=db)
            n = os.listdir(d)
            assert "metadata.json" in n
            assert "error.json" in n
            assert "data" not in n

        with tempfile.TemporaryDirectory() as d:
            debug.artefact(cmd.inp["file1"], d, connection=db)
            n = os.listdir(d)
            assert "metadata.json" in n
            assert "error.json" not in n
            assert "data" in n
