"""Test debugging functions."""
# std
import os.path
import tempfile

# funsies
from funsies import debug, Fun, shell
from funsies._context import get_connection
from funsies._run import run_op
from funsies.config import MockServer


def test_shell_run() -> None:
    """Test run on a shell command."""
    with Fun(MockServer()):
        db, store = get_connection()
        cmd = shell("cat file1", inp={"file1": b"bla bla"}, out=["bla"])
        _ = run_op(db, store, cmd.hash)

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
    with Fun(MockServer()):
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
    with Fun(MockServer()) as db:
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
