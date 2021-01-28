"""Test debugging functions."""
import os.path
import tempfile

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import debug, run_op, shell


def test_shell_run() -> None:
    """Test run on a shell command."""
    db = Redis()
    cmd = shell("cat file1", inp={"file1": b"bla bla"}, out=["bla"], connection=db)
    _ = run_op(db, cmd.hash)

    with tempfile.TemporaryDirectory() as d:
        debug.shell(cmd, d, connection=db)
        n = os.listdir(d)
        assert "stdout0" in n
        assert "stderr0" in n
        assert "input_files" in n
        assert "output_files" in n

        with open(os.path.join(d, "error.log"), "r") as f:
            assert "MissingOutput" in f.read()


def test_shell_norun() -> None:
    """Test run on a shell command that didn't run."""
    db = Redis()
    cmd = shell("cat file1", inp={"file1": b"bla bla"}, out=["bla"], connection=db)

    with tempfile.TemporaryDirectory() as d:
        debug.shell(cmd, d, connection=db)
        n = os.listdir(d)
        assert "input_files" in n
        assert "output_files" in n

        with open(os.path.join(d, "error.log"), "r") as f:
            assert "NotFound" in f.read()
