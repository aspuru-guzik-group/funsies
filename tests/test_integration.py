"""Test of Funsies shell capabilities."""
# std
import os
import shutil
import subprocess
import tempfile
import time

# external
import pytest

# module
import funsies
from funsies import execute, Fun, morph, put, reduce, shell, tag, take, wait_for


def join_bytes(*args: bytes) -> bytes:
    """Join things."""
    return "|".join([a.decode() for a in args]).encode()


# Make instead of read reference data
make_reference = False

# directory for reference data
ref_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "reference_data")


@pytest.mark.parametrize("reference", ["0.1"])
@pytest.mark.parametrize("nworkers", [1, 2, 3])
def test_integration(reference: str, nworkers: int) -> None:
    """Test full integration."""
    # make a temp file and copy reference database
    dir = tempfile.mkdtemp()
    if not make_reference:
        shutil.copy(os.path.join(ref_dir, reference, "appendonly.aof"), dir)

    shutil.copy(os.path.join(ref_dir, "redis.conf"), dir)

    # Start redis
    redis_server = subprocess.Popen(["redis-server", "redis.conf"], cwd=dir)
    # wait for server to start
    time.sleep(0.1)

    # spawn workers
    worker_pool = [subprocess.Popen(["rq", "worker"]) for i in range(nworkers)]

    # Start funsie script
    with Fun():
        dat = put(b"bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell(
            "cat file1 file2; grep 'bla' file2 file1 > file3; date >> file3",
            inp=dict(file1=step1, file2=dat),
            out=["file2", "file3"],
        )
        tag("a tagged file", step2.out["file3"])
        echo = shell("sleep 1", "date")
        merge = reduce(
            join_bytes,
            step2.out["file3"],
            echo.stdouts[1],
            name="merger",
        )

        execute(echo)
        execute(merge)
        wait_for(merge, timeout=10.0)

        # stop workers
        for i in range(nworkers):
            worker_pool[i].kill()

        # wait till completed
        assert take(step1) == b"BLA BLA"
        assert take(step2.stdout) == b"BLA BLAbla bla"

        if make_reference:
            with open(os.path.join(ref_dir, reference, "test1"), "wb") as f:
                out = take(merge)
                assert out is not None
                f.write(out)

            db = funsies.context.get_db()
            db.save()
            time.sleep(0.3)
            shutil.copy(
                os.path.join(dir, "appendonly.aof"),
                os.path.join(ref_dir, reference, "appendonly.aof"),
            )
        else:
            # Test against reference dbs
            with open(os.path.join(ref_dir, reference, "test1"), "rb") as f:
                data = f.read()

            assert take(merge) == data

    # stop db
    redis_server.kill()
    shutil.rmtree(dir)


# make_reference = True
# test_integration("0.1")
