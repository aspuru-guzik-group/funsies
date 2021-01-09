"""Test of Funsies shell capabilities."""
# std
import os
import shutil
import subprocess
import tempfile
import time

# external
import pytest
from redis import Redis
from rq import Queue

# module
from funsies import execute, morph, put, reduce, shell, take, wait_for

nworkers = 1


def join_bytes(*args: bytes) -> bytes:
    """Join things."""
    return "|".join([a.decode() for a in args]).encode()


# Make instead of read reference data
make_reference = False

# directory for reference data
ref_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "reference_data")


@pytest.mark.parametrize("reference", ["0.1"])
def test_integration(reference: str) -> None:
    """Test full integration."""
    # make a temp file and copy reference database
    dir = tempfile.mkdtemp()
    if not make_reference:
        shutil.copy(os.path.join(ref_dir, reference, "appendonly.aof"), dir)

    shutil.copy(os.path.join(ref_dir, "redis.conf"), dir)

    # Start redis
    redis_server = subprocess.Popen(["redis-server", "redis.conf"], cwd=dir)
    # wait for server to start
    time.sleep(1.0)
    db = Redis()

    # setup RQ
    queue = Queue(connection=db)

    # spawn workers
    worker_pool = [subprocess.Popen(["rq", "worker"]) for i in range(nworkers)]

    dat = put(db, b"bla bla")
    step1 = morph(db, lambda x: x.decode().upper().encode(), dat)
    step2 = shell(
        db,
        "cat file1 file2; grep 'bla' file2 file1 > file3; date >> file3",
        inp=dict(file1=step1, file2=dat),
        out=["file2", "file3"],
    )
    echo = shell(db, "sleep 1", "date")
    merge = reduce(
        db,
        join_bytes,
        step2.out["file3"],
        echo.stdouts[1],
        name="merger",
    )

    execute(db, queue, echo)
    execute(db, queue, merge)
    wait_for(db, merge, timeout=10.0)

    # stop workers
    for i in range(nworkers):
        worker_pool[i].kill()

    # wait till completed
    assert take(db, step1) == b"BLA BLA"
    assert take(db, step2.stdout) == b"BLA BLAbla bla"

    if make_reference:
        with open(os.path.join(ref_dir, reference, "test1"), "wb") as f:
            out = take(db, merge)
            assert out is not None
            f.write(out)

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

        assert take(db, merge) == data

    # stop db
    redis_server.kill()
    shutil.rmtree(dir)


# make_reference = True
# test_integration("0.1")
