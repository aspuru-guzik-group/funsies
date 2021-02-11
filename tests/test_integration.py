"""Test of Funsies backward compatibility."""
# std
import os
import shutil
import tempfile

# external
import pytest

# module
from funsies import (
    execute,
    ManagedFun,
    mapping,
    morph,
    put,
    reduce,
    shell,
    tag,
    take,
    wait_for,
)


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

    # Start funsie script
    with ManagedFun(nworkers=nworkers, directory=dir):
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
        estdout, merge = mapping(
            lambda x, y: (y, x), merge, echo.stdouts[1], noutputs=2
        )

        execute(estdout)
        wait_for(merge, timeout=10.0)
        wait_for(estdout, timeout=10.0)

        # wait till completed
        assert take(step1) == b"BLA BLA"
        assert take(step2.stdout) == b"BLA BLAbla bla"

        if make_reference:
            with open(os.path.join(ref_dir, reference, "test1"), "wb") as f:
                out = take(merge)
                assert out is not None
                f.write(out)

            shutil.copy(
                os.path.join(dir, "appendonly.aof"),
                os.path.join(ref_dir, reference, "appendonly.aof"),
            )
        else:
            # Test against reference dbs
            with open(os.path.join(ref_dir, reference, "test1"), "rb") as f:
                data = f.read()

            print(take(merge))
            assert take(merge) == data

    shutil.rmtree(dir)
