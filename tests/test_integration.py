"""Test of Funsies backward compatibility."""
# std
import os
import shutil
import tempfile
from typing import Dict

# external
import pytest

# module
from funsies import (
    _serdes,
    execute,
    ManagedFun,
    morph,
    put,
    py,
    reduce,
    shell,
    take,
    utils,
    wait_for,
)
from funsies.types import Result


def join_bytes(*args: bytes) -> bytes:
    """Join things."""
    return "|".join([a.decode() for a in args]).encode()


# Make instead of read reference data
make_reference = False

# directory for reference data
ref_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "reference_data")

# Versions
VERSION_FAILING = ["0.1", "0.2", "0.3"]
VERSION_WORKING = ["0.6"]


@pytest.mark.parametrize("reference", VERSION_WORKING)
@pytest.mark.parametrize("nworkers", [1, 2, 8])
def test_integration(reference: str, nworkers: int) -> None:
    """Test full integration."""
    # make a temp file and copy reference database
    dir = tempfile.mkdtemp()
    if not make_reference:
        shutil.copy(os.path.join(ref_dir, reference, "appendonly.aof"), dir)
    shutil.copy(os.path.join(ref_dir, "redis.conf"), dir)

    # Dictionary for test data
    test_data = {}

    # Start funsie script
    with ManagedFun(nworkers=nworkers, directory=dir, redis_args=["redis.conf"]):
        dat = put(b"bla bla")
        step1 = morph(lambda x: x.decode().upper().encode(), dat)
        step2 = shell(
            "cat file1 file2; grep 'bla' file2 file1 > file3; date >> file3",
            inp=dict(file1=step1, file2=dat),
            out=["file2", "file3"],
        )
        echo = shell("sleep 1", "date")
        merge = reduce(
            join_bytes,
            step2.out["file3"],
            echo.stdouts[1],
            name="merger",
        )

        def tolist(x: bytes, y: bytes) -> Dict[int, str]:
            return {1: x.decode(), 8: y.decode()}

        A = py(tolist, merge, echo.stdouts[1])
        test_data["test1"] = A

        def raises(inp: bytes) -> bytes:
            raise RuntimeError("an error was raised")

        def error_count(*inp: Result[bytes]) -> bytes:
            out = utils.match_results(inp, lambda x: 0, lambda x: 1)
            return str(sum(out)).encode()

        err = morph(raises, dat)
        count = reduce(
            error_count, dat, dat, err, dat, err, err, echo.stdouts[0], strict=False
        )
        cat = utils.concat(merge, dat, err, count, echo.stdouts[1], strict=False)
        test_data["test2"] = cat

        execute(step1)
        wait_for(step1, timeout=10.0)
        execute(step2)
        wait_for(step2, timeout=10.0)
        assert take(step1) == b"BLA BLA"
        assert take(step2.stdout) == b"BLA BLAbla bla"

        if make_reference:
            folder = os.path.join(ref_dir, reference)
            os.makedirs(folder, exist_ok=True)

            for name, artefact in test_data.items():
                with open(os.path.join(folder, name), "wb") as f:
                    execute(artefact)
                    wait_for(artefact, 10.0)
                    out = take(artefact)
                    data2 = _serdes.encode(artefact.kind, out)
                    assert isinstance(data2, bytes)
                    f.write(data2)

            shutil.copy(
                os.path.join(dir, "appendonly.aof"),
                os.path.join(folder, "appendonly.aof"),
            )
        else:
            # Test against reference dbs
            for name, artefact in test_data.items():
                execute(artefact)
                wait_for(artefact, 10.0)
                with open(os.path.join(ref_dir, reference, name), "rb") as f:
                    data = f.read()

                out = take(artefact)
                data_ref = _serdes.encode(artefact.kind, out)
                assert isinstance(data_ref, bytes)
                assert data == data_ref

    shutil.rmtree(dir)
