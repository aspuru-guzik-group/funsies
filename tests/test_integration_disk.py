"""Test of Funsies backward compatibility."""
# std
import os
import secrets
import shutil
import tempfile
from typing import Any

# external
import pytest

# funsies
from funsies import (
    _serdes,
    execute,
    ManagedFun,
    morph,
    put,
    reduce,
    shell,
    take,
    template,
    wait_for,
)

# Make instead of read reference data
make_reference = False

# directory for reference data
ref_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "reference_data")

# Versions
VERSION_WORKING = ["0.8_disk"]


@pytest.mark.parametrize("reference", VERSION_WORKING)
@pytest.mark.parametrize("nworkers", [1, 2, 8])
def test_integration(reference: str, nworkers: int) -> None:
    """Test full integration."""
    # make a temp file and copy reference database
    dir = tempfile.mkdtemp()
    if not make_reference:
        shutil.copy(os.path.join(ref_dir, reference, "appendonly.aof"), dir)
        shutil.copytree(
            os.path.join(ref_dir, reference, "data"), os.path.join(dir, "data")
        )
    shutil.copy(os.path.join(ref_dir, "redis.conf"), dir)

    # data url
    datadir = f"file://{os.path.join(dir, 'data')}"

    # Dictionary for test data
    test_data: dict[str, Any] = {}

    def update_data(a: dict[int, int], b: list[int]) -> dict[int, int]:
        for i in b:
            a[i] = a.get(i, 0) + 1
        return a

    def sum_data(x: dict[int, int]) -> int:
        return sum([int(k) * v for k, v in x.items()])

    def make_secret(x: int) -> str:
        return secrets.token_hex(x)

    # Start funsie script
    with ManagedFun(
        nworkers=nworkers,
        directory=dir,
        data_url=datadir,
        redis_args=["redis.conf"],
    ) as db:
        integers = put([5, 4, 8, 9, 9, 10, 1, 3])
        init_data = put({100: 9})
        test_data["init_data"] = init_data
        nbytes = put(4)

        s1 = reduce(update_data, init_data, integers)
        num = morph(sum_data, s1)
        date = shell("date").stdout
        test_data["date"] = date
        rand = morph(make_secret, nbytes)

        s4 = template(
            "date:{{date}}\n"
            + "some random bytes:{{random}}\n"
            + "a number: {{num}}\n"
            + "a string: {{string}}\n",
            {"date": date, "random": rand, "num": num, "string": "wazza"},
            name="a template",
        )
        test_data["s4"] = s4

        execute(s4)
        wait_for(s4, 5)

        # check that the db doesn't itself include data
        for k in db.keys():
            assert b"data" not in k

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
            shutil.copytree(
                os.path.join(dir, "data"),
                os.path.join(folder, "data"),
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

    # delete tempdir
    shutil.rmtree(dir)


# test_integration(VERSION_WORKING[-1], 1)
