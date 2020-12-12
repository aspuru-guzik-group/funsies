"""Basic test of diskcache functionality."""
# std
import tempfile

# external
from dask.distributed import Client

# package
from funsies import cliwrap


def test_task_cache() -> None:
    """Test task caching."""
    with tempfile.TemporaryDirectory() as tmpd:
        cliwrap.setup_cache(tmpd, 1, 1.0)
        assert cliwrap.__CACHE is not None

        cmd = cliwrap.Command(
            executable="cat",
            args=["file"],
        )
        task = cliwrap.Task([cmd], inputs={"file": b"12345"})
        results = cliwrap.run(task)
        assert results.commands[0].stdout == b"12345"

        k = 0
        for _ in cliwrap.__CACHE:
            k = k + 1
        assert k == 1

        results = cliwrap.run(task)
        assert results.commands[0].stdout == b"12345"
        assert results.cached


def test_task_cache_error() -> None:
    """Test cache failures."""
    with tempfile.TemporaryDirectory() as tmpd:
        cliwrap.setup_cache(tmpd, 1, 1.0)
        assert cliwrap.__CACHE is not None

        cmd = cliwrap.Command(
            executable="cat",
            args=["file"],
        )
        task = cliwrap.Task([cmd], inputs={"file": b"12345"})
        results = cliwrap.run(task)
        assert results.commands[0].stdout == b"12345"

        for key in cliwrap.__CACHE:
            cliwrap.__CACHE[key] = 3.0

        results = cliwrap.run(task)
        assert results.commands[0].stdout == b"12345"
        assert not results.cached


def test_dask_task_cache() -> None:
    """Test task caching by Dask workers."""
    pool = Client()

    # log_everything()
    # pool.run(log_everything)

    with tempfile.TemporaryDirectory() as tmpd:
        pool.run(cliwrap.setup_cache, tmpd, 1, 1.0)

        cmd = cliwrap.Command(
            executable="cat",
            args=["file"],
        )
        task = cliwrap.Task([cmd], inputs={"file": b"12345"})
        future = pool.submit(cliwrap.run, task)
        assert future.result().commands[0].stdout == b"12345"

        cliwrap.setup_cache(tmpd, 1, 1.0)
        assert cliwrap.__CACHE is not None
        k = 0
        for _ in cliwrap.__CACHE:
            k = k + 1
        assert k == 1

        future = pool.submit(cliwrap.run, task)
        assert future.result().commands[0].stdout == b"12345"
        assert future.result().cached

        # do many at once
        cmd = cliwrap.Command(
            executable="sleep",
            args=["0.3"],
        )
        task = cliwrap.Task([cmd])
        future = pool.submit(cliwrap.run, task)
        assert future.result().commands[0].returncode == 0

        futures = []
        for _ in range(10):
            futures += [pool.submit(cliwrap.run, task)]

        for future in futures:
            assert future.result().cached


def test_dask_task_cache_missing() -> None:
    """Test Dask execution without cache."""
    cliwrap.un_setup_cache()
    pool = Client()

    cmd = cliwrap.Command(
        executable="cat",
        args=["file"],
    )
    assert cliwrap.__CACHE is None
    task = cliwrap.Task([cmd], inputs={"file": b"12345"})
    future = pool.submit(cliwrap.run, task)
    assert future.result().commands[0].stdout == b"12345"


if __name__ == "__main__":
    pass
