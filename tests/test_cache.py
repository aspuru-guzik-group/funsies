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
        context = cliwrap.Context(
            cache=cliwrap.CacheSettings(path=tmpd, shards=1, timeout=1.0)
        )
        cmd = cliwrap.Command(
            executable="cat",
            args=["file"],
        )
        task = cliwrap.Task([cmd], inputs={"file": b"12345"})
        results = cliwrap.run(task, context)
        assert results.commands[0].stdout == b"12345"

        k = 0
        assert cliwrap.__CACHE is not None
        for _ in cliwrap.__CACHE:
            k = k + 1
        assert k == 1

        results = cliwrap.run(task, context)
        assert results.commands[0].stdout == b"12345"
        assert results.cached


def test_task_cache_error() -> None:
    """Test cache failures."""
    with tempfile.TemporaryDirectory() as tmpd:
        context = cliwrap.Context(
            cache=cliwrap.CacheSettings(path=tmpd, shards=1, timeout=1.0)
        )

        cmd = cliwrap.Command(
            executable="cat",
            args=["file"],
        )

        task = cliwrap.Task([cmd], inputs={"file": b"12345"})
        results = cliwrap.run(task, context)
        assert results.commands[0].stdout == b"12345"

        assert cliwrap.__CACHE is not None
        for key in cliwrap.__CACHE:
            cliwrap.__CACHE[key] = 3.0

        results = cliwrap.run(task, context)
        assert results.commands[0].stdout == b"12345"
        assert not results.cached


def test_dask_task_cache() -> None:
    """Test task caching by Dask workers."""
    pool = Client()

    # import logging

    # pool.run(logging.basicConfig, level=logging.DEBUG)

    with tempfile.TemporaryDirectory() as tmpd:
        context = cliwrap.Context(
            cache=cliwrap.CacheSettings(path=tmpd, shards=4, timeout=1.0)
        )

        cmd = cliwrap.Command(
            executable="cat",
            args=["file"],
        )
        task = cliwrap.Task([cmd], inputs={"file": b"12345"})
        future = pool.submit(cliwrap.run, task, context)
        assert future.result().commands[0].stdout == b"12345"

        future = pool.submit(cliwrap.run, task, context)
        assert future.result().commands[0].stdout == b"12345"
        assert future.result().cached

        # do many at once
        cmd = cliwrap.Command(
            executable="sleep",
            args=["0.3"],
        )
        task = cliwrap.Task([cmd])
        result = cliwrap.run(task, context)
        assert result.commands[0].returncode == 0

        futures = []
        for _ in range(10):
            futures += [pool.submit(cliwrap.run, task, context)]

        results = []
        for future in futures:
            results += [future.result()]

    for r in results:
        assert r.cached


# def test_dask_task_cache_missing() -> None:
#     """Test Dask execution without cache."""
#     cliwrap.un_setup_cache()
#     pool = Client()

#     cmd = cliwrap.Command(
#         executable="cat",
#         args=["file"],
#     )
#     assert cliwrap.__CACHE is None
#     task = cliwrap.Task([cmd], inputs={"file": b"12345"})
#     future = pool.submit(cliwrap.run, task)
#     assert future.result().commands[0].stdout == b"12345"


if __name__ == "__main__":
    pass
