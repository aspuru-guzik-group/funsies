"""Basic test of diskcache functionality."""
# std
import tempfile

# external
from dask.distributed import Client

# package
from funsies import CacheSettings, Command, Context, run, Task

pool = Client()


def test_task_cache() -> None:
    """Test task caching."""
    with tempfile.TemporaryDirectory() as tmpd:
        context = Context(cache=CacheSettings(path=tmpd, shards=1, timeout=1.0))
        cmd = Command(
            executable="cat",
            args=["file"],
        )
        task = Task([cmd], inputs={"file": b"12345"})
        results = run(task, context)
        assert results.commands[0].stdout == b"12345"

        k = 0
        from funsies.core import __CACHE

        assert __CACHE is not None
        for _ in __CACHE:
            k = k + 1
        assert k == 1

        results = run(task, context)
        assert results.commands[0].stdout == b"12345"
        assert results.cached


def test_task_cache_error() -> None:
    """Test cache failures."""
    with tempfile.TemporaryDirectory() as tmpd:
        context = Context(cache=CacheSettings(path=tmpd, shards=1, timeout=1.0))

        cmd = Command(
            executable="cat",
            args=["file"],
        )

        task = Task([cmd], inputs={"file": b"12345"})
        results = run(task, context)
        assert results.commands[0].stdout == b"12345"

        from funsies.core import __CACHE

        assert __CACHE is not None
        for key in __CACHE:
            __CACHE[key] = 3.0

        results = run(task, context)
        assert results.commands[0].stdout == b"12345"
        assert not results.cached


def test_dask_task_cache() -> None:
    """Test task caching by Dask workers."""
    with tempfile.TemporaryDirectory() as tmpd:
        context = Context(cache=CacheSettings(path=tmpd, shards=4, timeout=1.0))

        cmd = Command(
            executable="cat",
            args=["file"],
        )
        task = Task([cmd], inputs={"file": b"12345"})
        future = pool.submit(run, task, context)
        assert future.result().commands[0].stdout == b"12345"

        future = pool.submit(run, task, context)
        assert future.result().commands[0].stdout == b"12345"
        assert future.result().cached

        # do many at once
        cmd = Command(
            executable="sleep",
            args=["0.3"],
        )
        task = Task([cmd])
        result = run(task, context)
        assert result.commands[0].returncode == 0

        futures = []
        for _ in range(10):
            futures += [pool.submit(run, task, context)]

        results = []
        for future in futures:
            results += [future.result()]

    for r in results:
        assert r.cached


def test_dask_task_nocache() -> None:
    """Test Dask execution without cache."""
    cmd = Command(
        executable="cat",
        args=["file"],
    )

    task = Task([cmd], inputs={"file": b"12345"})
    future = pool.submit(run, task)
    assert future.result().commands[0].stdout == b"12345"


def test_task_cache_failure() -> None:
    """Test Dask execution without cache."""
    cmd = Command(
        executable="cat",
        args=["file"],
    )

    task = Task([cmd], inputs={"file": b"12345"})
    context = Context(cache=CacheSettings(path="", shards=-4, timeout=1.0))
    run(task, context)


if __name__ == "__main__":
    pass
