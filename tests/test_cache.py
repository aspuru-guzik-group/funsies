"""Tests of caching functionality."""
# std
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import tempfile
from typing import Union

# external
import pytest

# package
from funsies import Cache, Command, get_file, open_cache, run, Task


def test_task_cache() -> None:
    """Test task caching."""
    with tempfile.TemporaryDirectory() as tmpd:
        cache_id = Cache(path=tmpd, shards=1)
        cmd = Command(
            executable="cat",
            args=["file"],
        )
        task = Task([cmd], inputs={"file": b"12345"})
        results = run(cache_id, task)

        cache = open_cache(cache_id)
        assert get_file(cache, results.commands[0].stdout) == b"12345"

        assert cache["id"] == 1
        i = 0
        for _ in cache:
            i = i + 1

        # Cache should remain the same length
        results = run(cache_id, task)
        assert get_file(cache, results.commands[0].stdout) == b"12345"

        assert cache["id"] == 1
        j = 0
        for _ in cache:
            j = j + 1

        assert i == j


def test_task_cache_error() -> None:
    """Test cache failures."""
    with tempfile.TemporaryDirectory() as tmpd:
        cache_id = Cache(path=tmpd, shards=1)

        cmd = Command(
            executable="cat",
            args=["file"],
        )

        cache = open_cache(cache_id)

        task = Task([cmd], inputs={"file": b"12345"})
        results = run(cache_id, task)
        assert get_file(cache, results.commands[0].stdout) == b"12345"

        # manipulate the cache
        for key in cache:
            if "TaskOutput" in key:
                cache[key] = 3.0

        results = run(cache_id, task)
        assert get_file(cache, results.commands[0].stdout) == b"12345"


@pytest.mark.parametrize("Executor", [ThreadPoolExecutor, ProcessPoolExecutor])
@pytest.mark.parametrize("nworkers,njobs", [(1, 1), (1, 3), (3, 1), (3, 8), (10, 40)])
def test_task_cache_mp(
    Executor: Union[ThreadPoolExecutor, ProcessPoolExecutor], nworkers: int, njobs: int
) -> None:
    """Test caching in multiprocessing context."""
    with tempfile.TemporaryDirectory() as tmpd:
        cache_id = Cache(path=tmpd, shards=nworkers)

        cmd = Command(
            executable="sleep",
            args=["0.03"],
        )

        task = Task([cmd], inputs={"file": b"12345"})
        results = []
        with Executor(max_workers=nworkers) as t:
            results += [t.submit(run, cache_id, task) for k in range(njobs)]
            outputs = [r.result() for r in results]

        ids = []
        for o in outputs:
            # no errors are raised
            assert o.raises is None
            ids += [o.task_id]

        # There shouldn't be more tasks actually runned then there are workers
        # (and usually, less)
        assert len(set(ids)) <= nworkers


@pytest.mark.parametrize("Executor", [ThreadPoolExecutor, ProcessPoolExecutor])
@pytest.mark.parametrize("nworkers,njobs", [(1, 1), (1, 3), (3, 1), (3, 8), (10, 40)])
def test_task_cache_always1(
    Executor: Union[ThreadPoolExecutor, ProcessPoolExecutor], nworkers: int, njobs: int
) -> None:
    """Test caching in multiprocessing context."""
    with tempfile.TemporaryDirectory() as tmpd:
        cache_id = Cache(path=tmpd, shards=nworkers)

        cmd = Command(
            executable="sleep",
            args=["0.03"],
        )

        task = Task([cmd], inputs={"file": b"12345"})
        results = []
        run(cache_id, task)  # making a cache entry already
        with Executor(max_workers=nworkers) as t:
            results += [t.submit(run, cache_id, task) for k in range(njobs)]
            outputs = [r.result() for r in results]

        for o in outputs:
            # no errors are raised
            assert o.raises is None
            assert o.task_id == 1


if __name__ == "__main__":
    pass
