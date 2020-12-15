"""Tests for commandline wrapper."""
# stdlib
import tempfile

# external
from diskcache import FanoutCache

# module
from funsies import (
    add_file,
    CachedFile,
    FileType,
    CacheSpec,
    get_file,
    open_cache,
)


def test_file_getset() -> None:
    """Test file setting and getting."""
    with tempfile.TemporaryDirectory() as t:
        c = CacheSpec(t)
        cache = open_cache(c)
        fid = CachedFile(0, FileType.FILE_INPUT, "bla")
        val = add_file(cache, fid, b"blabla")
        assert val

        val = get_file(cache, fid)
        assert val == b"blabla"


def test_file_getset_errs() -> None:
    """Test file setting and getting errors."""
    with tempfile.TemporaryDirectory() as t:
        c = CacheSpec(t)
        cache = open_cache(c)
        assert isinstance(cache, FanoutCache)
        fid = CachedFile(0, FileType.FILE_INPUT, "bla")
        val = get_file(cache, fid)
        assert val is None

        val = add_file(cache, fid, b"")
        assert val is fid
        n = len(cache)

        val = add_file(cache, fid, b"")
        assert val is fid
        assert len(cache) == n

        fid = CachedFile(0, FileType.FILE_INPUT, "blabla")
        val = add_file(cache, fid, None)
        assert val is fid
        val = get_file(cache, fid)
        assert val is not None
        assert val == b""
