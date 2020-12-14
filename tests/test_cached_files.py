"""Tests for commandline wrapper."""
# stdlib
import tempfile

# module
from funsies import add_file, Cache, CachedFile, CachedFileType, get_file, open_cache


def test_file_getset() -> None:
    """Test file setting and getting."""
    with tempfile.TemporaryDirectory() as t:
        c = Cache(t)
        cache = open_cache(c)
        fid = CachedFile(0, CachedFileType.FILE_INPUT, "bla")
        val = add_file(cache, fid, b"blabla")
        assert val

        val = get_file(cache, fid)
        assert val == b"blabla"


def test_file_getset_errs() -> None:
    """Test file setting and getting errors."""
    with tempfile.TemporaryDirectory() as t:
        c = Cache(t)
        cache = open_cache(c)
        fid = CachedFile(0, CachedFileType.FILE_INPUT, "bla")
        val = get_file(cache, fid)
        assert val is None

        val = add_file(cache, fid, "")
        assert val is fid
        n = len(cache)

        val = add_file(cache, fid, "")
        assert val is fid
        assert len(cache) == n
