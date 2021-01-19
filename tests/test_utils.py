"""Test of Funsies utility functions."""
# std
from typing import List

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import errors, Fun, put, run_op, take, utils


def test_concat() -> None:
    """Test concatenation."""
    with Fun(Redis()) as db:
        dat1 = put("bla")
        dat2 = put("bla")
        cat = utils.concat(dat1, dat2)
        run_op(db, cat.parent)
        assert take(cat) == b"blabla"

        cat = utils.concat(dat1, dat1, dat1, join=b" ")
        run_op(db, cat.parent)
        assert take(cat) == b"bla bla bla"


def test_match() -> None:
    """Test error matching."""
    results = [b"bla bla", errors.Error(kind=errors.ErrorKind.NotFound)]
    assert utils.match_results(results, lambda x: x) == [b"bla bla"]

    def unity(x: bytes) -> bytes:
        return x

    def err(x: errors.Error) -> errors.ErrorKind:
        return x.kind

    results2: List[errors.Result[bytes]] = [
        b"bla bla",
        errors.Error(kind=errors.ErrorKind.NotFound),
    ]
    assert utils.match_results(results2, unity, err) == [
        b"bla bla",
        errors.ErrorKind.NotFound,
    ]
