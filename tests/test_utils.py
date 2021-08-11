"""Test of Funsies utility functions."""
# std
from typing import List

# external
import pytest

# funsies
from funsies import errors, Fun, morph, options, put, take, utils
from funsies._run import run_op
from funsies.config import MockServer
from funsies.types import Error, ErrorKind, UnwrapError


def test_concat() -> None:
    """Test concatenation."""
    with Fun(MockServer()) as db:
        dat1 = put(b"bla")
        dat2 = put(b"bla")
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


def test_truncate() -> None:
    """Test truncation."""
    with Fun(MockServer()) as db:
        inp = "\n".join([f"{k}" for k in range(10)])
        dat1 = put(inp.encode())
        trunc = utils.truncate(dat1, 2, 3)
        run_op(db, trunc.parent)
        assert take(trunc) == ("\n".join(inp.split("\n")[2:-3])).encode()


def test_exec_all() -> None:
    """Test execute_all."""
    with Fun(MockServer(), defaults=options(distributed=False)):
        results = []

        def div_by(x: float) -> float:
            return 10.0 / x

        for i in range(10, -1, -1):
            val = put(float(i))
            results += [morph(div_by, val)]

        with pytest.raises(UnwrapError):
            take(results[0])

        err = utils.execute_all(results)
        print(take(results[0]))
        v = take(err, strict=False)
        assert isinstance(v, Error)
        assert v.kind == ErrorKind.ExceptionRaised
