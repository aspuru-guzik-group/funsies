"""Test of Funsies utility functions."""
# std
import pickle
from typing import List, Tuple, Union

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import errors, Fun, mapping, morph, put, reduce, take, utils
from funsies.run import run_op


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


def test_pickled() -> None:
    """Test concatenation."""
    with Fun(Redis()) as db:
        dat1 = put(pickle.dumps("bla"))
        fun = utils.pickled(lambda x: x.upper() + x)
        out = morph(fun, dat1)
        run_op(db, out.parent)
        assert pickle.loads(take(out)) == "BLAbla"

        def sum_integers(*integers: Union[int, bytes]) -> Tuple[int, int]:
            out1 = [i for i in integers if isinstance(i, int)]
            out2 = [int(b.decode()) for b in integers if isinstance(b, bytes)]
            return sum(out1), sum(out2)

        def sum_integers_1(*integers: Union[int, bytes]) -> int:
            out1 = [i for i in integers if isinstance(i, int)]
            out2 = [int(b.decode()) for b in integers if isinstance(b, bytes)]
            return sum(out1) + sum(out2)

        int1 = put(pickle.dumps(1))
        int8 = put(pickle.dumps(8))
        str5 = put("5")
        str9 = put("9")
        fun = utils.pickled(sum_integers, 2)
        fun2 = utils.pickled(sum_integers_1)
        out_int, out_bytes = mapping(fun, int1, str5, int8, str9, noutputs=2)
        out_both = reduce(fun2, int1, str5, int8, str9)
        run_op(db, out_int.parent)
        run_op(db, out_both.parent)
        assert pickle.loads(take(out_int)) == 9
        assert pickle.loads(take(out_bytes)) == 14
        assert pickle.loads(take(out_both)) == 23
