"""Test of parametric funsies."""
from __future__ import annotations

# external
from fakeredis import FakeStrictRedis as Redis

# funsies
from funsies import execute, Fun, morph, options, put, reduce, take
import funsies.parametric as p


def test_parametric_store_recall() -> None:
    """Test storing and recalling parametrics."""
    db = Redis()
    with Fun(db, options(distributed=False)):
        a = put(3)
        b = put(4)

        s = reduce(lambda x, y: x + y, a, b)
        s2 = morph(lambda x: 3 * x, s)

        execute(s2)
        assert take(s2) == 21

        # parametrize
        p.commit("math", inp=dict(a=a, b=b), out=dict(s=s, s2=s2))

    with Fun(db, options(distributed=False)):
        out = p.recall("math", inp=dict(a=5, b=8))
        execute(out["s2"])
        assert take(out["s2"]) == 39


def test_parametric_store_recall_optional() -> None:
    """Test storing a parametric with optional parameters."""
    db = Redis()
    with Fun(db, options(distributed=False)):
        a = put(3)
        b = put("fun")

        s = reduce(lambda x, y: x * y, a, b)
        s2 = morph(lambda x: x.upper(), s)

        # parametrize
        p.commit("fun", inp=dict(a=a, b=b), out=dict(s=s2))

    with Fun(db, options(distributed=False)):
        out = p.recall("fun", inp=dict(a=5))
        execute(out["s"])
        assert take(out["s"]) == "FUNFUNFUNFUNFUN"

        # nested
        out = p.recall("fun", inp=dict(b="lol"))
        out = p.recall("fun", inp=dict(b=out["s"], a=2))
        execute(out["s"])
        assert take(out["s"]) == "LOLLOLLOLLOLLOLLOL"
