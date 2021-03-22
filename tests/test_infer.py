"""Test inference of function types."""
# from __future__ import annotations

# std
from typing import Dict, Tuple

# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# funsies
import funsies as f
import funsies._infer as _infer
import funsies.types as types


def test_infer() -> None:
    """Test inference."""
    # not that annotations from __future__ will break this on python 3.7

    def test_fun(a: int) -> bytes:
        ...

    def test_fun2(a: int, b: float) -> None:
        ...

    def test_fun3(a: int, b: int) -> Tuple[int, int]:
        ...

    def test_fun3b(a: int, b: int) -> Tuple[int, int]:
        ...

    def test_fun4(a: int):  # noqa
        ...

    def test_fun5(a: int, b: int) -> Tuple[bytes, str, Dict[str, str], bytes]:
        ...

    def test_fun6(a: int, b: int) -> Tuple[int, ...]:
        ...

    assert _infer.output_types(test_fun) == (types.Encoding.blob,)
    with pytest.raises(TypeError):
        assert _infer.output_types(test_fun4) is None
    assert _infer.output_types(test_fun2) == ()
    assert _infer.output_types(test_fun3) == _infer.output_types(test_fun3b)
    assert _infer.output_types(test_fun5) == (
        types.Encoding.blob,
        types.Encoding.json,
        types.Encoding.json,
        types.Encoding.blob,
    )
    with pytest.raises(TypeError):
        assert _infer.output_types(test_fun6) is None


def test_infer_errs() -> None:
    """Test inference applied to functions."""
    db = Redis()
    with f.Fun(db):
        a = f.put(b"bla bla")
        b = f.put(3)
        with pytest.raises(TypeError):
            f.py(lambda x, y, z: (x, y), a, a, b)

        # should NOT raise
        f.py(
            lambda x, y, z: (x, y),
            a,
            a,
            b,
            out=[types.Encoding.blob, types.Encoding.blob],
        )
