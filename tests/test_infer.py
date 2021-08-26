"""Test inference of function types."""
# from __future__ import annotations

# std
from typing import Dict, Tuple

# external
import pytest

# funsies
import funsies as f
import funsies._infer as _infer
from funsies.config import MockServer
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
    assert _infer.output_types(test_fun2) == (types.Encoding.json,)
    assert _infer.output_types(test_fun3) == _infer.output_types(test_fun3b)
    assert _infer.output_types(test_fun5) == (
        types.Encoding.blob,
        types.Encoding.json,
        types.Encoding.json,
        types.Encoding.blob,
    )
    with pytest.raises(TypeError):
        assert _infer.output_types(test_fun6) is None


def test_infer_forward_ref() -> None:
    """Test inference of unkown forward refs."""

    def test_fun7(a: "Thing", b: int) -> float:  # type:ignore # noqa
        ...

    def test_fun7b(a: "Thing", b: int) -> Dict[float, "what"]:  # type:ignore # noqa
        ...

    assert _infer.output_types(test_fun7) == (types.Encoding.json,)
    assert _infer.output_types(test_fun7b) == (types.Encoding.json,)


def test_infer_errs() -> None:
    """Test inference applied to functions."""
    with f.Fun(MockServer()):
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

        def i1o2(x: bytes) -> Tuple[bytes, bytes]:
            return x, x

        def i2o1(x: bytes, y: bytes) -> bytes:
            return x

        with pytest.raises(TypeError):
            out = f.morph(i1o2, a)  # type:ignore # noqa:F841

        with pytest.raises(TypeError):
            out = f.reduce(i1o2, a)  # type:ignore # noqa:F841

        with pytest.raises(TypeError):
            out = f.reduce(lambda x, y: x, a, b)  # type:ignore # noqa:F841

        # If we pass out= then the inference is skipped
        out = f.morph(i1o2, a, out=types.Encoding.blob)  # type:ignore # noqa:F841
        out = f.reduce(i1o2, a, out=types.Encoding.blob)  # type:ignore # noqa:F841
