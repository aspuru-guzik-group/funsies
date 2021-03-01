"""Test inference of function types."""
# from __future__ import annotations

from typing import Tuple, Dict

import pytest

# module
from funsies import _infer, types


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

    # assert _infer.output_types(test_fun) == (types.Encoding.blob,)
    # with pytest.raises(TypeError):
    #     assert _infer.output_types(test_fun4) is None

    # assert _infer.output_types(test_fun2) == ()
    # assert _infer.output_types(test_fun3) == _infer.output_types(test_fun3b)
    assert _infer.output_types(test_fun5) == (
        types.Encoding.blob,
        types.Encoding.json,
        types.Encoding.json,
        types.Encoding.blob,
    )
    with pytest.raises(TypeError):
        assert _infer.output_types(test_fun6) is None


test_infer()
