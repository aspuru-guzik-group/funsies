"""Tests of dynamic funsies."""
from __future__ import annotations

# std
from typing import Sequence

# external
import pytest

# funsies
import funsies
from funsies import dynamic
from funsies.config import MockServer
from funsies.types import Artefact, Encoding


def test_map_reduce() -> None:
    """Test simple map-reduce."""

    def split(a: bytes, b: bytes) -> list[dict[str, int]]:
        a = a.split()
        b = b.split()
        out = []
        for ia, ib in zip(a, b):
            out += [
                {
                    "sum": int(ia.decode()) + int(ib.decode()),
                    "product": int(ia.decode()) * int(ib.decode()),
                }
            ]
        return out

    def apply(inp: Artefact) -> Artefact:
        out = funsies.morph(lambda x: f"{x['sum']}//{x['product']}", inp)
        return out

    def combine(inp: Sequence[Artefact]) -> Artefact:
        out = [funsies.morph(lambda y: y.encode(), x, out=Encoding.blob) for x in inp]
        return funsies.utils.concat(*out)

    with funsies.Fun(MockServer(), funsies.options(distributed=False)):
        num1 = funsies.put(b"1 2 3 4 5")
        num2 = funsies.put(b"11 10 11 10 11")

        outputs = dynamic.sac(
            split,
            apply,
            combine,
            num1,
            num2,
            out=Encoding.blob,
        )
        funsies.execute(outputs)
        assert funsies.take(outputs) == b"12//1112//2014//3314//4016//55"


@pytest.mark.slow
@pytest.mark.parametrize("nworkers", [1, 2])
def test_nested_map_reduce(nworkers: int) -> None:
    """Test nested map-reduce."""
    # ------------------------------------------------------------------------
    # Inner
    def sum_inputs(*inp: int) -> int:
        out = 0
        for el in inp:
            out += el
        return out

    def split_inner(inp: str) -> list[int]:
        a = inp.split(" ")
        return [int(el) for el in a]

    def apply_inner(inp: Artefact) -> Artefact:
        return funsies.reduce(sum_inputs, inp, 1)

    def combine_inner(inp: Sequence[Artefact]) -> Artefact:
        return funsies.reduce(sum_inputs, *inp)

    # ------------------------------------------------------------------------
    # outer
    def split_outer(inp: list[str], fac: int) -> list[str]:
        out = [x + f" {fac}" for x in inp]
        return out

    def apply_outer(inp: Artefact) -> Artefact:
        outputs = dynamic.sac(
            split_inner,
            apply_inner,
            combine_inner,
            inp,
            out=Encoding.json,
        )
        return outputs

    def combine_outer(inp: Sequence[Artefact]) -> Artefact:
        out = [
            funsies.morph(lambda y: f"{y}".encode(), x, out=Encoding.blob) for x in inp
        ]
        return funsies.utils.concat(*out, join=b",,")

    with funsies.ManagedFun(nworkers=nworkers):
        num1 = funsies.put("1 2 3 4 5")
        outputs = dynamic.sac(
            split_inner, apply_inner, combine_inner, num1, out=Encoding.json
        )
        funsies.execute(outputs)
        funsies.wait_for(outputs, timeout=30.0)
        assert funsies.take(outputs) == 20

        # Now try the nested one
        num = funsies.put(["1 2", "3 4 7", "10 12", "1"])
        factor = funsies.put(-2)
        # split -> 1 2 -2|3 4 7 -2|10 12 -2| 1 -2
        # apply -> split2 -> 1, 2,-2 | 3,4,7,-2|10,12,-2|1,-2
        # apply2 -> 2, 3,-1 | 4,5,8,-1|11,13,-1|2,-1
        # combine2 -> 4|16|23|1
        # combine -> 4,,16,,23,,1
        ans = b"4,,16,,23,,1"

        outputs = dynamic.sac(
            split_outer,
            apply_outer,
            combine_outer,
            num,
            factor,
            out=Encoding.blob,
        )
        funsies.execute(outputs)
        funsies.wait_for(outputs, timeout=30.0)
        assert funsies.take(outputs) == ans


@pytest.mark.slow
def test_waiting_on_map_reduce() -> None:
    """Test waiting on the (linked) result of map-reduce."""

    def split(a: bytes, b: bytes) -> list[dict[str, int]]:
        a = a.split()
        b = b.split()
        out = []
        for ia, ib in zip(a, b):
            out += [
                {
                    "sum": int(ia.decode()) + int(ib.decode()),
                    "product": int(ia.decode()) * int(ib.decode()),
                }
            ]
        return out

    def apply(inp: Artefact) -> Artefact:
        out = funsies.morph(lambda x: f"{x['sum']}//{x['product']}", inp)
        return out

    def combine(inp: Sequence[Artefact]) -> Artefact:
        out = [funsies.morph(lambda y: y.encode(), x, out=Encoding.blob) for x in inp]
        return funsies.utils.concat(*out)

    with funsies.ManagedFun(nworkers=1):
        num1 = funsies.put(b"1 2 3 4 5")
        num2 = funsies.put(b"11 10 11 10 11")

        outputs = dynamic.sac(
            split,
            apply,
            combine,
            num1,
            num2,
            out=Encoding.blob,
        )
        funsies.execute(outputs)
        funsies.wait_for(outputs, timeout=1.0)
        assert funsies.take(outputs) == b"12//1112//2014//3314//4016//55"
