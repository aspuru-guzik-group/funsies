"""Tests of dynamic funsies."""
from __future__ import annotations

# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# module
import funsies
from funsies import dynamic
from funsies.types import Artefact


def test_map_reduce() -> None:
    """Test simple map-reduce."""

    def split(inp: dict[str, bytes]) -> list[dict[str, bytes]]:
        a = inp["a"].split()
        b = inp["b"].split()
        out = []
        for ia, ib in zip(a, b):
            out += [
                {
                    "sum": str(int(ia.decode()) + int(ib.decode())).encode(),
                    "product": str(int(ia.decode()) * int(ib.decode())).encode(),
                }
            ]
        return out

    def apply(inp: dict[str, Artefact]) -> dict[str, Artefact]:
        return {"cat": funsies.utils.concat(inp["sum"], inp["product"], join="//")}

    def combine(inp: list[dict[str, Artefact]]) -> dict[str, Artefact]:
        return {"out": funsies.utils.concat(*[el["cat"] for el in inp], join="\n")}

    with funsies.Fun(Redis(), funsies.options(distributed=False)) as db:
        num1 = funsies.put("1 2 3 4 5")
        num2 = funsies.put("11 10 11 10 11")

        operation = dynamic.map_reduce(
            split, apply, combine, inp={"a": num1, "b": num2}, out=["out"]
        )
        g = Artefact.grab(db, operation.out["out"])
        final = funsies.morph(lambda x: x, g)
        funsies.execute(final)
        print(final.hash)


@pytest.mark.parametrize("nworkers", [1, 8])
def test_nested_map_reduce(nworkers: int) -> None:
    """Test nested map-reduce."""

    def sum_inputs(*inp: bytes) -> bytes:
        out = 0
        for el in inp:
            out += int(el.decode())
        return str(out).encode()

    def split2(inp: dict[str, bytes]) -> list[dict[str, bytes]]:
        a = inp["in"].split(b" ")
        out = [{"out": el} for el in a]
        return out

    def apply2(inp: dict[str, Artefact]) -> dict[str, Artefact]:
        return {"out": funsies.reduce(sum_inputs, inp["out"], "1")}

    def combine2(inp: list[dict[str, Artefact]]) -> dict[str, Artefact]:
        return {"out": funsies.reduce(sum_inputs, *[el["out"] for el in inp])}

    def split(inp: dict[str, bytes]) -> list[dict[str, bytes]]:
        a = inp["in"].split(b"\n")
        out = [{"out": el} for el in a]
        return out

    def apply(inp: dict[str, Artefact]) -> dict[str, Artefact]:
        operation = dynamic.map_reduce(
            split2,
            apply2,
            combine2,
            inp={"in": inp["out"]},
            out=["out"],
        )
        return {"out": Artefact.grab(funsies._context.get_db(), operation.out["out"])}

    def combine(inp: list[dict[str, Artefact]]) -> dict[str, Artefact]:
        return {"out": funsies.utils.concat(*[el["out"] for el in inp], join=",,")}

    with funsies.ManagedFun(nworkers=nworkers) as db:
        num = funsies.put("1 2\n3 4 7\n10 12\n1")
        # split -> 1 2\n 3 4 7\n10 12\n1
        # apply -> split2 -> 1, 2 | 3,4,7|10,12|1
        # apply2 -> 2, 3| 4,5,8|11,13|2
        # combine2 -> 5|17|24|2
        # combine -> 5,,17,,24,,2

        operation = dynamic.map_reduce(
            split, apply, combine, inp={"in": num}, out=["out"]
        )
        g = Artefact.grab(db, operation.out["out"])
        final = funsies.morph(lambda x: x, g)
        funsies.execute(final)
        funsies.wait_for(final)
        assert funsies.take(final) == b"5,,17,,24,,2"
