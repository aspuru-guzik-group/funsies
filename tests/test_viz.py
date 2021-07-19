"""Test of visualization routines."""
from __future__ import annotations

# std
from typing import Any, Sequence

# external
from fakeredis import FakeStrictRedis as Redis

# funsies
import funsies
from funsies import (
    _dag,
    _graphviz,
    dynamic,
    execute,
    Fun,
    morph,
    options,
    put,
    reset,
    shell,
    utils,
    wait_for,
)
from funsies.types import Artefact, Encoding


def raises(k: bytes) -> bytes:
    """A function that raises."""
    raise RuntimeError()


def upper(k: bytes) -> bytes:
    """A function that capitalizes."""
    return k.decode().upper().encode()


def test_dag_dump() -> None:
    """Test simple DAG dump to file."""
    with Fun(Redis(), options(distributed=False)) as db:
        dat = put(b"bla bla")
        dat2 = put(b"blaXbla")
        errorstep = morph(raises, dat2)
        step1 = morph(upper, dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        step2b = utils.concat(step2.stdout, errorstep, strict=False)
        step3 = shell("cat file1", inp=dict(file1=step2b))

        step4 = shell("cat file1", inp=dict(file1=step1))
        step4b = shell("cat file2", inp=dict(file2=step4.stdout))

        out = utils.concat(step1, dat, step2.stdout, step3.stdout)

        _dag.build_dag(db, out.hash)
        execute(step2b)
        execute(step4b)
        wait_for(step4b)
        reset(step4)

        nodes, artefacts, labels, links = _graphviz.export(db, [out.hash, step4b.hash])
        dot = _graphviz.format_dot(
            nodes, artefacts, labels, links, [out.hash, step4b.hash]
        )
        assert len(dot) > 0
        assert len(nodes) == 8
        assert len(labels) == 8

        # TODO pass through dot for testing?
        with open("g.dot", "w") as f:
            f.write(dot)


def test_dynamic_dump() -> None:
    """Test whether a dynamic DAG gets graphed properly."""

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

    def apply(inp: Artefact[dict[str, Any]]) -> Artefact[str]:
        out = funsies.morph(lambda x: f"{x['sum']}//{x['product']}", inp)
        return out

    def combine(inp: Sequence[Artefact[str]]) -> Artefact[bytes]:
        def enc(inp: str) -> bytes:
            return inp.encode()

        out = [funsies.morph(enc, x, out=Encoding.blob) for x in inp]
        return funsies.utils.concat(*out)

    with funsies.ManagedFun(nworkers=1) as db:
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
        outputs = funsies.morph(lambda x: x, outputs)
        nodes, artefacts, labels, links = _graphviz.export(db, [outputs.hash])
        assert len(artefacts) == 4  # not yet generated subdag parents
        print(len(artefacts))
        funsies.execute(outputs)
        funsies.wait_for(outputs, timeout=1.0)
        nodes, artefacts, labels, links = _graphviz.export(db, [outputs.hash])
        assert len(artefacts) == 22  # generated subdag parents
        assert funsies.take(outputs) == b"12//1112//2014//3314//4016//55"
