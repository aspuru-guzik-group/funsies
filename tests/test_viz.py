"""Test of visualization routines."""
from __future__ import annotations

# std

# external
from fakeredis import FakeStrictRedis as Redis

# module
from funsies import (
    dag,
    execute,
    Fun,
    graphviz,
    morph,
    options,
    put,
    reset,
    shell,
    utils,
    wait_for,
)


def raises(k: bytes) -> bytes:
    """A function that raises."""
    raise RuntimeError()


def upper(k: bytes) -> bytes:
    """A function that capitalizes."""
    return k.decode().upper().encode()


def test_dag_dump() -> None:
    """Test simple DAG dump to file."""
    with Fun(Redis(), options(distributed=False)) as db:
        dat = put("bla bla")
        dat2 = put("blaXbla")
        errorstep = morph(raises, dat2)
        step1 = morph(upper, dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        step2b = utils.concat(step2.stdout, errorstep, strict=False)
        step3 = shell("cat file1", inp=dict(file1=step2b))

        step4 = shell("cat file1", inp=dict(file1=step1))
        step4b = shell("cat file2", inp=dict(file2=step4.stdout))

        out = utils.concat(step1, dat, step2.stdout, step3.stdout)

        dag.build_dag(db, out.hash)
        execute(step2b)
        execute(step4b)
        wait_for(step4b)
        reset(step4)

        nodes, artefacts, labels = graphviz.export(db, [out.hash, step4b.hash])
        dot = graphviz.format_dot(nodes, artefacts, labels, [out.hash, step4b.hash])
        assert len(dot) > 0

        # TODO pass through dot for testing?
        with open("g.dot", "w") as f:
            f.write(dot)


test_dag_dump()
