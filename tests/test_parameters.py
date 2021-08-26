"""Tests of funsies parametrization."""
from __future__ import annotations

# funsies
from funsies import execute, Fun, morph, options, put, shell, take
import funsies._parametrize as _p
from funsies.config import MockServer
from funsies.utils import concat


def capitalize(inp: bytes) -> bytes:
    """Capitalize."""
    return inp.upper()


def test_subgraph() -> None:
    """Test that we can isolate the required operators for parametrization."""
    with Fun(MockServer(), options(distributed=False)) as db:
        dat = put(b"bla bla")
        step1 = morph(capitalize, dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))

        # random not included ops
        stepA = shell("echo 'bla'")
        _ = concat(dat, dat)
        _ = morph(capitalize, b"another word")

        final = shell(
            "cat file1 file2", inp={"file1": stepA.stdout, "file2": step2.stdout}
        )

        ops = _p._parametrize_subgraph(db, {"input": dat}, {"output": final.stdout})
        assert len(ops) == 3
        assert step1.parent in ops
        assert step2.hash in ops
        assert final.hash in ops

        # get edges
        edges = _p._subgraph_edges(db, ops)
        print(edges)


def test_toposort() -> None:
    """Test that we can topologically sort the subset."""
    with Fun(MockServer(), options(distributed=False)) as db:
        dat = put(b"bla bla")
        step1 = morph(capitalize, dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))

        # random not included ops
        stepA = shell("echo 'bla'")
        _ = concat(dat, dat)
        _ = morph(capitalize, b"another word")

        final = shell(
            "cat file1 file2", inp={"file1": stepA.stdout, "file2": step2.stdout}
        )

        ops = _p._parametrize_subgraph(db, {"input": dat}, {"output": final.stdout})
        edges = _p._subgraph_edges(db, ops)
        sorted_ops = _p._subgraph_toposort(ops, edges)
        assert sorted_ops[0] == step1.parent
        assert sorted_ops[1] == step2.hash
        assert sorted_ops[2] == final.hash


def test_parametrize() -> None:
    """Test that parametrization works."""
    with Fun(MockServer(), options(distributed=False)) as db:
        dat = put(b"bla bla")
        dat2 = put(b"bla bla bla")

        step1 = morph(capitalize, dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        final = shell("cat file1 file3", inp={"file1": step1, "file3": step2.stdout})

        pinp = {"input": dat}
        pout = {"final.stdout": final.stdout, "step1": step1}
        new_inp = {"input": dat2}

        ops = _p._parametrize_subgraph(db, pinp, pout)
        edges = _p._subgraph_edges(db, ops)
        sorted_ops = _p._subgraph_toposort(ops, edges)
        pinp2 = dict([(k, v.hash) for k, v in pinp.items()])
        pout2 = dict([(k, v.hash) for k, v in pout.items()])
        new_out = _p._do_parametrize(db, sorted_ops, pinp2, pout2, new_inp)

        # re-run with dat2, check if the same.
        step1 = morph(capitalize, dat2)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat2))
        final = shell("cat file1 file3", inp={"file1": step1, "file3": step2.stdout})

        assert new_out["final.stdout"] == final.stdout
        assert new_out["step1"] == step1


def test_parametric() -> None:
    """Test that parametric DAGs work."""
    with Fun(MockServer(), options(distributed=False)) as db:
        dat = put(b"bla bla")
        step1 = morph(capitalize, dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        final = shell("cat file1 file3", inp={"file1": step1, "file3": step2.stdout})

        param = _p.make_parametric(
            db, "param", {"input": dat}, {"output": final.stdout}
        )
        param2 = _p.Parametric.grab(db, param.hash)
        assert param == param2


def test_parametric_eval() -> None:
    """Test that parametric evaluate properly."""
    with Fun(MockServer(), options(distributed=False)) as db:
        dat = put(b"bla bla")
        step1 = morph(capitalize, dat)
        step2 = shell("cat file1 file2", inp=dict(file1=step1, file2=dat))
        final = shell("cat file1 file3", inp={"file1": step1, "file3": step2.stdout})
        execute(final.stdout)
        # b'BLA BLABLA BLAbla bla'

        param = _p.make_parametric(
            db, "param", {"input": dat}, {"output": final.stdout}
        )
        dat2 = put(b"lol lol")
        out = param.evaluate(db, {"input": dat2})
        execute(out["output"])
        assert take(out["output"]) == b"LOL LOLLOL LOLlol lol"
