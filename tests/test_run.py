"""Test running a funsie."""
# std
from typing import Dict

# external
from fakeredis import FakeStrictRedis as Redis

# funsies
from funsies import _graph
from funsies import _pyfunc as p
from funsies import _shell as s
from funsies import _subdag as sub
from funsies import options
from funsies._run import run_op
from funsies.types import Encoding, RunStatus


def capitalize(inputs: Dict[str, bytes]) -> Dict[str, bytes]:
    """Capitalize artifacts."""
    out = {}
    for key, val in inputs.items():
        out[key] = val.upper()
    return out


def uncapitalize(inputs: Dict[str, bytes]) -> Dict[str, bytes]:
    """Uncapitalize artifacts."""
    out = {}
    for key, val in inputs.items():
        out[key] = val.decode().lower().encode()
    return out


def test_shell_run() -> None:
    """Test run on a shell command."""
    opt = options()
    db = Redis()
    cmd = s.shell_funsie(["cat file1"], {"file1": Encoding.blob}, [])
    inp = {"file1": _graph.constant_artefact(db, b"bla bla")}
    operation = _graph.make_op(db, cmd, inp, opt)
    status = run_op(db, operation.hash)

    # test return values
    assert status == RunStatus.executed

    # check data is good
    dat = _graph.get_data(db, _graph.Artefact[bytes].grab(db, operation.inp["file1"]))
    assert dat == b"bla bla"

    dat = _graph.get_data(
        db, _graph.Artefact[bytes].grab(db, operation.out[f"{s.STDOUT}0"])
    )
    assert dat == b"bla bla"


def test_pyfunc_run() -> None:
    """Test run on a python function."""
    db = Redis()
    opt = options()
    cmd = p.python_funsie(
        capitalize, {"inp": Encoding.json}, {"inp": Encoding.json}, name="capit"
    )
    inp = {"inp": _graph.constant_artefact(db, "bla bla")}
    operation = _graph.make_op(db, cmd, inp, opt)
    status = run_op(db, operation.hash)

    # test return values
    assert status == RunStatus.executed

    # check data is good
    dat = _graph.get_data(db, _graph.Artefact[str].grab(db, operation.inp["inp"]))
    assert dat == "bla bla"

    dat = _graph.get_data(db, _graph.Artefact[str].grab(db, operation.out["inp"]))
    assert dat == "BLA BLA"


def test_cached_run() -> None:
    """Test cached result."""
    db = Redis()
    opt = options()
    cmd = p.python_funsie(
        capitalize, {"inp": Encoding.json}, {"inp": Encoding.json}, name="capit"
    )
    inp = {"inp": _graph.constant_artefact(db, "bla bla")}
    operation = _graph.make_op(db, cmd, inp, opt)
    status = run_op(db, operation.hash)

    # test return values
    assert status == RunStatus.executed
    status = run_op(db, operation.hash)
    assert status == RunStatus.using_cached


def test_cached_instances() -> None:
    """Test cached result from running same code twice."""
    db = Redis()
    opt = options()
    cmd = p.python_funsie(
        capitalize, {"inp": Encoding.json}, {"inp": Encoding.json}, name="capit"
    )
    inp = {"inp": _graph.constant_artefact(db, "bla bla")}
    operation = _graph.make_op(db, cmd, inp, opt)
    status = run_op(db, operation.hash)
    # test return values
    assert status == RunStatus.executed

    cmd = p.python_funsie(
        capitalize, {"inp": Encoding.json}, {"inp": Encoding.json}, name="capit"
    )
    inp = {"inp": _graph.constant_artefact(db, "bla bla")}
    operation = _graph.make_op(db, cmd, inp, opt)
    status = run_op(db, operation.hash)
    assert status == RunStatus.using_cached


def test_dependencies() -> None:
    """Test cached result."""
    db = Redis()
    opt = options()
    cmd = p.python_funsie(
        capitalize, {"inp": Encoding.json}, {"inp": Encoding.json}, name="capit"
    )

    cmd2 = p.python_funsie(
        uncapitalize, {"inp": Encoding.json}, {"inp": Encoding.json}, name="uncap"
    )
    operation = _graph.make_op(
        db, cmd, inp={"inp": _graph.constant_artefact(db, "bla bla")}, opt=opt
    )
    operation2 = _graph.make_op(
        db, cmd2, inp={"inp": _graph.Artefact.grab(db, operation.out["inp"])}, opt=opt
    )

    status = run_op(db, operation2.hash)
    assert status == RunStatus.unmet_dependencies

    status = run_op(db, operation.hash)
    assert status == RunStatus.executed

    status = run_op(db, operation2.hash)
    assert status == RunStatus.executed


def test_subdag() -> None:
    """Test run of a subdag function."""
    # funsies
    import funsies as f

    db = Redis()
    opt = options()
    with f.Fun(db):

        def map_reduce(inputs: Dict[str, bytes]) -> Dict[str, _graph.Artefact]:
            """Basic map reduce."""
            inp_data = inputs["inp"].split(b" ")
            for el in inp_data:
                out = f.morph(lambda x: x.upper(), el, opt=options())
            return {"out": f.utils.concat(out, join="-")}

        cmd = sub.subdag_funsie(
            map_reduce, {"inp": Encoding.blob}, {"out": Encoding.blob}
        )
        inp = {"inp": _graph.constant_artefact(db, b"bla bla blo lol")}
        operation = _graph.make_op(db, cmd, inp, opt)
        status = run_op(db, operation.hash)

        # test return values
        assert status == RunStatus.subdag_ready

        # test output data
        dat = _graph.get_data(
            db,
            _graph.Artefact[bytes].grab(db, operation.out["out"]),
            do_resolve_link=False,
        )
        assert isinstance(dat, f.errors.Error)
        assert dat.kind == "UnresolvedLink"

        datl = _graph.get_data(
            db,
            _graph.Artefact[bytes].grab(db, operation.out["out"]),
            do_resolve_link=True,
        )
        assert isinstance(datl, f.errors.Error)
        assert datl.kind == "NotFound"
