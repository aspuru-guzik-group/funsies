"""Tests of error handling."""
# std

# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# module
import funsies
from funsies import _graph, Fun, hash_t, options


def test_artefact_add() -> None:
    """Test adding const artefacts."""
    store = Redis()
    a = _graph.constant_artefact(store, b"bla bla")
    b = _graph.get_artefact(store, a.hash)
    assert b is not None
    assert a == b


def test_artefact_load_errors() -> None:
    """Test loading artefact errors."""
    store = Redis()
    with pytest.raises(RuntimeError):
        _ = _graph.get_artefact(store, "bla")  # type:ignore

    # TODO check that warnings are logged?
    _graph.constant_artefact(store, b"bla bla")
    _graph.constant_artefact(store, b"bla bla")

    _graph.variable_artefact(store, hash_t("1"), "file")
    _graph.variable_artefact(store, hash_t("1"), "file")


def test_artefact_update() -> None:
    """Test updating a const artefact."""
    store = Redis()
    art = _graph.constant_artefact(store, b"bla bla")
    with pytest.raises(TypeError):
        _graph.set_data(store, art, b"b")


def test_not_generated() -> None:
    """What happens when an artefact is not generated?"""
    with Fun(Redis()) as db:
        s = funsies.shell("cp file1 file2", inp=dict(file1="bla"), out=["file3"])
        funsies.run_op(db, s.op.hash)
        assert funsies.take(s.returncode) == b"0"
        with pytest.raises(funsies.UnwrapError):
            funsies.take(s.out["file3"])


def test_error_propagation() -> None:
    """Test propagation of errors."""
    with Fun(Redis()) as db:
        s1 = funsies.shell("cp file1 file3", inp=dict(file1="bla"), out=["file2"])
        s2 = funsies.shell(
            "cat file1 file2", inp=dict(file1="a file", file2=s1.out["file2"])
        )
        funsies.run_op(db, s1.op.hash)
        funsies.run_op(db, s2.op.hash)
        out = funsies.take(s2.stdout, strict=False)
        print(out)
        assert isinstance(out, funsies.Error)
        assert out.source == s1.op.hash


def test_error_propagation_morph() -> None:
    """Test propagation of errors."""
    with Fun(Redis()) as db:
        s1 = funsies.shell("cp file1 file3", inp=dict(file1="bla"), out=["file2"])

        def fun_strict(inp: bytes) -> bytes:
            return inp

        def fun_lax(inp: funsies.Result[bytes]) -> bytes:
            return b"bla bla"

        s2 = funsies.morph(fun_strict, s1.out["file2"])
        s3 = funsies.morph(fun_lax, s1.out["file2"])
        s4 = funsies.morph(fun_lax, s1.out["file2"], strict=False)

        funsies.run_op(db, s1.op.hash)
        funsies.run_op(db, s2.parent)

        out = funsies.take(s2, strict=False)
        assert isinstance(out, funsies.Error)
        assert out.source == s1.op.hash

        funsies.run_op(db, s3.parent)
        out = funsies.take(s3, strict=False)
        assert isinstance(out, funsies.Error)
        assert out.source == s1.op.hash

        funsies.run_op(db, s4.parent)
        out = funsies.take(s4)
        assert out == b"bla bla"


def test_error_propagation_shell() -> None:
    """Test propagation of errors."""
    store = Redis()
    s1 = funsies.shell(
        "cp file1 file3",
        inp=dict(file1="bla"),
        out=["file2"],
        connection=store,
        opt=options(),
    )
    s2 = funsies.shell(
        "cat file2", inp=dict(file2=s1.out["file2"]), connection=store, opt=options()
    )
    s3 = funsies.shell(
        "cat file2",
        inp=dict(file2=s1.out["file2"]),
        strict=False,
        connection=store,
        opt=options(),
    )

    funsies.run_op(store, s1.op.hash)
    funsies.run_op(store, s2.op.hash)
    with pytest.raises(funsies.UnwrapError):
        funsies.take(s2.stderr, connection=store)

    funsies.run_op(store, s3.op.hash)
    with Fun(store):
        assert funsies.take(s3.stderr) != b""
        assert funsies.take(s3.returncode) != b"0"


def test_error_tolerant() -> None:
    """Test error tolerant funsie."""
    store = Redis()

    def error_tolerant_fun(inp: funsies.Result[bytes]) -> bytes:
        if isinstance(inp, funsies.Error):
            return b"err"
        else:
            return b""

    with Fun(store):
        s1 = funsies.shell("cp file1 file3", inp=dict(file1="bla"), out=["file2"])
        s2 = funsies.morph(error_tolerant_fun, s1.out["file2"], strict=False)

        with pytest.raises(RuntimeError):
            # Test operation not found
            funsies.run_op(store, s2.hash)
        funsies.run_op(store, s1.op)
        funsies.run_op(store, s2.parent)
        assert funsies.take(s2) == b"err"
