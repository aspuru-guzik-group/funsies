"""Tests of error handling."""
# std

# external
from fakeredis import FakeStrictRedis as Redis
import pytest

# funsies
import funsies
from funsies import _graph, Fun, options
from funsies._context import get_connection
from funsies._run import run_op
from funsies._storage import RedisStorage
from funsies.config import MockServer
from funsies.types import Encoding, Error, hash_t, Result, UnwrapError


def test_artefact_add() -> None:
    """Test adding const artefacts."""
    db = Redis()
    store = RedisStorage(db)
    a = _graph.constant_artefact(db, store, b"bla bla")
    b = _graph.Artefact[bytes].grab(db, a.hash)
    assert b is not None
    assert a == b


def test_artefact_load_errors() -> None:
    """Test loading artefact errors."""
    db = Redis()
    store = RedisStorage(db)
    with pytest.raises(RuntimeError):
        _ = _graph.Artefact.grab(db, hash_t("bla"))

    # TODO check that warnings are logged?
    _graph.constant_artefact(db, store, b"bla bla")
    _graph.constant_artefact(db, store, b"bla bla")

    _graph.variable_artefact(db, hash_t("1"), "file", Encoding.blob)
    _graph.variable_artefact(db, hash_t("1"), "file", Encoding.blob)


def test_artefact_update() -> None:
    """Test updating a const artefact."""
    db = Redis()
    store = RedisStorage(db)
    art = _graph.constant_artefact(db, store, b"bla bla")
    with pytest.raises(TypeError):
        _graph.set_data(db, store, art.hash, b"b", _graph.ArtefactStatus.done)


def test_not_generated() -> None:
    """What happens when an artefact is not generated?"""
    with Fun(MockServer()):
        db, store = get_connection()
        s = funsies.shell("cp file1 file2", inp=dict(file1="bla"), out=["file3"])
        run_op(db, store, s.op.hash)
        assert funsies.take(s.returncode) == 0
        with pytest.raises(UnwrapError):
            funsies.take(s.out["file3"])


def test_error_propagation() -> None:
    """Test propagation of errors."""
    with Fun(MockServer()):
        db, store = get_connection()
        s1 = funsies.shell("cp file1 file3", inp=dict(file1="bla"), out=["file2"])
        s2 = funsies.shell(
            "cat file1 file2", inp=dict(file1="a file", file2=s1.out["file2"])
        )
        run_op(db, store, s1.op.hash)
        run_op(db, store, s2.op.hash)
        out = funsies.take(s2.stdout, strict=False)
        print(out)
        assert isinstance(out, Error)
        assert out.source == s1.op.hash


def test_error_propagation_morph() -> None:
    """Test propagation of errors."""
    with Fun(MockServer()):
        db, store = get_connection()
        s1 = funsies.shell("cp file1 file3", inp=dict(file1="bla"), out=["file2"])

        def fun_strict(inp: bytes) -> bytes:
            return inp

        def fun_lax(inp: Result[bytes]) -> bytes:
            return b"bla bla"

        s2 = funsies.morph(fun_strict, s1.out["file2"])
        s3 = funsies.morph(fun_lax, s1.out["file2"])
        s4 = funsies.morph(fun_lax, s1.out["file2"], strict=False)

        run_op(db, store, s1.op.hash)
        run_op(db, store, s2.parent)

        out = funsies.take(s2, strict=False)
        assert isinstance(out, Error)
        assert out.source == s1.op.hash

        print(s3.parent)
        run_op(db, store, s3.parent)
        out = funsies.take(s3, strict=False)
        assert isinstance(out, Error)
        assert out.source == s1.op.hash

        run_op(db, store, s4.parent)
        out = funsies.take(s4)
        assert out == b"bla bla"


def test_error_propagation_shell() -> None:
    """Test propagation of errors."""
    db = Redis()
    store = RedisStorage(db)
    s1 = funsies.shell(
        "cp file1 file3",
        inp=dict(file1="bla"),
        out=["file2"],
        connection=(db, store),
        opt=options(),
    )
    s2 = funsies.shell(
        "cat file2",
        inp=dict(file2=s1.out["file2"]),
        connection=(db, store),
        opt=options(),
    )
    s3 = funsies.shell(
        "cat file2",
        inp=dict(file2=s1.out["file2"]),
        strict=False,
        connection=(db, store),
        opt=options(),
    )

    run_op(db, store, s1.op.hash)
    run_op(db, store, s2.op.hash)
    with pytest.raises(UnwrapError):
        funsies.take(s2.stderr, connection=(db, store))

    run_op(db, store, s3.op.hash)
    assert funsies.take(s3.stderr, connection=(db, store)) != b""
    assert isinstance(funsies.take(s3.returncode, connection=(db, store)), int)
    assert funsies.take(s3.returncode, connection=(db, store)) != 0


def test_error_tolerant() -> None:
    """Test error tolerant funsie."""

    def error_tolerant_fun(inp: Result[bytes]) -> bytes:
        if isinstance(inp, Error):
            return b"err"
        else:
            return b""

    with Fun(MockServer()):
        db, store = get_connection()
        s1 = funsies.shell("cp file1 file3", inp=dict(file1="bla"), out=["file2"])
        s2 = funsies.morph(error_tolerant_fun, s1.out["file2"], strict=False)

        with pytest.raises(RuntimeError):
            # Test operation not found
            run_op(db, store, s2.hash)

        run_op(db, store, s1.op)
        run_op(db, store, s2.parent)
        assert funsies.take(s2) == b"err"
