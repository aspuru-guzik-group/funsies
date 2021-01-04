"""Test of Funsies datastructures."""

# external
import pytest

# module
from funsies import _funsies as f


def test_instantiate() -> None:
    """Test the instantiation of a Funsie class."""
    out = f.Funsie(
        how=f.FunsieHow.shell,
        what="cat",
        inp={"infile": "bytes"},
        out={"stdout": "bytes"},
    )
    assert out is not None


def test_pack_unpack() -> None:
    """Test the packing of a Funsie class."""
    out = f.Funsie(
        how=f.FunsieHow.shell,
        what="cat",
        inp={"infile": "bytes"},
        out={"stdout": "bytes"},
    )

    b = out.pack()
    out2 = f.Funsie.unpack(b)
    assert out == out2


def test_str() -> None:
    """Test funsies to string."""
    out = f.Funsie(
        how=f.FunsieHow.shell,
        what="cat",
        inp={"infile": "bytes", "infile2": "str"},
        out={"out2": "bytes", "stdout": "bytes"},
    )

    out2 = f.Funsie(
        how=f.FunsieHow.shell,
        what="cat",
        inp={"infile2": "str", "infile": "bytes"},
        out={"stdout": "bytes", "out2": "bytes"},
    )

    out3 = f.Funsie(
        how=f.FunsieHow.python,
        what="cat",
        inp={"infile2": "str", "infile": "bytes"},
        out={"stdout": "bytes", "out2": "bytes"},
    )

    assert str(out) == str(out2)
    assert out.hash() == out2.hash()
    assert out.hash() != out3.hash()


def test_artefact_add() -> None:
    """Test adding explicit artefacts."""
    kv = {}
    f.store_artefact(kv, "bla bla")
    print(kv)

    with pytest.raises(TypeError):
        f.store_artefact(kv, None)


test_artefact_add()
