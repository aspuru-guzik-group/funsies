"""Test of Funsies datastructures."""
# module
from funsies import _funsies as f


def test_instantiate() -> None:
    """Test the instantiation of a Funsie class."""
    out = f.Funsie(
        how=f.FunsieHow.shell,
        what=b"cat",
        inp={"infile": "bytes"},
        out={"stdout": "bytes"},
    )
    assert out is not None


def test_pack_unpack() -> None:
    """Test the packing of a Funsie class."""
    out = f.Funsie(
        how=f.FunsieHow.shell,
        what=b"cat",
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
        what=b"cat",
        inp={"infile": "bytes", "infile2": "str"},
        out={"out2": "bytes", "stdout": "bytes"},
    )

    out2 = f.Funsie(
        how=f.FunsieHow.shell,
        what=b"cat",
        inp={"infile2": "str", "infile": "bytes"},
        out={"stdout": "bytes", "out2": "bytes"},
    )

    out3 = f.Funsie(
        how=f.FunsieHow.python,
        what=b"cat",
        inp={"infile2": "str", "infile": "bytes"},
        out={"stdout": "bytes", "out2": "bytes"},
    )

    assert str(out) == str(out2)
    assert str(out) != str(out3)
